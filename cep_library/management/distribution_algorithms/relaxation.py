from enum import Enum
import math
import sys
import networkx as nx
from copy import deepcopy
from typing import Any, List, Tuple

from cep_library import configs
from cep_library.cep.model.cep_task import CEPTask
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
from cep_library.management.model.task_alteration import TaskAlterationModel
from cep_library.management.model.topology import Topology, TopologyNode
from cep_library.management.statistics.consumer_server_statics import (
    ConsumerServerStatics,
)
from cep_library.management.statistics.load_helper import (
    get_estimated_data_delay,
    get_max_percentage,
)
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import (
    ManagementServerStatisticsAnalyzer,
)
from cep_library.raw.model.raw_update_event import RawUpdateEvent
from cep_library.raw.model.raw_settings import RawSettings


class SpringElemenType(Enum):
    SINK = 0
    RAWOUTPUT = 1
    EVENTEXECUTION = 2
    CONSUMER = 3
    HEAD = 4


class SpringSinkElement:
    def __init__(self) -> None:
        pass


class SpringSourceElement:
    def __init__(self) -> None:
        pass


class SpringRawSourceElement:
    def __init__(self, data_id: str, settings: RawSettings) -> None:
        self.data_id = data_id
        self.settings = deepcopy(settings)

    def output_topic_exists(self, topic: str) -> bool:
        return topic == self.settings.output_topic.output_topic


class SpringEventElement:
    def __init__(self, event_id: str, cep_task: CEPTask) -> None:
        self.event_id = event_id
        self.cep_task = deepcopy(cep_task)
        self.reset_settings()

    def reset_settings(self):
        for rs in self.cep_task.settings.required_sub_tasks:
            rs.subscription_topics = []

    def input_topic_exists(self, topic: str) -> bool:
        return topic in [
            rs.input_topic for rs in self.cep_task.settings.required_sub_tasks
        ]

    def output_topic_exists(self, topic: str) -> bool:
        return topic == self.cep_task.settings.output_topic.output_topic


class SpringConsumerElement:
    def __init__(self, data_id: str, settings: CEPConsumerSettings) -> None:
        self.data_id = data_id
        self.settings = deepcopy(settings)
        self.reset_settings()

    def reset_settings(self):
        self.settings.source_topics = []

    def input_topic_exists(self, topic: str) -> bool:
        return topic == self.settings.topic_name


class SpringNode:
    def __init__(self, node_type: SpringElemenType, d_id: str, ele: Any) -> None:
        self.node_type = node_type
        self.d_id = d_id
        self.ele = ele


class SpringGraph:
    def __init__(
        self,
        workers: dict[str, dict[str, int | float]],
        producers: dict[str, dict[str, RawSettings]],
        consumers: dict[str, dict[str, CEPConsumerSettings]],
        rawstats: RawServerStatAnalyzer,
        mssa: ManagementServerStatisticsAnalyzer,
        consumerstats: ConsumerServerStatics,
    ) -> None:
        self.G = nx.DiGraph()

        self.rawstats = rawstats
        self.mssa = mssa
        self.consumerstats = consumerstats

        print("Generating the graph")
        print(f"Workers: {workers}")

        self.producers: dict[str, dict[str, RawSettings]] = producers
        self.consumers: dict[str, dict[str, CEPConsumerSettings]] = consumers

        self.node_count = 0

        self.worker_device_ids = list(workers.keys())

        self.device_limits: dict[str, int] = {}
        w_key: str
        for w_key, _ in workers.items():
            self.device_limits[w_key] = 0

        self.event_counts: dict[str, int] = {}

    def init_event_limit(self, event_id: str) -> None:
        self.event_counts[event_id] = 0

    def get_node_action_count(self, node_id: str) -> float:
        return self.device_limits[node_id]

    def get_size(self, print_log: bool = False) -> float:

        assumed_weight: float = self.G.size(weight="weight")
        (
            max_bw_load,
            above_threshold_count,
            total_load_sum,
            total_load_min,
            total_load_max,
            link_load_sum,
            link_load_max,
        ) = self.get_highest_bw_load(print_loads=print_log)
        if configs.RELAX_MULTIPLY_BANDWIDTH_BALANCER:
            eventual_assumed_weight: float = assumed_weight * above_threshold_count
        elif configs.RELAX_INCLUDE_BANDWIDTH_BALANCER:
            eventual_assumed_weight: float = assumed_weight + max_bw_load
        elif configs.RELAX_MULTIPLY_HIGHEST_LOAD:
            eventual_assumed_weight: float = assumed_weight * max_bw_load
        elif configs.RELAX_USE_MAX_BW_USAGE:
            eventual_assumed_weight: float = total_load_max
        elif configs.RELAX_USE_SUM_BW_USAGE:
            eventual_assumed_weight: float = total_load_sum
        elif configs.RELAX_USE_SUM_LINK_USAGE:
            eventual_assumed_weight: float = link_load_sum
        elif configs.RELAX_USE_MAX_LINK_USAGE:
            eventual_assumed_weight: float = link_load_max
        else:
            eventual_assumed_weight: float = assumed_weight

        return eventual_assumed_weight

    def validate_device_limits(self) -> None:
        for _, d_count in self.device_limits.items():
            if d_count > configs.DEVICE_ACTION_LIMIT:
                raise Exception("Device action limit has been breached!!")

    def device_limit_reached(self, d_id: str, skip_load_check: bool = False) -> bool:
        device_cpu_loads, device_uplink_load, device_downlink_load = (
            self.get_entire_load()
        )

        limit_reached: bool = (
            d_id in device_cpu_loads
            and device_cpu_loads[d_id] >= configs.DEVICE_EXECUTION_LIMIT
        ) or (
            configs.RELAX_USE_HARD_BW_LIMIT
            and (
                (
                    d_id in device_uplink_load
                    and device_uplink_load[d_id] > (configs.CUMULATIVE_LOAD_LIMIT / 2)
                )
                or (
                    d_id in device_downlink_load
                    and device_downlink_load[d_id] > (configs.CUMULATIVE_LOAD_LIMIT / 2)
                )
            )
            and not skip_load_check
        )

        if limit_reached:
            print(f"Device limit reached: {d_id}")

        return limit_reached

    def event_parallelization_limit_reached(self, evnet_id: str) -> bool:
        return self.event_counts[evnet_id] >= configs.ALLOWED_PARALLEL_EXECUTION_COUNT

    def add_static_node(self, node: SpringNode):
        self.G.add_node(node)
        self.node_count += 1

    def set_head(self, node: SpringNode):
        self.Head = node

    def set_sink(self, node: SpringNode):
        self.Sink = node

    def add_executor_node(self, node: SpringNode):
        if node.node_type != SpringElemenType.EVENTEXECUTION:
            raise Exception("Not an executor node!")

        ele: SpringEventElement = node.ele

        if self.event_counts[ele.event_id] >= configs.ALLOWED_PARALLEL_EXECUTION_COUNT:
            raise Exception(
                "Should not assign an event beyond its parallelization limit!"
            )

        if self.device_limits[node.d_id] >= configs.DEVICE_ACTION_LIMIT:
            raise Exception("Should not assign to a device that reached its limit!")

        self.G.add_node(node)
        self.node_count += 1

        self.device_limits[node.d_id] += 1
        self.event_counts[ele.event_id] += 1
        return True

    def remove_executor_node(self, node: SpringNode):
        if node.node_type != SpringElemenType.EVENTEXECUTION:
            raise Exception("Not an executor node!")

        ele: SpringEventElement = node.ele
        self.G.remove_node(node)
        self.device_limits[node.d_id] -= 1
        self.event_counts[ele.event_id] -= 1
        self.node_count -= 1

        if (
            self.device_limits[node.d_id] < 0
            or self.event_counts[ele.event_id] < 0
            or self.node_count < 0
        ):
            raise Exception("Counts cannot be below 0!!!")

    def add_edge(
        self,
        source: SpringNode,
        target: SpringNode,
        weight: float,
        topic: str,
        expected_event_byte: float,
    ):
        if weight is None or weight == 0:
            raise Exception("Weight cannot be none!")
        if topic is None or topic == "":
            raise Exception("Weight cannot be none!")
        if source.d_id != target.d_id and expected_event_byte == 0:
            raise Exception(
                "Devices are different but expected_event_byte is equal to 0!"
            )

        self.G.add_edge(
            source,
            target,
            weight=weight,
            topic=topic,
            expected_event_byte=expected_event_byte,
        )

    def the_bw_ratio(self, ratio: float) -> float:
        value: float = (
            1
            + max(0, 1 + math.floor((ratio - 0.75) / 0.1)) * configs.GA_OVERLOAD_PENALTY
        )
        if ratio < 0.75 and value > 1:
            raise Exception("Invalid bw penalty calculation detected!")
        return value

    def get_all_device_loads(self) -> dict[str, float]:
        device_loads: dict[str, float] = {}
        u: SpringNode
        v: SpringNode
        for u, v, data_dict in self.G.edges.data(data=True):
            if "expected_event_byte" in data_dict:
                load = data_dict["expected_event_byte"]
                if u.d_id not in device_loads:
                    device_loads[u.d_id] = 0.0
                if v.d_id not in device_loads:
                    device_loads[v.d_id] = 0.0

                if u.d_id != v.d_id and load == 0.0:
                    raise Exception(
                        f"Devices are different but load is equal to 0, {u.d_id}:{u.node_type}, {v.d_id}:{v.node_type}, {load}"
                    )

                if u.d_id != v.d_id:
                    device_loads[u.d_id] += load
                    device_loads[v.d_id] += load

        return device_loads

    def get_device_loads(self, d_id: str) -> float:
        device_loads: dict[str, float] = {}
        u: SpringNode
        v: SpringNode
        for u, v, data_dict in self.G.edges.data(data=True):
            if "expected_event_byte" in data_dict:
                load = data_dict["expected_event_byte"]
                if u.d_id not in device_loads:
                    device_loads[u.d_id] = 0.0
                if v.d_id not in device_loads:
                    device_loads[v.d_id] = 0.0

                if u.d_id != v.d_id and load == 0.0:
                    raise Exception(
                        f"Devices are different but load is equal to 0, {u.d_id}:{u.node_type}, {v.d_id}:{v.node_type}, {load}"
                    )

                if u.d_id != v.d_id:
                    device_loads[u.d_id] += load
                    device_loads[v.d_id] += load
        if d_id not in device_loads:
            return 0.0
        return device_loads[d_id]

    def get_entire_load(
        self,
    ) -> Tuple[dict[str, float], dict[str, float], dict[str, float]]:
        device_cpu_loads: dict[str, float] = {}
        u: SpringNode
        for u in list(nx.topological_sort(self.G)):
            if u.d_id not in device_cpu_loads:
                device_cpu_loads[u.d_id] = 0.0
            if u.node_type == SpringElemenType.RAWOUTPUT:
                raw_ele: SpringRawSourceElement = u.ele
                device_cpu_loads[u.d_id] += self.rawstats.get_expected_event_count(
                    raw_ele.settings.producer_name,
                    raw_ele.settings.output_topic.output_topic,
                )
            if u.node_type == SpringElemenType.CONSUMER:
                cons_ele: SpringConsumerElement = u.ele
                device_cpu_loads[
                    u.d_id
                ] += self.consumerstats.get_expected_event_counts(
                    cons_ele.settings.host_name, cons_ele.settings.topic_name
                )
            if u.node_type == SpringElemenType.EVENTEXECUTION:
                exec_ele: SpringEventElement = u.ele
                for rti in exec_ele.cep_task.settings.required_sub_tasks:
                    device_cpu_loads[
                        u.d_id
                    ] += self.mssa.get_expected_event_input_counts(
                        exec_ele.cep_task.settings.action_name, rti.input_topic
                    )

        u: SpringNode
        v: SpringNode
        device_uplink_load: dict[str, float] = {}
        device_downlink_load: dict[str, float] = {}
        for u, v, data_dict in self.G.edges.data(data=True):
            if "expected_event_byte" in data_dict:
                load = data_dict["expected_event_byte"]
                if u.d_id not in device_uplink_load:
                    device_uplink_load[u.d_id] = 0.0
                if u.d_id not in device_downlink_load:
                    device_downlink_load[u.d_id] = 0.0
                if v.d_id not in device_uplink_load:
                    device_uplink_load[v.d_id] = 0.0
                if v.d_id not in device_downlink_load:
                    device_downlink_load[v.d_id] = 0.0

                if u.d_id != v.d_id and load == 0.0:
                    raise Exception(
                        f"Devices are different but load is equal to 0, {u.d_id}:{u.node_type}, {v.d_id}:{v.node_type}, {load}"
                    )

                if u.d_id != v.d_id:
                    device_uplink_load[u.d_id] += load
                    device_downlink_load[v.d_id] += load

        return device_cpu_loads, device_uplink_load, device_downlink_load

    def get_expected_delay(
        self,
        link_load: float,
        min_link_load: float,
        max_link_load: float,
        min_link_duration: float,
        max_link_duration: float,
    ) -> float:
        if max_link_load == min_link_load:
            return min_link_duration

        return min_link_duration + (link_load - min_link_load) * (
            max_link_duration - min_link_duration
        ) / (max_link_load - min_link_load)

    def get_highest_bw_load(
        self, print_loads: bool = False
    ) -> Tuple[float, float, float, float, float, float, float]:
        device_cpu_loads: dict[str, float] = {}
        u: SpringNode
        for u in list(nx.topological_sort(self.G)):
            if u.d_id not in device_cpu_loads:
                device_cpu_loads[u.d_id] = 0.0
            if u.node_type == SpringElemenType.RAWOUTPUT:
                raw_ele: SpringRawSourceElement = u.ele
                device_cpu_loads[u.d_id] += self.rawstats.get_expected_event_count(
                    raw_ele.settings.producer_name,
                    raw_ele.settings.output_topic.output_topic,
                )
            if u.node_type == SpringElemenType.CONSUMER:
                cons_ele: SpringConsumerElement = u.ele
                device_cpu_loads[
                    u.d_id
                ] += self.consumerstats.get_expected_event_counts(
                    cons_ele.settings.host_name, cons_ele.settings.topic_name
                )
            if u.node_type == SpringElemenType.EVENTEXECUTION:
                exec_ele: SpringEventElement = u.ele
                for rti in exec_ele.cep_task.settings.required_sub_tasks:
                    device_cpu_loads[
                        u.d_id
                    ] += self.mssa.get_expected_event_input_counts(
                        exec_ele.cep_task.settings.action_name, rti.input_topic
                    )

        device_loads: dict[str, float] = {}
        device_min_loads: dict[str, float] = {}
        device_max_loads: dict[str, float] = {}
        u: SpringNode
        v: SpringNode
        device_link_count: dict[str, float] = {}
        device_uplink_count: dict[str, float] = {}
        device_downlink_count: dict[str, float] = {}
        device_uplink_load: dict[str, float] = {}
        device_downlink_load: dict[str, float] = {}
        for u, v, data_dict in self.G.edges.data(data=True):
            if "expected_event_byte" in data_dict:
                load = data_dict["expected_event_byte"]
                if u.d_id not in device_loads:
                    device_loads[u.d_id] = 0.0
                    device_link_count[u.d_id] = 0.0
                    device_uplink_count[u.d_id] = 0.0
                    device_downlink_count[u.d_id] = 0.0
                    device_min_loads[u.d_id] = sys.maxsize
                    device_max_loads[u.d_id] = 0.0
                if v.d_id not in device_loads:
                    device_loads[v.d_id] = 0.0
                    device_link_count[v.d_id] = 0.0
                    device_uplink_count[v.d_id] = 0.0
                    device_downlink_count[v.d_id] = 0.0
                    device_min_loads[v.d_id] = sys.maxsize
                    device_max_loads[v.d_id] = 0.0

                if u.d_id not in device_uplink_load:
                    device_uplink_load[u.d_id] = 0.0
                if v.d_id not in device_uplink_load:
                    device_uplink_load[v.d_id] = 0.0

                if u.d_id not in device_downlink_load:
                    device_downlink_load[u.d_id] = 0.0
                if v.d_id not in device_downlink_load:
                    device_downlink_load[v.d_id] = 0.0

                if u.d_id != v.d_id and load == 0.0:
                    raise Exception(
                        f"Devices are different but load is equal to 0, {u.d_id}:{u.node_type}, {v.d_id}:{v.node_type}, {load}"
                    )

                if u.d_id != v.d_id:
                    device_loads[u.d_id] += load
                    device_loads[v.d_id] += load

                    device_link_count[u.d_id] += 1.0
                    device_link_count[v.d_id] += 1.0

                    device_uplink_count[u.d_id] += 1.0
                    device_downlink_count[v.d_id] += 1.0

                    device_uplink_load[u.d_id] += load
                    device_downlink_load[v.d_id] += load

                    device_min_loads[u.d_id] = min(device_min_loads[u.d_id], load)
                    device_max_loads[u.d_id] = max(device_max_loads[u.d_id], load)

                    device_min_loads[v.d_id] = min(device_min_loads[v.d_id], load)
                    device_max_loads[v.d_id] = max(device_max_loads[v.d_id], load)

        above_threshold: float = 1.0
        total_delay_min: float = sys.maxsize * 1.0
        total_delay_max: float = 0.0
        total_delay_sum: float = 0.0
        device_over_limits: dict[str, float] = {}

        half_limit = configs.CUMULATIVE_LOAD_LIMIT / 2.0
        if print_loads:
            print("up-down loads")
        for d_id, d_load in device_uplink_load.items():
            if print_loads:
                print(f"uplink: {d_id} = {d_load} - {d_load > half_limit}")
            crr_d_load: float = d_load
            if d_load > (configs.CUMULATIVE_LOAD_LIMIT / 2.0):
                crr_d_load = crr_d_load * 1000000000.0
            total_delay_max = max(total_delay_max, crr_d_load)
            total_delay_sum += crr_d_load

        for d_id, d_load in device_downlink_load.items():
            if print_loads:
                print(f"downlink: {d_id} = {d_load} - {d_load > half_limit}")
            crr_d_load: float = d_load
            if d_load > (configs.CUMULATIVE_LOAD_LIMIT / 2.0):
                crr_d_load = crr_d_load * 1000000000.0
            total_delay_max = max(total_delay_max, crr_d_load)
            total_delay_sum += crr_d_load

        if print_loads:
            print("=================================")

        link_delay_sum: float = 0.0
        link_delay_max: float = 0.0
        for u, v, data_dict in self.G.edges.data(data=True):
            if "expected_event_byte" in data_dict:

                if u.d_id != v.d_id:
                    load: float = data_dict["expected_event_byte"]

                    u_rate: float = get_estimated_data_delay(device_loads[u.d_id])
                    v_rate: float = get_estimated_data_delay(device_loads[v.d_id])

                    if configs.RELAX_USE_OVERLOAD_PENALTY:
                        u_rate = u_rate * device_over_limits[u.d_id]

                    if configs.RELAX_USE_OVERLOAD_PENALTY:
                        v_rate = v_rate * device_over_limits[v.d_id]

                    if configs.RELAX_USE_RATE_ONLY_FOR_MAX_LINK:
                        u_load_rate: float = 0.0
                        v_load_rate: float = 0.0

                        max_delay: float = max(u_rate, v_rate)
                    elif configs.RELAX_USE_PENALTY_ON_LOAD:
                        u_load_rate: float = device_over_limits[u.d_id] * load
                        v_load_rate: float = device_over_limits[v.d_id] * load

                        max_delay: float = max(u_load_rate, v_load_rate)
                    elif configs.RELAX_USE_LINK_COUNT_FOR_BALANCE:
                        u_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_link_count[u.d_id]
                        )
                        v_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_link_count[v.d_id]
                        )

                        max_delay: float = max(u_load_rate, v_load_rate)
                    elif configs.RELAX_USE_LINK_COUNT_WITH_OVERLOAD_PENALTY:
                        u_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_link_count[u.d_id]
                        )
                        v_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_link_count[v.d_id]
                        )

                        u_load_rate = u_load_rate * device_over_limits[u.d_id]
                        v_load_rate = v_load_rate * device_over_limits[v.d_id]

                        max_delay: float = max(u_load_rate, v_load_rate)

                    elif configs.RELAX_USE_ADVANCED_LINK_COST:

                        u_min_duration: float = device_min_loads[u.d_id] / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_link_count[u.d_id]
                        )
                        v_min_duration: float = device_min_loads[v.d_id] / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_link_count[v.d_id]
                        )

                        u_load_rate: float = self.get_expected_delay(
                            link_load=load,
                            min_link_load=device_min_loads[u.d_id],
                            max_link_load=device_max_loads[u.d_id],
                            min_link_duration=u_min_duration,
                            max_link_duration=u_rate,
                        )

                        v_load_rate: float = self.get_expected_delay(
                            link_load=load,
                            min_link_load=device_min_loads[v.d_id],
                            max_link_load=device_max_loads[v.d_id],
                            min_link_duration=v_min_duration,
                            max_link_duration=v_rate,
                        )

                        if (
                            configs.RELAX_USE_ADVANCED_LINK_COST_WITH_LOAD_PENALTY
                            == True
                        ):
                            u_load_rate = u_load_rate * device_over_limits[u.d_id]
                            v_load_rate = v_load_rate * device_over_limits[v.d_id]

                        max_delay: float = max(u_load_rate, v_load_rate)

                    elif configs.RELAX_USE_UP_DOWN_LINK_COUNT_FOR_BALANCE:
                        u_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / device_uplink_count[u.d_id]
                        )
                        v_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT
                            / device_downlink_count[v.d_id]
                        )

                        max_delay: float = max(u_load_rate, v_load_rate)
                    elif configs.RELAX_USE_UP_DOWN_LINK_LOAD:
                        u_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )
                        v_load_rate: float = load / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )

                        u_device_load: float = device_uplink_load[u.d_id] / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )
                        v_device_load: float = device_downlink_load[v.d_id] / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )

                        if u_device_load > 0.5:
                            u_load_rate = u_load_rate * 1.25
                        elif u_device_load > 0.75:
                            u_load_rate = u_load_rate * 2.0
                        elif u_device_load > 1.0:
                            u_load_rate = u_load_rate * 1000000.0

                        if v_device_load > 0.5:
                            v_load_rate = v_load_rate * 1.25
                        elif v_device_load > 0.75:
                            v_load_rate = v_load_rate * 2.0
                        elif v_device_load > 1.0:
                            v_load_rate = v_load_rate * 1000000.0

                        if device_cpu_loads[v.d_id] > configs.DEVICE_EXECUTION_LIMIT:
                            v_load_rate = v_load_rate * 1000000.0

                        max_delay: float = max(u_load_rate, v_load_rate)
                    elif configs.RELAX_USE_FULL_UP_DOWN_LINK_LOAD:
                        u_load_rate: float = device_uplink_load[u.d_id] / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )
                        v_load_rate: float = device_downlink_load[v.d_id] / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )

                        if device_cpu_loads[u.d_id] > configs.DEVICE_EXECUTION_LIMIT:
                            u_load_rate = u_load_rate * 1000000000.0

                        if device_cpu_loads[v.d_id] > configs.DEVICE_EXECUTION_LIMIT:
                            v_load_rate = v_load_rate * 1000000000.0

                        if device_uplink_load[u.d_id] > (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        ):
                            u_load_rate = u_load_rate * 1000000000.0
                        elif device_uplink_load[u.d_id] > (
                            configs.CUMULATIVE_LOAD_LIMIT
                            * configs.CP_SAT_BW_FIRST_TH
                            / 2.0
                        ):
                            u_load_rate = u_load_rate * configs.CP_SAT_ABOVE_BW_MULTP

                        if device_downlink_load[v.d_id] > (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        ):
                            v_load_rate = v_load_rate * 1000000000.0
                        elif device_downlink_load[v.d_id] > (
                            configs.CUMULATIVE_LOAD_LIMIT
                            * configs.CP_SAT_BW_FIRST_TH
                            / 2.0
                        ):
                            v_load_rate = v_load_rate * configs.CP_SAT_ABOVE_BW_MULTP

                        max_delay: float = max(u_load_rate, v_load_rate)
                    else:
                        u_load_rate: float = (
                            u_rate * load / configs.CUMULATIVE_LOAD_LIMIT
                        )
                        v_load_rate: float = (
                            v_rate * load / configs.CUMULATIVE_LOAD_LIMIT
                        )

                        max_delay: float = max(u_load_rate, v_load_rate)

                    link_delay_max = max(link_delay_max, max_delay)
                    link_delay_sum += max_delay

                    if print_loads:
                        print(
                            f"Load: {load}, u byte: {device_loads[u.d_id]}, v byte: {device_loads[v.d_id]}, u rate: {u_rate}, v rate: {v_rate}, u load rate: {u_load_rate}, v load rate: {v_load_rate}, max rate: {max_delay}, link load max: {link_delay_max}"
                        )

        if print_loads:
            print("=================================")

        return (
            load,
            above_threshold,
            total_delay_sum,
            total_delay_min,
            total_delay_max,
            link_delay_sum,
            link_delay_max,
        )

    def add_dummy_edge(self, source: SpringNode, target: SpringNode):
        self.G.add_edge(source, target, weight=0.0, topic="")

    def remove_edge(self, source: SpringNode, target: SpringNode):
        self.G.remove_edge(source, target)

    def get_topologically_ordered_nodes(
        self, reverse: bool = False
    ) -> List[SpringNode]:
        if configs.RELAX_USE_REVERSED_ORDER and reverse:
            lst = list(nx.topological_sort(self.G))
            lst.reverse()
            return lst
        else:
            return list(nx.topological_sort(self.G))

    def get_event_locations(self, event_name: str) -> List[str]:
        event_devices: List[str] = []
        n: SpringNode
        for n in self.G:
            if n.node_type == SpringElemenType.EVENTEXECUTION:
                cep_node: SpringEventElement = n.ele
                if cep_node.cep_task.settings.action_name == event_name:
                    if n.d_id in event_devices:
                        raise Exception(
                            "A device can only have a single instance of an event!"
                        )
                    event_devices.append(n.d_id)
        return event_devices

    def get_successors(self, node: SpringNode) -> List[SpringNode]:
        return list(self.G.successors(node))

    def get_predecessors(self, node: SpringNode) -> List[SpringNode]:
        return list(self.G.predecessors(node))

    def find_related_parents(self, topic_name: str) -> List[SpringNode]:
        parents: List[SpringNode] = []
        n: SpringNode
        for n in self.G.nodes:
            if n.node_type in [
                SpringElemenType.HEAD,
                SpringElemenType.SINK,
                SpringElemenType.CONSUMER,
            ]:
                continue
            if n.node_type == SpringElemenType.RAWOUTPUT:
                raw_ele: SpringRawSourceElement = n.ele
                if raw_ele.output_topic_exists(topic_name):
                    parents.append(n)
            if n.node_type == SpringElemenType.EVENTEXECUTION:
                event_ele: SpringEventElement = n.ele
                if event_ele.output_topic_exists(topic_name):
                    parents.append(n)
        return parents

    def find_related_children(self, topic_name: str) -> List[SpringNode]:
        children: List[SpringNode] = []
        n: SpringNode
        for n in self.G.nodes:
            if n.node_type in [
                SpringElemenType.HEAD,
                SpringElemenType.SINK,
                SpringElemenType.RAWOUTPUT,
            ]:
                continue
            if n.node_type == SpringElemenType.CONSUMER:
                raw_ele: SpringConsumerElement = n.ele
                if raw_ele.input_topic_exists(topic_name):
                    children.append(n)
            if n.node_type == SpringElemenType.EVENTEXECUTION:
                event_ele: SpringEventElement = n.ele
                if event_ele.input_topic_exists(topic_name):
                    children.append(n)
        return children


class RelaxationSolver:
    def __init__(
        self,
        topology: Topology,
        producers: dict[str, dict[str, RawSettings]],
        workers: dict,
        event_consumers: dict[str, dict[str, CEPConsumerSettings]],
        cep_tasks: List[CEPTask],
        rawstats: RawServerStatAnalyzer,
        mssa: ManagementServerStatisticsAnalyzer,
        consumerstats: ConsumerServerStatics,
        old_distribution_history: dict,
        mopa_cost: bool = False,
    ) -> None:

        self.workers = workers
        self.producers = producers
        self.consumers = event_consumers
        self.mssa = mssa
        self.rawstats = rawstats
        self.consumerstats = consumerstats

        self.old_event_locations: dict[str, List[str]] = {}
        execs: List[TopologyNode] = topology.get_executor_nodes()
        for exe in execs:

            if exe.name in old_distribution_history:
                self.old_event_locations[exe.name] = old_distribution_history[exe.name]
            else:
                self.old_event_locations[exe.name] = []

        self.different_device_penalty = configs.STATISTICS_REMOTE_MULTIPLIER
        self.oscillation_penalty = configs.RELAX_OSCILLATION_PENALTY
        self.force_ease_spring_count_limit = 5
        self.mopa_cost = mopa_cost

        self.G = SpringGraph(
            workers, producers, event_consumers, rawstats, mssa, consumerstats
        )

        self.generate_static_nodes()

        self.determine_initial_assignments(topology)

    def get_consumer_input_weight(
        self, consumer_name: str, consumer_topic: str, remote: bool = False
    ) -> float:
        if not self.mopa_cost:
            return self.consumerstats.get_reading_byte_mul_ns_square(
                consumer_name, consumer_topic, remote
            )
        else:
            return self.consumerstats.get_reading_byte_mul_ns(
                consumer_name, consumer_topic, remote
            )

    def get_event_input_topic_weight(
        self, event_id: str, topic_id: str, remote: bool = False
    ) -> float:
        if not self.mopa_cost:
            return self.mssa.topic_get_avg_byte_mul_ns_square(
                event_id, topic_id, remote
            )
        else:
            return self.mssa.topic_get_avg_byte_mul_ns(event_id, topic_id, remote)

    def get_event_output_weight(self, event_id: str, remote: bool = False) -> float:
        if not self.mopa_cost:
            return self.mssa.event_output_get_avg_byte_mul_ns_square(event_id, remote)
        else:
            return self.mssa.event_output_get_avg_byte_mul_ns(event_id, remote)

    def generate_static_nodes(self):

        source = SpringSourceElement()
        source_node = SpringNode(SpringElemenType.HEAD, "", source)
        self.G.add_static_node(source_node)
        self.G.set_head(source_node)

        sink = SpringSinkElement()
        sink_node = SpringNode(SpringElemenType.SINK, "", sink)
        self.G.add_static_node(sink_node)
        self.G.set_sink(sink_node)

        raw_count = 0
        for _, raw_settings in self.producers.items():
            for _, prd in raw_settings.items():
                new_source = SpringRawSourceElement(
                    data_id=f"{prd.producer_name}:{prd.output_topic.output_topic}",
                    settings=prd,
                )
                n_node = SpringNode(
                    node_type=SpringElemenType.RAWOUTPUT,
                    d_id=prd.producer_name,
                    ele=new_source,
                )
                self.G.add_static_node(n_node)
                self.G.add_dummy_edge(source_node, n_node)
                raw_count += 1

        cons_count = 0
        for cons_d_id, cons_topics in self.consumers.items():
            for cons_topic_name, cons_topic_setting in cons_topics.items():
                new_cons_node = SpringConsumerElement(
                    data_id=f"{cons_d_id}:{cons_topic_name}",
                    settings=cons_topic_setting,
                )
                n_node = SpringNode(
                    node_type=SpringElemenType.CONSUMER,
                    d_id=cons_topic_setting.host_name,
                    ele=new_cons_node,
                )
                self.G.add_static_node(n_node)
                self.G.add_dummy_edge(n_node, sink_node)
                cons_count += 1

        if self.G.node_count != (1 + 1 + raw_count + cons_count):
            raise Exception("Static node generation filed to generate proper nodes!")

    def determine_initial_assignments(self, topology: Topology):

        print(
            "Finding initial optimal nodes for the tasks with consideration of the static producers."
        )
        print("Each task becomes the producer of the next task in the flow graph.")
        ordered_task_nodes = topology.get_topologically_ordered_executor_nodes()
        on: TopologyNode
        for on in ordered_task_nodes:

            self.G.init_event_limit(on.name)

            cep_task: CEPTask = on.node_data

            input_topic_names: List[str] = [
                rt.input_topic for rt in cep_task.settings.required_sub_tasks
            ]
            output_topic_name: str = cep_task.settings.output_topic.output_topic

            node_relations: List[Tuple[SpringNode, str, bool]] = []
            unrelated_nodes: List[str] = deepcopy(self.G.worker_device_ids)

            for itn in input_topic_names:

                evnet_topic_parents = self.G.find_related_parents(itn)
                for tc in evnet_topic_parents:

                    rel = (tc, itn, True)
                    if rel in node_relations:
                        raise Exception("Duplicate relationship should not happen!")

                    node_relations.append(rel)

                    if tc.d_id in unrelated_nodes:
                        unrelated_nodes.remove(tc.d_id)

            topic_children = self.G.find_related_children(output_topic_name)
            for tp in topic_children:

                rel = (tp, output_topic_name, False)
                if rel in node_relations:
                    raise Exception("Duplicate relationship should not happen!")

                node_relations.append(rel)

                if tp.d_id in unrelated_nodes:
                    unrelated_nodes.remove(tp.d_id)

            if len(node_relations) == 0:
                raise Exception("At least one relation should always exists!")

            new_event_info = SpringEventElement(event_id=on.name, cep_task=cep_task)

            placement_suggestions: dict[str, float] = {}

            old_locations: List[str] = self.old_event_locations[
                cep_task.settings.action_name
            ]

            for node, _, _ in node_relations:

                if node.d_id in placement_suggestions:
                    continue

                sp_node = SpringNode(
                    node_type=SpringElemenType.EVENTEXECUTION,
                    d_id=node.d_id,
                    ele=new_event_info,
                )
                assumed_weight = self.get_assumed_weight_for_theoretical_assignment(
                    sp_node, node_relations, old_locations
                )
                if assumed_weight == -1.0:
                    continue
                placement_suggestions[sp_node.d_id] = assumed_weight

            for un_d_id in unrelated_nodes:

                if un_d_id in placement_suggestions:
                    continue

                sp_node = SpringNode(
                    node_type=SpringElemenType.EVENTEXECUTION,
                    d_id=un_d_id,
                    ele=new_event_info,
                )
                assumed_weight = self.get_assumed_weight_for_theoretical_assignment(
                    sp_node, node_relations, old_locations
                )
                if assumed_weight == -1.0:
                    continue
                placement_suggestions[sp_node.d_id] = assumed_weight

            sorted_placements: dict[str, float] = dict(
                sorted(placement_suggestions.items(), key=lambda item: item[1])
            )

            skip_load_check: bool = False
            if len(list(sorted_placements.keys())) == 0:
                for w_key in self.G.worker_device_ids:
                    w_load: float = self.G.get_device_loads(w_key)
                    sorted_placements[w_key] = w_load

                sorted_placements: dict[str, float] = dict(
                    sorted(sorted_placements.items(), key=lambda item: item[1])
                )
                skip_load_check = True

            print(
                f"number of sorted: {len(list(sorted_placements.keys()))}, {skip_load_check}"
            )

            n_initial_assignment_count: int = 1
            initially_assigned_count: int = 0

            for d_id, _ in sorted_placements.items():

                if self.G.device_limit_reached(
                    d_id, skip_load_check
                ) or self.G.event_parallelization_limit_reached(
                    new_event_info.cep_task.settings.action_name
                ):
                    continue

                if initially_assigned_count == n_initial_assignment_count:
                    continue

                sp_node = SpringNode(
                    node_type=SpringElemenType.EVENTEXECUTION,
                    d_id=d_id,
                    ele=new_event_info,
                )

                self.G.add_executor_node(sp_node)

                for o_node, o_topic, is_parent in node_relations:

                    if is_parent:

                        topic_event_byte: float = (
                            self.mssa.get_expected_topic_load_per_second(
                                cep_task.settings.action_name, o_topic, False
                            )
                        )
                        if o_node.d_id == sp_node.d_id:
                            topic_event_byte = 0.0

                        topic_cost = self.get_event_input_topic_weight(
                            cep_task.settings.action_name,
                            o_topic,
                            o_node.d_id != sp_node.d_id,
                        )

                        if (
                            sp_node.d_id not in old_locations
                            and configs.RELAX_USE_OSCILLATION_PENALTY
                        ):
                            topic_event_byte = (
                                topic_event_byte * self.oscillation_penalty
                            )

                        weight = (
                            topic_cost * self.oscillation_penalty
                            if sp_node.d_id not in old_locations
                            else topic_cost
                        )

                        self.G.add_edge(
                            o_node,
                            sp_node,
                            weight=weight,
                            topic=o_topic,
                            expected_event_byte=topic_event_byte,
                        )

                    else:

                        if o_node.node_type == SpringElemenType.CONSUMER:
                            cons_ele: CEPConsumerSettings = o_node.ele.settings

                            topic_event_byte: float = (
                                self.consumerstats.get_expected_load_per_second(
                                    cons_ele.host_name, o_topic, False
                                )
                            )
                            if o_node.d_id == sp_node.d_id:
                                topic_event_byte = 0.0
                            topic_cost = self.get_consumer_input_weight(
                                o_node.d_id, o_topic, o_node.d_id != sp_node.d_id
                            )
                        elif o_node.node_type == SpringElemenType.EVENTEXECUTION:
                            succ_ele: CEPTask = o_node.ele.cep_task

                            topic_event_byte: float = (
                                self.mssa.get_expected_topic_load_per_second(
                                    succ_ele.settings.action_name, o_topic, False
                                )
                            )
                            if o_node.d_id == sp_node.d_id:
                                topic_event_byte = 0.0

                            topic_cost = self.get_event_input_topic_weight(
                                succ_ele.settings.action_name,
                                o_topic,
                                o_node.d_id != sp_node.d_id,
                            )
                        else:
                            raise Exception("Invalid node type detected!")

                        weight = (
                            topic_cost * self.oscillation_penalty
                            if sp_node.d_id not in old_locations
                            else topic_cost
                        )

                        if (
                            sp_node.d_id not in old_locations
                            and configs.RELAX_USE_OSCILLATION_PENALTY
                        ):
                            topic_event_byte = (
                                topic_event_byte * self.oscillation_penalty
                            )

                        self.G.add_edge(
                            sp_node,
                            o_node,
                            weight=weight,
                            topic=o_topic,
                            expected_event_byte=topic_event_byte,
                        )

                initially_assigned_count += 1

            if initially_assigned_count == 0:
                raise Exception("All events should be assigned initially!")

        initial_bpd = self.G.get_size()
        print(f"Network weight sum after initial distribution is: {initial_bpd}")

        self.validate_assignment_dag(self.G)

        return self

    def get_assumed_weight_for_theoretical_assignment(
        self,
        sp_node: SpringNode,
        node_relations: List[Tuple[SpringNode, str, bool]],
        old_locations: List[str],
    ) -> float:

        if self.G.device_limit_reached(sp_node.d_id):
            return -1.0

        self.G.add_executor_node(sp_node)

        for o_node, o_topic, is_parent in node_relations:
            if is_parent:

                topic_event_byte: float = self.mssa.get_expected_topic_load_per_second(
                    sp_node.ele.cep_task.settings.action_name, o_topic, False
                )
                if o_node.d_id == sp_node.d_id:
                    topic_event_byte = 0.0

                topic_cost = self.get_event_input_topic_weight(
                    sp_node.ele.cep_task.settings.action_name,
                    o_topic,
                    o_node.d_id != sp_node.d_id,
                )

                weight = (
                    topic_cost * self.oscillation_penalty
                    if sp_node.d_id not in old_locations
                    else topic_cost
                )

                if (
                    sp_node.d_id not in old_locations
                    and configs.RELAX_USE_OSCILLATION_PENALTY
                ):
                    topic_event_byte = topic_event_byte * self.oscillation_penalty

                self.G.add_edge(
                    o_node,
                    sp_node,
                    weight=weight,
                    topic=o_topic,
                    expected_event_byte=topic_event_byte,
                )
            else:

                if o_node.node_type == SpringElemenType.CONSUMER:
                    cons_ele: CEPConsumerSettings = o_node.ele.settings

                    topic_event_byte: float = (
                        self.consumerstats.get_expected_load_per_second(
                            cons_ele.host_name, o_topic, False
                        )
                    )
                    if o_node.d_id == sp_node.d_id:
                        topic_event_byte = 0.0
                    topic_cost = self.get_consumer_input_weight(
                        o_node.d_id, o_topic, o_node.d_id != sp_node.d_id
                    )
                elif o_node.node_type == SpringElemenType.EVENTEXECUTION:
                    succ_ele: CEPTask = o_node.ele.cep_task

                    topic_event_byte: float = (
                        self.mssa.get_expected_topic_load_per_second(
                            succ_ele.settings.action_name, o_topic, False
                        )
                    )
                    if o_node.d_id == sp_node.d_id:
                        topic_event_byte = 0.0

                    topic_cost = self.get_event_input_topic_weight(
                        succ_ele.settings.action_name,
                        o_topic,
                        o_node.d_id != sp_node.d_id,
                    )
                else:
                    raise Exception("Invalid node type detected!")

                weight = (
                    topic_cost * self.oscillation_penalty
                    if sp_node.d_id not in old_locations
                    else topic_cost
                )

                if (
                    sp_node.d_id not in old_locations
                    and configs.RELAX_USE_OSCILLATION_PENALTY
                ):
                    topic_event_byte = topic_event_byte * self.oscillation_penalty

                self.G.add_edge(
                    sp_node,
                    o_node,
                    weight=weight,
                    topic=o_topic,
                    expected_event_byte=topic_event_byte,
                )

        eventual_assumed_weight = self.G.get_size()

        for o_node, o_topic, is_parent in node_relations:
            if is_parent:
                self.G.remove_edge(o_node, sp_node)
            else:
                self.G.remove_edge(sp_node, o_node)
        self.G.remove_executor_node(sp_node)

        return eventual_assumed_weight

    def iteration_solver(self):

        for _ in range(configs.RELAX_SPRING_ITERATION_COUNT):

            self.iteration_solver_ease_springs()

            if configs.RELAX_INCLUDE_SWAPPING_STEP:
                self.iteration_swap_events()

    def iteration_swap_events(self):
        print("Iterating for swapping events....")
        all_events = self.G.get_topologically_ordered_nodes(reverse=True)
        for spn in self.G.get_topologically_ordered_nodes(reverse=True):
            if spn.node_type != SpringElemenType.EVENTEXECUTION:
                continue

            spn_data: CEPTask = spn.ele.cep_task
            swapping_valid: bool = False
            swapping_target: SpringNode = spn

            crr_assumed_weight: float = self.G.get_size()

            for other_event in all_events:

                if other_event.node_type != SpringElemenType.EVENTEXECUTION:
                    continue

                other_data: CEPTask = other_event.ele.cep_task
                if spn_data.settings.action_name == other_data.settings.action_name:
                    continue

                assumed_weight = self.check_swapping(spn, other_event, revert=True)

                if assumed_weight < crr_assumed_weight:
                    swapping_valid = True
                    swapping_target = other_event
                    assumed_weight = crr_assumed_weight

            if swapping_valid:
                print("==== Better swapping detected ====")
                self.check_swapping(spn, swapping_target, revert=False)

    def update_predecessor_relationship(
        self, node: SpringNode, preds: List[SpringNode]
    ):

        cep_node: CEPTask = node.ele.cep_task

        old_locations: List[str] = self.old_event_locations[
            cep_node.settings.action_name
        ]

        for n in preds:

            topic_name: str = self.G.G[n][node]["topic"]
            topic_cost: float = self.get_event_input_topic_weight(
                cep_node.settings.action_name, topic_name, n.d_id != node.d_id
            )
            topic_event_byte: float = self.mssa.get_expected_topic_load_per_second(
                cep_node.settings.action_name, topic_name, False
            )
            if n.d_id == node.d_id:
                topic_event_byte = 0.0

            if node.d_id not in old_locations and configs.RELAX_USE_OSCILLATION_PENALTY:
                topic_event_byte = topic_event_byte * self.oscillation_penalty

            weight = (
                topic_cost * self.oscillation_penalty
                if node.d_id not in old_locations
                else topic_cost
            )

            self.G.G[n][node]["weight"] = weight

            self.G.G[n][node]["expected_event_byte"] = topic_event_byte

    def update_successor_relationship(self, node: SpringNode, succs: List[SpringNode]):

        cep_node: CEPTask = node.ele.cep_task

        old_locations: List[str] = self.old_event_locations[
            cep_node.settings.action_name
        ]

        for n in succs:

            topic_name = self.G.G[node][n]["topic"]

            if n.node_type == SpringElemenType.CONSUMER:
                cons_ele: CEPConsumerSettings = n.ele.settings

                topic_event_byte: float = (
                    self.consumerstats.get_expected_load_per_second(
                        cons_ele.host_name, topic_name, False
                    )
                )
                if n.d_id == node.d_id:
                    topic_event_byte = 0.0

                topic_cost = self.get_consumer_input_weight(
                    n.d_id, topic_name, n.d_id != node.d_id
                )
            elif n.node_type == SpringElemenType.EVENTEXECUTION:
                succ_ele: CEPTask = n.ele.cep_task

                topic_event_byte: float = self.mssa.get_expected_topic_load_per_second(
                    succ_ele.settings.action_name, topic_name, False
                )
                if n.d_id == node.d_id:
                    topic_event_byte = 0.0

                topic_cost = self.get_event_input_topic_weight(
                    succ_ele.settings.action_name, topic_name, n.d_id != node.d_id
                )
            else:
                raise Exception("Invalid node type detected!")

            weight = (
                topic_cost * self.oscillation_penalty
                if node.d_id not in old_locations
                else topic_cost
            )

            if node.d_id not in old_locations and configs.RELAX_USE_OSCILLATION_PENALTY:
                topic_event_byte = topic_event_byte * self.oscillation_penalty

            self.G.G[node][n]["weight"] = weight

            self.G.G[node][n]["expected_event_byte"] = topic_event_byte

    def check_swapping(
        self, source_node: SpringNode, target_node: SpringNode, revert: bool
    ) -> float:
        source_preds = self.G.get_predecessors(source_node)
        source_succs = self.G.get_successors(source_node)

        target_preds = self.G.get_predecessors(target_node)
        target_succs = self.G.get_successors(target_node)

        source_d_id = deepcopy(source_node.d_id)
        target_d_id = deepcopy(target_node.d_id)

        source_node.d_id = target_d_id
        target_node.d_id = source_d_id

        self.update_predecessor_relationship(source_node, source_preds)
        self.update_successor_relationship(source_node, source_succs)

        self.update_predecessor_relationship(target_node, target_preds)
        self.update_successor_relationship(target_node, target_succs)

        assumed_weight = self.G.get_size()

        if revert:
            self.check_swapping(source_node, target_node, revert=False)

        return assumed_weight

    def iteration_solver_ease_springs(self):
        print("Iterating for force optimization....")

        for spn in self.G.get_topologically_ordered_nodes(reverse=True):
            if spn.node_type != SpringElemenType.EVENTEXECUTION:
                continue

            see: SpringEventElement = spn.ele

            force_comparer: dict[str, float] = {}

            crr_assumed_weight = self.G.get_size()

            force_comparer[spn.d_id] = crr_assumed_weight

            preds: List[SpringNode] = self.G.get_predecessors(spn)
            succs: List[SpringNode] = self.G.get_successors(spn)

            existing_event_locations = self.G.get_event_locations(
                see.cep_task.settings.action_name
            )

            for p in preds:

                if (
                    p.d_id == spn.d_id
                    or p.d_id in force_comparer
                    or p.d_id in existing_event_locations
                ):
                    continue

                if self.G.device_limit_reached(p.d_id):
                    continue

                old_id = deepcopy(spn.d_id)
                assumed_force = self.calculate_assumed_force(
                    spn, preds, succs, p.d_id, old_id, revert=True
                )

                force_comparer[p.d_id] = assumed_force

            for p in succs:

                if (
                    p.d_id == spn.d_id
                    or p.d_id in force_comparer
                    or p.d_id in existing_event_locations
                ):
                    continue

                if self.G.device_limit_reached(p.d_id):
                    continue

                old_id = deepcopy(spn.d_id)
                assumed_force = self.calculate_assumed_force(
                    spn, preds, succs, p.d_id, old_id, revert=True
                )

                force_comparer[p.d_id] = assumed_force

            if len(force_comparer) == 0:
                raise Exception("Comparison list cannot be empty!")

            sorted_placements: dict[str, float] = dict(
                sorted(force_comparer.items(), key=lambda item: item[1])
            )

            assigned_device: str
            assigned_device, _ = next(iter(sorted_placements.items()))

            if assigned_device != spn.d_id:
                print("===== A node assignment has changed vie easining =====")
                old_id = deepcopy(spn.d_id)
                self.calculate_assumed_force(
                    spn, preds, succs, assigned_device, old_id, revert=False
                )

                self.G.device_limits[assigned_device] += 1
                self.G.device_limits[old_id] -= 1

        return self

    def calculate_assumed_force(
        self,
        node: SpringNode,
        preds: List[SpringNode],
        succs: List[SpringNode],
        new_d_id: str,
        old_d_id: str,
        revert: bool,
    ):

        if len(preds) == 0 or len(succs) == 0:
            raise Exception("Number of predecessors or successor cannot be 0!")

        node.d_id = new_d_id

        self.update_predecessor_relationship(node, preds)

        self.update_successor_relationship(node, succs)

        assumed_weight = self.G.get_size()

        if revert:
            self.calculate_assumed_force(node, preds, succs, old_d_id, "", revert=False)

        return assumed_weight

    def validate_assignment_dag(self, connection_validation_graph: SpringGraph):

        print("Validating the DAG relationship...")
        number_of_sink_edge_added = 0
        u: SpringNode
        for u in connection_validation_graph.G.nodes:
            if (
                not list(connection_validation_graph.G.successors(u))
                and u != connection_validation_graph.Sink
            ):
                connection_validation_graph.G.add_edge(
                    u, connection_validation_graph.Sink
                )
                number_of_sink_edge_added += 1

        if number_of_sink_edge_added > 0:
            raise Exception(
                "Sink connections should have been added with the static method!"
            )

        is_weakly_connected = nx.is_weakly_connected(connection_validation_graph.G)
        print("Is graph weakly connected: ", is_weakly_connected)
        is_directed = connection_validation_graph.G.is_directed()
        print("Is graph directed: ", is_directed)
        is_acyclic = nx.is_directed_acyclic_graph(connection_validation_graph.G)
        print("Is graph directed acyclic: ", is_acyclic)

        if (
            len(
                nx.bfs_tree(
                    connection_validation_graph.G, connection_validation_graph.Head
                )
            )
            != connection_validation_graph.node_count
        ):
            raise Exception(
                f"Not all nodes are between the source and the sink, number of nodes: {connection_validation_graph.node_count}, current number of connected nodes: {len(connection_validation_graph.G.nodes)}"
            )

        if not is_directed or not is_acyclic or not is_weakly_connected:
            print(
                "Graph needs to be directed acyclic and all nodes should connect to each other!"
            )
            raise Exception(
                "Graph needs to be directed acyclic and all nodes should connect to each other!"
            )

    def determine_distribution(
        self,
    ) -> tuple[
        List[TaskAlterationModel],
        List[RawUpdateEvent],
        List[CEPConsumerUpdateEvent],
        dict[str, List[str]],
        float,
    ]:
        print("===============================")
        print("Determning the distributions...")
        final_size: float = self.G.get_size(print_log=True)

        print("===============================")
        print(f"bw limit: {configs.CUMULATIVE_LOAD_LIMIT}")
        print("Final device loads")
        finalized_loads: dict[str, float] = self.G.get_all_device_loads()
        _, uplink_loads, downlink_loads = self.G.get_entire_load()
        for fl_id, fl_load in finalized_loads.items():
            print(f"Device: {fl_id}, load: {fl_load}")
        print("===============================")

        print(f"Final size: {final_size}")
        print("===============================")

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []
        consumer_alterations: List[CEPConsumerUpdateEvent] = []
        new_distribution_history: dict[str, List[str]] = {}
        device_task_counts: dict[str, int] = {}

        for w_key, _ in self.workers.items():
            device_task_counts[w_key] = 0

        n_tasks_activated = 0
        n_tasks_deactivated = 0
        n_consumer_requests = 0

        task_activated_devices: dict[str, List[str]] = {}
        for spn in self.G.get_topologically_ordered_nodes():
            if spn.node_type == SpringElemenType.RAWOUTPUT:
                n_data: SpringRawSourceElement = spn.ele

                producer: RawSettings = n_data.settings
                producer.output_topic.target_databases = [spn.d_id]

                producer_target_db_update = RawUpdateEvent()
                producer_target_db_update.producer_name = producer.producer_name
                producer_target_db_update.output_topic = producer.output_topic
                producer_target_db_update.raw_name = producer.raw_data_name

                succs = self.G.get_successors(spn)

                if not succs:
                    raise Exception("Each raw source should have a successor!")

                succ: SpringNode
                topic_target = f"{producer.output_topic.output_topic}"
                for succ in succs:
                    if succ.node_type == SpringElemenType.EVENTEXECUTION:
                        if configs.LOGS_ACTIVE:
                            print(f"Proessing event succesor: {succ.d_id}")
                        see: SpringEventElement = succ.ele
                        for rt in see.cep_task.settings.required_sub_tasks:
                            if rt.input_topic == producer.output_topic.output_topic:
                                if topic_target not in rt.subscription_topics:
                                    rt.subscription_topics.append(topic_target)
                    elif succ.node_type == SpringElemenType.CONSUMER:
                        if configs.LOGS_ACTIVE:
                            print(f"Proessing consumer succesor: {succ.d_id}")
                        sce: SpringConsumerElement = succ.ele
                        if (
                            producer.output_topic.output_topic
                            != sce.settings.topic_name
                        ):
                            raise Exception(
                                "Invalid topic connection exists in the graph!"
                            )
                        if topic_target not in sce.settings.source_topics:
                            sce.settings.source_topics.append(topic_target)

                if not producer.output_topic.target_databases:
                    raise Exception(
                        f"Output targets cannot be empty:{producer.output_topic.output_topic}:{producer.producer_name}:{producer.raw_data_name}!"
                    )

                producer_updates.append(producer_target_db_update)
                if configs.LOGS_ACTIVE:
                    print(
                        f"Assigning production: {producer.output_topic.output_topic} - {producer.raw_data_name}, to: {producer.producer_name}"
                    )

            if spn.node_type == SpringElemenType.EVENTEXECUTION:

                ele_data: SpringEventElement = spn.ele
                task = deepcopy(ele_data.cep_task)
                task.settings.output_topic.target_databases = [spn.d_id]

                device_task_counts[spn.d_id] += 1

                succs = self.G.get_successors(spn)

                if not succs:
                    raise Exception("Each raw source should have a successor!")

                succ: SpringNode
                topic_target = f"{task.settings.output_topic.output_topic}"
                for succ in succs:
                    if succ.node_type == SpringElemenType.EVENTEXECUTION:
                        if configs.LOGS_ACTIVE:
                            print(f"Proessing event succesor: {succ.d_id}")
                        see: SpringEventElement = succ.ele
                        for rt in see.cep_task.settings.required_sub_tasks:
                            if (
                                rt.input_topic
                                == task.settings.output_topic.output_topic
                            ):
                                if topic_target not in rt.subscription_topics:
                                    rt.subscription_topics.append(topic_target)
                    elif succ.node_type == SpringElemenType.CONSUMER:
                        if configs.LOGS_ACTIVE:
                            print(f"Proessing consumer succesor: {succ.d_id}")
                        sce: SpringConsumerElement = succ.ele
                        if (
                            task.settings.output_topic.output_topic
                            != sce.settings.topic_name
                        ):
                            raise Exception(
                                "Invalid topic connection exists in the graph!"
                            )
                        if topic_target not in sce.settings.source_topics:
                            sce.settings.source_topics.append(topic_target)

                alteration = TaskAlterationModel()
                alteration.host = spn.d_id
                alteration.activate = True
                alteration.job_name = task.settings.action_name
                alteration.cep_task = task
                alteration.migration_requests = []
                alteration.only_migration = False
                alterations.append(alteration)
                n_tasks_activated += 1

                if task.settings.action_name not in task_activated_devices:
                    task_activated_devices[task.settings.action_name] = []
                task_activated_devices[task.settings.action_name].append(spn.d_id)

                if task.settings.action_name not in new_distribution_history:
                    new_distribution_history[task.settings.action_name] = []
                if spn.d_id not in new_distribution_history[task.settings.action_name]:
                    new_distribution_history[task.settings.action_name].append(spn.d_id)

                if configs.LOGS_ACTIVE:
                    print(
                        f"Assigning task: {task.settings.action_name}, to: {spn.d_id}"
                    )

            if spn.node_type == SpringElemenType.CONSUMER:

                cons_data: SpringConsumerElement = spn.ele
                alteration = CEPConsumerUpdateEvent()
                alteration.host = cons_data.settings.host_name
                alteration.topic_name = cons_data.settings.topic_name
                alteration.topics = cons_data.settings.source_topics

                if len(alteration.topics) == 0:
                    raise Exception("Invalid consumer assignment detected!")

                consumer_alterations.append(alteration)
                n_consumer_requests += 1

                if configs.LOGS_ACTIVE:
                    print(
                        f"Assigning consumer: {cons_data.settings.host_name}:{cons_data.settings.topic_name}, from: {alteration.topics}"
                    )

        for d_id, counts in device_task_counts.items():
            print(f"Device: {d_id}, count: {counts}")
            if counts > configs.DEVICE_ACTION_LIMIT:
                raise Exception("Invalid number of actions!")

        if configs.LOGS_ACTIVE:
            print("Task activation status: ", task_activated_devices)

        for td, activated_devices in task_activated_devices.items():
            if configs.LOGS_ACTIVE:
                print(
                    f"activated devices: {activated_devices}, all devices: {self.G.worker_device_ids}"
                )
            for w in self.G.worker_device_ids:
                if w not in activated_devices:

                    deactivation = TaskAlterationModel()
                    deactivation.host = w
                    deactivation.activate = False
                    deactivation.job_name = td
                    deactivation.migration_requests = []
                    deactivation.only_migration = False
                    alterations.append(deactivation)
                    n_tasks_deactivated += 1

        print(
            "Number of tasks activated is: ",
            n_tasks_activated,
            " deactivated: ",
            n_tasks_deactivated,
        )

        max_load_perc: float = get_max_percentage(uplink_loads, downlink_loads)

        return (
            alterations,
            producer_updates,
            consumer_alterations,
            new_distribution_history,
            max_load_perc,
        )


def relaxation(
    topology: Topology,
    producers: dict[str, dict[str, RawSettings]],
    workers: dict[str, dict[str, int | float]],
    event_consumers: dict[str, dict[str, CEPConsumerSettings]],
    cep_tasks: List[CEPTask],
    rawstats: RawServerStatAnalyzer,
    mssa: ManagementServerStatisticsAnalyzer,
    consumerstats: ConsumerServerStatics,
    old_distribution_history: dict,
    mopa_cost: bool = False,
) -> tuple[
    List[TaskAlterationModel],
    List[RawUpdateEvent],
    List[CEPConsumerUpdateEvent],
    dict[str, List[str]],
    float,
]:
    print("----------------------------------------")
    print("Running Relaxation algorithm...")

    if not workers:
        return [], [], [], old_distribution_history, 0.0

    rs = RelaxationSolver(
        topology,
        producers,
        workers,
        event_consumers,
        cep_tasks,
        rawstats,
        mssa,
        consumerstats,
        old_distribution_history,
        mopa_cost,
    )
    rs.iteration_solver()
    return rs.determine_distribution()
