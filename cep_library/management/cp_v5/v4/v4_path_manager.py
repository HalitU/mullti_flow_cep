from typing import List
from ortools.sat.python import cp_model

from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library import configs
from cep_library.management.cp_v5.common.cp_sat_event_info import CPSATV3EventInfo
from cep_library.management.cp_v5.common.cp_sat_raw_source import CPSATV3RawSource
from cep_library.management.cp_v5.v4.v4_assignment_vars import (
    CPSATV4AssignmentVariables,
)
from cep_library.management.cp_v5.v4.v4_device_limits import CPSATV4DeviceLimits
from cep_library.management.model.topology import Topology


class CPSATV4PathManager:
    def __init__(
        self,
        model: cp_model.CpModel,
        variables: CPSATV4AssignmentVariables,
        events: dict[str, CPSATV3EventInfo],
        raws: dict[str, CPSATV3RawSource],
        consumers: dict[str, dict[str, CEPConsumerSettings]],
        topology: Topology,
        worker_device_ids: List[str],
        cpv: CPSATV4DeviceLimits,
    ) -> None:
        self.model: cp_model.CpModel = model
        self.variables = variables
        self.events = events
        self.raws = raws
        self.consumers = consumers
        self.related_events: dict[str, List[CPSATV3EventInfo]] = {}
        self.topology = topology
        self.worker_device_ids = worker_device_ids
        self.cpv = cpv
        self.path_sums: dict[str, List[cp_model.IntVar]] = {}
        self.traversal_records: dict[str, cp_model.IntVar] = {}
        self.minc = 0
        self.maxc = configs.CP_SAT_UPPER_INT_LIMIT

    def objective_func(
        self,
        total_link_cost: cp_model.IntVar,
        max_link_cost: cp_model.IntVar,
        max_connection_cost: cp_model.IntVar,
        global_optimal: bool = False,
    ):
        print("Running the objective function cost preparation....")

        if configs.CP_USE_MAX_CONNECTION_LOAD_COST:
            diff: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "diff")
            self.model.Add(diff == max_connection_cost)
        elif configs.CP_USE_MAX_LINK_LOAD_COST:
            diff: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "diff")
            self.model.Add(diff == max_link_cost)
        elif configs.CP_USE_SUM_LINK_LOAD_COST:
            diff: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "diff")
            self.model.Add(diff == total_link_cost)
        elif configs.CP_OPT_MAX_DEVICE_LOAD:
            diff: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "diff")
            d_loads: List[cp_model.IntVar] = []
            for k, v in self.cpv.worker_device_downlink_load.items():
                d_loads.append(v + self.cpv.worker_device_uplink_load[k])
            self.model.AddMaxEquality(diff, d_loads)
        elif configs.CP_USE_DAG_LINK_LOAD_COST:
            diff: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "diff")
            print("USING DAG BASED COST")
            self.traverse_dag_cost()

            print(f"Number of paths found: {len(list(self.path_sums.keys()))}")

            all_durations: List[cp_model.IntVar] = []
            for _, vals in self.path_sums.items():
                if configs.LOGS_ACTIVE:
                    print("------------------------")
                    print(vals)

                duration: cp_model.IntVar = self.model.NewIntVar(
                    self.minc, self.maxc, "bottleneck"
                )
                self.model.Add(duration == cp_model.LinearExpr.Sum(vals))

                if configs.CP_SAT_USE_HARD_DELAY_LIMITS:
                    self.model.Add(duration <= configs.CP_SAT_MAX_BOTTLENECK_DURATION)

                if configs.CP_USE_DAG_BOTTLENECK:
                    bottleneck: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "bottleneck"
                    )
                    self.model.AddMaxEquality(bottleneck, vals)
                    all_durations.append(bottleneck + duration)
                else:
                    all_durations.append(duration)

            kek: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "ff")
            self.model.AddMaxEquality(kek, all_durations)

            self.model.Add(diff == kek)
        else:
            raise Exception("Invalid operation")

        self.model.Minimize(diff)
        return diff

    def traverse_dag_cost(self) -> None:

        for raw_id, raw_topics in self.variables.raw_source_target_costs.items():

            for raw_topic, raw_topic_devices in raw_topics.items():
                costs: List[cp_model.IntVar] = []

                raw_costs: List[cp_model.IntVar] = []
                for _, link_cost in raw_topic_devices.items():
                    raw_costs.append(link_cost)

                if not configs.KEEP_RAWDATA_AT_SOURCE:
                    crr_raw_topic_link_cost: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, f"raw:{raw_id}:{raw_topic}"
                    )
                    self.model.AddMaxEquality(crr_raw_topic_link_cost, raw_costs)

                    costs.append(crr_raw_topic_link_cost)
                path_id = f"{raw_id}:{raw_topic}"

                if raw_topic not in self.related_events:
                    self.related_events[raw_topic] = [
                        event
                        for _, event in self.events.items()
                        if event.topic_exists(raw_topic)
                    ]
                related_events = self.related_events[raw_topic]

                for re in related_events:
                    self.traverse_reading_path_costs(
                        costs, re.event_id, raw_topic, path_id
                    )

                costs = []

    def traverse_reading_path_costs(
        self,
        accumulative_costs: List[cp_model.IntVar],
        event_id: str,
        topic_id: str,
        path_id: str,
    ):

        node_id: str = f"{event_id}:{topic_id}"
        if node_id not in self.traversal_records:
            costs: List[cp_model.IntVar] = self.variables.event_reading_location_costs[
                event_id
            ][topic_id]
            crr_topic_reading_cost: cp_model.IntVar = self.model.NewIntVar(
                self.minc, self.maxc, "c"
            )
            self.model.AddMaxEquality(crr_topic_reading_cost, costs)
            self.traversal_records[node_id] = crr_topic_reading_cost
        crr_topic_reading_cost = self.traversal_records[node_id]

        accumulative_costs.append(crr_topic_reading_cost)

        path_ref: str = f"{path_id}:{event_id}:{topic_id}"

        self.traverse_writing_path_costs(accumulative_costs, event_id, path_ref)

        del accumulative_costs[-1]

    def traverse_writing_path_costs(
        self, accumulative_costs: List[cp_model.IntVar], event_id: str, path_id: str
    ):

        for topic_id, out_t_devices in self.variables.event_output_location_costs[
            event_id
        ].items():

            node_id = f"{event_id}:{topic_id}"
            if node_id not in self.traversal_records:
                crr_out_topic_cost: cp_model.IntVar = self.model.NewIntVar(
                    self.minc, self.maxc, f"e_write:{event_id}:{topic_id}"
                )
                self.model.AddMaxEquality(crr_out_topic_cost, out_t_devices)
                self.traversal_records[node_id] = crr_out_topic_cost
            crr_out_topic_cost = self.traversal_records[node_id]

            if topic_id not in self.related_events:
                self.related_events[topic_id] = [
                    event
                    for _, event in self.events.items()
                    if event.topic_exists(topic_id)
                ]
            related_events: List[CPSATV3EventInfo] = self.related_events[topic_id]

            path_ref: str = f"{path_id}:{event_id}:{topic_id}"
            for re in related_events:

                accumulative_costs.append(crr_out_topic_cost)

                self.traverse_reading_path_costs(
                    accumulative_costs, re.event_id, topic_id, path_ref
                )

                del accumulative_costs[-1]

            accumulative_costs.append(crr_out_topic_cost)

            self.traverse_consumer_reading_path_cost(
                accumulative_costs, topic_id, path_ref
            )

            del accumulative_costs[-1]

    def traverse_consumer_reading_path_cost(
        self, accumulative_costs: List[cp_model.IntVar], topic: str, path_id: str
    ):
        original_length = len(accumulative_costs)
        for consumer_id, consumer_topics in self.consumers.items():

            if topic not in consumer_topics:
                continue

            node_id = f"{consumer_id}:{topic}"
            if node_id not in self.traversal_records:
                costs: List[cp_model.IntVar] = []
                for _, cs in self.variables.event_consumer_reading_costs[consumer_id][
                    topic
                ].items():
                    costs.append(cs)
                consumer_reading_cost: cp_model.IntVar = self.model.NewIntVar(
                    self.minc, self.maxc, f"cons:{consumer_id}:{topic}"
                )
                self.model.AddMaxEquality(consumer_reading_cost, costs)
                self.traversal_records[node_id] = consumer_reading_cost
            consumer_reading_cost = self.traversal_records[node_id]

            accumulative_costs.append(consumer_reading_cost)

            print(
                f"Consumer: {consumer_id}:{topic}, number of accumulative costs: {len(accumulative_costs)}"
            )

            path_ref = f"{path_id}:{consumer_id}:{topic}"
            if path_ref in self.path_sums:
                raise Exception("Duplicate traversal detected!")
            self.path_sums[path_ref] = accumulative_costs.copy()

            del accumulative_costs[-1]

            if len(accumulative_costs) != original_length:
                raise Exception("Invalid path traversal detected")
