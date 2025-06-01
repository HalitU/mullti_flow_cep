import math
from typing import List, Tuple
from ortools.sat.python import cp_model

from cep_library import configs
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.cp_v5.common.cp_sat_event_info import CPSATV3EventInfo
from cep_library.management.cp_v5.common.cp_sat_raw_source import CPSATV3RawSource
from cep_library.management.cp_v5.v4.v4_assignment_vars import (
    CPSATV4AssignmentVariables,
)
from cep_library.management.model.topology import Topology
from cep_library.management.statistics.consumer_server_statics import (
    ConsumerServerStatics,
)
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import (
    ManagementServerStatisticsAnalyzer,
)


class CPSATV4DeviceLimits:
    def __init__(
        self,
        model: cp_model.CpModel,
        model_variables: CPSATV4AssignmentVariables,
        device_ids: List[str],
        raw_settings: dict[str, CPSATV3RawSource],
        event_information: dict[str, CPSATV3EventInfo],
        consumers: dict[str, dict[str, CEPConsumerSettings]],
        topology: Topology,
        rawstats: RawServerStatAnalyzer,
        mssa: ManagementServerStatisticsAnalyzer,
        consumerstats: ConsumerServerStatics,
    ) -> None:
        print("---------------")
        print(f"Using bw penalty: {configs.CP_SAT_ABOVE_BW_MULTP}")
        print("---------------")

        self.model = model
        self.event_information = event_information

        self.minc = 0
        self.maxc = configs.CP_SAT_UPPER_INT_LIMIT
        self.topology: Topology = topology

        self.all_link_costs: List[cp_model.IntVar] = []

        self.worker_device_uplink_load: dict[str, cp_model.IntVar] = {}
        self.worker_device_uplink_vars: dict[str, List[cp_model.IntVar]] = {}
        self.worker_device_uplink_50_status: dict[str, cp_model.IntVar] = {}

        self.worker_device_downlink_load: dict[str, cp_model.IntVar] = {}
        self.worker_device_downlink_vars: dict[str, List[cp_model.IntVar]] = {}
        self.worker_device_downlink_50_status: dict[str, cp_model.IntVar] = {}

        self.device_ids = device_ids

        self.worker_cpu_loads: dict[str, List[cp_model.IntVar]] = {}

        self.model_variables = model_variables
        self.raw_settings = raw_settings
        self.consumers = consumers

        self.rawstats = rawstats
        self.mssa = mssa
        self.consumerstats = consumerstats

        self.uplink_delays: dict[str, cp_model.IntVar] = {}
        self.downlink_delays: dict[str, cp_model.IntVar] = {}

        self.bw_limit_int: int = math.ceil(
            configs.CUMULATIVE_LOAD_LIMIT / configs.CP_SAT_DIVIDER
        )
        self.up_bw_limit_int: int = math.ceil(self.bw_limit_int / 2)
        self.down_bw_limit_int: int = math.ceil(self.bw_limit_int / 2)

        for d_id in device_ids:
            self.worker_device_uplink_load[d_id] = self.model.NewIntVar(
                self.minc,
                math.ceil(
                    self.bw_limit_int * configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO / 2
                ),
                "dl",
            )
            self.worker_device_uplink_vars[d_id] = []

            self.worker_device_downlink_load[d_id] = self.model.NewIntVar(
                self.minc,
                math.ceil(
                    self.bw_limit_int * configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO / 2
                ),
                "dl",
            )
            self.worker_device_downlink_vars[d_id] = []

            self.worker_cpu_loads[d_id] = []

    def generate_device_load_penalty_var(self):
        for wdl in self.device_ids:

            uplink_50: cp_model.IntVar = self.model.NewBoolVar("v")
            self.model.Add(
                self.worker_device_uplink_load[wdl]
                >= math.ceil(configs.CP_SAT_BW_FIRST_TH * self.up_bw_limit_int)
            ).OnlyEnforceIf(uplink_50)
            self.model.Add(
                self.worker_device_uplink_load[wdl]
                < math.ceil(configs.CP_SAT_BW_FIRST_TH * self.up_bw_limit_int)
            ).OnlyEnforceIf(uplink_50.Not())

            downlink_50: cp_model.IntVar = self.model.NewBoolVar("v")
            self.model.Add(
                self.worker_device_downlink_load[wdl]
                >= math.ceil(configs.CP_SAT_BW_FIRST_TH * self.down_bw_limit_int)
            ).OnlyEnforceIf(downlink_50)
            self.model.Add(
                self.worker_device_downlink_load[wdl]
                < math.ceil(configs.CP_SAT_BW_FIRST_TH * self.down_bw_limit_int)
            ).OnlyEnforceIf(downlink_50.Not())

            self.worker_device_uplink_50_status[wdl] = uplink_50
            self.worker_device_downlink_50_status[wdl] = downlink_50

    def get_expected_delay_penalty_double_layer(
        self,
        is_uplink: bool,
        device_id: str,
        byte_load: int,
        device_load_limit: int,
        exists_var: cp_model.IntVar,
    ) -> cp_model.IntVar:
        if configs.CP_SAT_USE_OVERALL_DELAY_COST:
            if is_uplink:
                if device_id in self.uplink_delays:
                    delay = self.uplink_delays[device_id]
                else:
                    delay: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "oc"
                    )
                    self.model.AddDivisionEquality(
                        delay,
                        self.worker_device_uplink_load[device_id]
                        * configs.CP_SAT_TIME_MULTIPLIER,
                        self.up_bw_limit_int,
                    )

                    finalized_delay: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "oc"
                    )
                    self.model.Add(
                        finalized_delay == configs.CP_SAT_ABOVE_BW_MULTP * delay
                    ).OnlyEnforceIf(self.worker_device_uplink_50_status[device_id])
                    self.model.Add(finalized_delay == delay).OnlyEnforceIf(
                        self.worker_device_uplink_50_status[device_id].Not()
                    )

                    self.uplink_delays[device_id] = finalized_delay
            else:
                if device_id in self.downlink_delays:
                    delay = self.downlink_delays[device_id]
                else:
                    delay: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "oc"
                    )
                    self.model.AddDivisionEquality(
                        delay,
                        self.worker_device_downlink_load[device_id]
                        * configs.CP_SAT_TIME_MULTIPLIER,
                        self.down_bw_limit_int,
                    )

                    finalized_delay: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "oc"
                    )
                    self.model.Add(
                        finalized_delay == configs.CP_SAT_ABOVE_BW_MULTP * delay
                    ).OnlyEnforceIf(self.worker_device_downlink_50_status[device_id])
                    self.model.Add(finalized_delay == delay).OnlyEnforceIf(
                        self.worker_device_downlink_50_status[device_id].Not()
                    )

                    self.downlink_delays[device_id] = finalized_delay

            overall_cost: cp_model.IntVar = self.model.NewIntVar(
                self.minc, self.maxc, "oc"
            )
            self.model.AddMultiplicationEquality(overall_cost, delay, exists_var)
            return overall_cost

        raise Exception("Invalid setting detected!")

    def get_expected_delay_penalty(
        self,
        is_uplink: bool,
        device_id: str,
        byte_load: int,
        device_load_limit: int,
        exists_var: cp_model.IntVar,
    ) -> cp_model.IntVar:
        return self.get_expected_delay_penalty_double_layer(
            is_uplink=is_uplink,
            device_id=device_id,
            byte_load=byte_load,
            device_load_limit=device_load_limit,
            exists_var=exists_var,
        )

    def get_load_cost(
        self,
        exists_var: cp_model.IntVar,
        byte_load: int,
        source_device_id: str,
        target_device_id: str,
    ) -> cp_model.IntVar:
        source_expected_delay = self.get_expected_delay_penalty(
            is_uplink=True,
            device_id=source_device_id,
            byte_load=byte_load,
            device_load_limit=self.up_bw_limit_int,
            exists_var=exists_var,
        )

        target_expected_delay = self.get_expected_delay_penalty(
            is_uplink=False,
            device_id=target_device_id,
            byte_load=byte_load,
            device_load_limit=self.down_bw_limit_int,
            exists_var=exists_var,
        )

        max2: cp_model.IntVar = self.model.NewIntVar(self.minc, self.maxc, "med")
        self.model.AddMaxEquality(max2, [source_expected_delay, target_expected_delay])
        return max2

    def prepareBandwidthLimitBalancer(
        self,
    ) -> Tuple[
        cp_model.IntVar,
        cp_model.IntVar,
        dict[str, List[cp_model.IntVar]],
        dict[str, List[cp_model.IntVar]],
        cp_model.IntVar,
    ]:
        self.checkExecutionCPULoad()
        self.generate_device_load_penalty_var()
        self.checkRawOutputBandwidth()
        self.checkExecutionInputBandwidth()
        self.checkExecutionOutputBandwidth()
        self.checkConsumerBandwidth()
        return self.prepareBandwidthConstraints()

    def checkExecutionCPULoad(self):

        for e_id, e_settings in self.event_information.items():
            total_load: int = 0
            for rti in e_settings.cep_task.settings.required_sub_tasks:
                expected_cpu_load: int = math.ceil(
                    self.mssa.get_expected_event_input_counts(
                        e_settings.cep_task.settings.action_name, rti.input_topic
                    )
                )
                if expected_cpu_load <= 0:
                    raise Exception("INVALID LOAD COUNT DETECTED!!!")
                total_load += expected_cpu_load

            for d_id, exec_var in self.model_variables.event_execution_location_vars[
                e_id
            ].items():
                self.worker_cpu_loads[d_id].append(exec_var * expected_cpu_load)

    def checkExecutionInputBandwidth(self):
        for e_id, e_topics in self.model_variables.event_reading_locations.items():
            crr_event_data: CPSATV3EventInfo = self.event_information[e_id]
            crr_execution_devices: List[str] = (
                crr_event_data.get_output_topic_currently_written_devices()
            )

            for topic_id, topic_devices in e_topics.items():
                crr_read_locations: List[str] = (
                    crr_event_data.get_input_topic_locations(topic_id)
                )

                byte_load: float = (
                    self.mssa.get_expected_topic_load_per_second(
                        crr_event_data.cep_task.settings.action_name, topic_id, False
                    )
                    * configs.CP_READ_MULTIPLIER
                ) / configs.CP_SAT_LINK_DIVIDER

                print(f"Event input: {e_id}, {topic_id}, byte load: {byte_load}")

                for d_id, read_var in topic_devices.items():

                    if (
                        configs.CP_USE_OSCILLATION_PENALTY
                        and d_id not in crr_read_locations
                    ):
                        current_byte_load: float = (
                            byte_load * configs.CP_OSCILLATION_PENALTY
                        )
                    else:
                        current_byte_load = byte_load

                    if (
                        configs.CP_EVENT_CHANGE_PENALTY_ACTIVE
                        and d_id not in crr_execution_devices
                    ):
                        current_byte_load: float = math.ceil(
                            current_byte_load * configs.CP_EVENT_CHANGE_PENALTY
                        )
                    else:
                        current_byte_load = math.ceil(current_byte_load)

                    exec_var = self.model_variables.event_execution_location_vars[e_id][
                        d_id
                    ]

                    uplink_exists = self.model.NewBoolVar("ls")
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var.Not(), read_var]) == 2
                    ).OnlyEnforceIf(uplink_exists)
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var.Not(), read_var]) != 2
                    ).OnlyEnforceIf(uplink_exists.Not())

                    downlink_exists = self.model.NewBoolVar("ls")
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var, read_var.Not()]) == 2
                    ).OnlyEnforceIf(downlink_exists)
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var, read_var.Not()]) != 2
                    ).OnlyEnforceIf(downlink_exists.Not())

                    self.worker_device_uplink_vars[d_id].append(
                        uplink_exists * current_byte_load
                    )
                    self.worker_device_downlink_vars[d_id].append(
                        downlink_exists * current_byte_load
                    )

                    if configs.CP_OPT_MAX_DEVICE_LOAD:
                        continue

                    source_expected_delay = self.get_expected_delay_penalty(
                        is_uplink=True,
                        device_id=d_id,
                        byte_load=current_byte_load,
                        device_load_limit=self.up_bw_limit_int,
                        exists_var=uplink_exists,
                    )

                    target_expected_delay = self.get_expected_delay_penalty(
                        is_uplink=False,
                        device_id=d_id,
                        byte_load=current_byte_load,
                        device_load_limit=self.down_bw_limit_int,
                        exists_var=downlink_exists,
                    )

                    link_cost: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "med"
                    )
                    self.model.AddMaxEquality(
                        link_cost, [source_expected_delay, target_expected_delay]
                    )

                    self.model_variables.event_reading_location_costs[e_id][
                        topic_id
                    ].append(link_cost)

                    if not configs.CP_USE_DAG_LINK_LOAD_COST:
                        self.all_link_costs.append(link_cost)

    def checkConsumerBandwidth(self):
        for (
            consumer_id,
            consumer_topics,
        ) in self.model_variables.event_consumer_location_vars.items():
            for consumer_topic_id, topic_devices in consumer_topics.items():
                consumer_device_id = self.consumers[consumer_id][
                    consumer_topic_id
                ].host_name

                expected_cpu_load: int = math.ceil(
                    self.consumerstats.get_expected_event_counts(
                        consumer_id, consumer_topic_id
                    )
                )
                if expected_cpu_load <= 0:
                    raise Exception("INVALID LOAD COUNT DETECTED!!!")
                cpu_load: cp_model.IntVar = self.model.NewIntVar(
                    expected_cpu_load, expected_cpu_load, "cpu_load"
                )
                self.model.Add(cpu_load == expected_cpu_load)
                self.worker_cpu_loads[consumer_device_id].append(cpu_load)

                byte_load = math.ceil(
                    (
                        self.consumerstats.get_expected_load_per_second(
                            consumer_id, consumer_topic_id, False
                        )
                        * configs.CP_READ_MULTIPLIER
                    )
                    / configs.CP_SAT_LINK_DIVIDER
                )
                print(
                    f"Consumer: {consumer_id}, {consumer_topic_id}, byte load: {byte_load}"
                )

                for device_id, consumer_read_var in topic_devices.items():

                    if device_id == consumer_device_id:
                        continue

                    self.worker_device_uplink_vars[device_id].append(
                        consumer_read_var * byte_load
                    )
                    self.worker_device_downlink_vars[consumer_device_id].append(
                        consumer_read_var * byte_load
                    )

                    if configs.CP_OPT_MAX_DEVICE_LOAD:
                        continue

                    link_cost: cp_model.IntVar = self.get_load_cost(
                        byte_load=byte_load,
                        exists_var=consumer_read_var,
                        source_device_id=device_id,
                        target_device_id=consumer_device_id,
                    )

                    self.model_variables.event_consumer_reading_costs[consumer_id][
                        consumer_topic_id
                    ][device_id] = link_cost

                    if not configs.CP_USE_DAG_LINK_LOAD_COST:

                        self.all_link_costs.append(link_cost)

    def checkExecutionOutputBandwidth(self):
        for e_id, e_topics in self.model_variables.event_output_location_vars.items():
            crr_event_data: CPSATV3EventInfo = self.event_information[e_id]
            crr_written_devices: List[str] = (
                crr_event_data.get_output_topic_currently_written_devices()
            )
            crr_execution_devices: List[str] = (
                crr_event_data.get_output_topic_currently_written_devices()
            )

            for e_topic, e_devices in e_topics.items():

                out_byte: float = (
                    self.mssa.get_expected_output_topic_load_per_second(
                        crr_event_data.cep_task.settings.action_name, e_topic, False
                    )
                    * configs.CP_WRITE_MULTIPLIER
                ) / configs.CP_SAT_LINK_DIVIDER

                print(f"Event output: {e_id}, {e_topic}, byte load: {out_byte}")

                for device_id, write_var in e_devices.items():

                    if (
                        configs.CP_USE_OSCILLATION_PENALTY
                        and device_id not in crr_written_devices
                    ):
                        current_byte_load: float = (
                            out_byte * configs.CP_OSCILLATION_PENALTY
                        )
                    else:
                        current_byte_load = out_byte

                    if (
                        configs.CP_EVENT_CHANGE_PENALTY_ACTIVE
                        and device_id not in crr_execution_devices
                    ):
                        current_byte_load: float = math.ceil(
                            current_byte_load * configs.CP_EVENT_CHANGE_PENALTY
                        )
                    else:
                        current_byte_load = math.ceil(current_byte_load)

                    exec_var = self.model_variables.event_execution_location_vars[e_id][
                        device_id
                    ]

                    uplink_exists = self.model.NewBoolVar("ls")
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var, write_var.Not()]) == 2
                    ).OnlyEnforceIf(uplink_exists)
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var, write_var.Not()]) != 2
                    ).OnlyEnforceIf(uplink_exists.Not())

                    downlink_exists = self.model.NewBoolVar("ls")
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var.Not(), write_var]) == 2
                    ).OnlyEnforceIf(downlink_exists)
                    self.model.Add(
                        cp_model.LinearExpr.Sum([exec_var.Not(), write_var]) != 2
                    ).OnlyEnforceIf(downlink_exists.Not())

                    self.worker_device_uplink_vars[device_id].append(
                        uplink_exists * current_byte_load
                    )
                    self.worker_device_downlink_vars[device_id].append(
                        downlink_exists * current_byte_load
                    )

                    if configs.CP_OPT_MAX_DEVICE_LOAD:
                        continue

                    source_expected_delay = self.get_expected_delay_penalty(
                        is_uplink=True,
                        device_id=device_id,
                        byte_load=current_byte_load,
                        device_load_limit=self.up_bw_limit_int,
                        exists_var=uplink_exists,
                    )

                    target_expected_delay = self.get_expected_delay_penalty(
                        is_uplink=False,
                        device_id=device_id,
                        byte_load=current_byte_load,
                        device_load_limit=self.down_bw_limit_int,
                        exists_var=downlink_exists,
                    )

                    link_cost: cp_model.IntVar = self.model.NewIntVar(
                        self.minc, self.maxc, "med"
                    )
                    self.model.AddMaxEquality(
                        link_cost, [source_expected_delay, target_expected_delay]
                    )

                    self.model_variables.event_output_location_costs[e_id][
                        e_topic
                    ].append(link_cost)

                    if not configs.CP_USE_DAG_LINK_LOAD_COST:
                        self.all_link_costs.append(link_cost)

    def checkRawOutputBandwidth(self):
        for raw_id, raw_topics in self.model_variables.raw_source_targets.items():
            raw_source_device_id = self.raw_settings[raw_id].device_id
            currently_stored_devices: List[str] = self.raw_settings[
                raw_id
            ].currently_stored_devices
            for topic_id, topic_devices in raw_topics.items():

                expected_cpu_load: int = math.ceil(
                    self.rawstats.get_expected_event_count(
                        self.raw_settings[raw_id].settings.producer_name, topic_id
                    )
                )
                if expected_cpu_load <= 0:
                    raise Exception("INVALID LOAD COUNT DETECTED!!!")
                cpu_load: cp_model.IntVar = self.model.NewIntVar(
                    expected_cpu_load, expected_cpu_load, "cpu_load"
                )
                self.model.Add(cpu_load == expected_cpu_load)
                self.worker_cpu_loads[raw_source_device_id].append(cpu_load)

                byte_load = (
                    self.rawstats.get_expected_load_per_second(
                        self.raw_settings[raw_id].settings.producer_name,
                        topic_id,
                        False,
                    )
                    * configs.CP_WRITE_MULTIPLIER
                ) / configs.CP_SAT_LINK_DIVIDER

                print(f"Raw: {raw_id}, {topic_id}, byte load: {byte_load}")

                for device_id, write_device_var in topic_devices.items():

                    if device_id == raw_source_device_id:
                        continue

                    if (
                        configs.CP_USE_OSCILLATION_PENALTY
                        and device_id not in currently_stored_devices
                    ):
                        current_byte_load: float = math.ceil(
                            byte_load * configs.CP_OSCILLATION_PENALTY
                        )
                    else:
                        current_byte_load = math.ceil(byte_load)

                    if configs.LOGS_ACTIVE:
                        print(
                            f"{raw_id}, {topic_id}, byte load: {current_byte_load}, crr written devices: {currently_stored_devices}"
                        )

                    self.worker_device_uplink_vars[raw_source_device_id].append(
                        write_device_var * current_byte_load
                    )
                    self.worker_device_downlink_vars[device_id].append(
                        write_device_var * current_byte_load
                    )

                    if configs.CP_OPT_MAX_DEVICE_LOAD:
                        continue

                    link_cost: cp_model.IntVar = self.get_load_cost(
                        byte_load=current_byte_load,
                        exists_var=write_device_var,
                        source_device_id=raw_source_device_id,
                        target_device_id=device_id,
                    )

                    self.model_variables.raw_source_target_costs[raw_id][topic_id][
                        device_id
                    ] = link_cost

                    if not configs.CP_USE_DAG_LINK_LOAD_COST:
                        self.all_link_costs.append(link_cost)

    def prepareBandwidthConstraints(
        self,
    ) -> Tuple[
        cp_model.IntVar,
        cp_model.IntVar,
        dict[str, List[cp_model.IntVar]],
        dict[str, List[cp_model.IntVar]],
        cp_model.IntVar,
    ]:

        if not configs.CP_USE_DAG_LINK_LOAD_COST and not configs.CP_OPT_MAX_DEVICE_LOAD:
            device_total_link_loads: List[cp_model.IntVar] = []
        for load_d_id, load_vars in self.worker_device_uplink_vars.items():
            if len(load_vars) == 0:
                raise Exception("All devices should have contraint variables.")
            print(f"Number of load variables: {load_d_id}, {len(load_vars)}")
            self.model.Add(
                self.worker_device_uplink_load[load_d_id]
                == cp_model.LinearExpr.Sum(load_vars)
            )

            if (
                not configs.CP_USE_DAG_LINK_LOAD_COST
                and not configs.CP_OPT_MAX_DEVICE_LOAD
            ):
                device_total_link_loads.append(
                    self.worker_device_uplink_load[load_d_id]
                )

        for load_d_id, load_vars in self.worker_device_downlink_vars.items():
            if len(load_vars) == 0:
                raise Exception("All devices should have contraint variables.")
            print(f"Number of load variables: {load_d_id}, {len(load_vars)}")
            self.model.Add(
                self.worker_device_downlink_load[load_d_id]
                == cp_model.LinearExpr.Sum(load_vars)
            )

            if (
                not configs.CP_USE_DAG_LINK_LOAD_COST
                and not configs.CP_OPT_MAX_DEVICE_LOAD
            ):
                device_total_link_loads.append(
                    self.worker_device_downlink_load[load_d_id]
                )

        if not configs.CP_USE_DAG_LINK_LOAD_COST and not configs.CP_OPT_MAX_DEVICE_LOAD:

            max_link_cost: cp_model.IntVar = self.model.NewIntVar(
                self.minc, self.maxc, "max_link_load"
            )
            self.model.AddMaxEquality(max_link_cost, self.all_link_costs)

            total_link_cost: cp_model.IntVar = self.model.NewIntVar(
                self.minc, self.maxc, "total_load"
            )
            self.model.Add(
                total_link_cost == cp_model.LinearExpr.Sum(self.all_link_costs)
            )

            max_connection_cost: cp_model.IntVar = self.model.NewIntVar(
                self.minc, self.maxc, "max_connection_cost"
            )
            self.model.AddMaxEquality(max_connection_cost, device_total_link_loads)
        else:
            max_link_cost: cp_model.IntVar = self.model.NewIntVar(0, 0, "max_link_load")
            total_link_cost: cp_model.IntVar = self.model.NewIntVar(0, 0, "total_load")
            max_connection_cost: cp_model.IntVar = self.model.NewIntVar(
                0, 0, "max_connection_cost"
            )

        for _, cpu_vars in self.worker_cpu_loads.items():
            self.model.Add(
                cp_model.LinearExpr.Sum(cpu_vars) <= configs.DEVICE_EXECUTION_LIMIT
            )

        return (
            total_link_cost,
            max_link_cost,
            self.worker_device_uplink_vars,
            self.worker_device_downlink_vars,
            max_connection_cost,
        )
