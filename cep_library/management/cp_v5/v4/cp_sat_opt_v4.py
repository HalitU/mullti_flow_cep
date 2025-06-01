from typing import List
from ortools.sat.python import cp_model
from cep_library import configs
from cep_library.cep.model.cep_task import CEPTask
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.cp_v5.common.cp_sat_optimizer import CPSATV3Optimizer
from cep_library.management.cp_v5.common.cp_sat_cost_adapter import CPSATV3CostAdapter
from cep_library.management.cp_v5.common.cp_sat_event_info import CPSATV3EventInfo
from cep_library.management.cp_v5.common.cp_sat_raw_source import CPSATV3RawSource
from cep_library.management.cp_v5.v4.v4_assignment_vars import (
    CPSATV4AssignmentVariables,
)
from cep_library.management.cp_v5.v4.v4_constraint_manager import V4ConstraintManager
from cep_library.management.cp_v5.v4.v4_device_limits import CPSATV4DeviceLimits
from cep_library.management.cp_v5.v4.v4_distribution import CPSATV4Distribuion
from cep_library.management.cp_v5.v4.v4_path_manager import CPSATV4PathManager
from cep_library.management.model.topology import Topology, TopologyNode
from cep_library.management.statistics.consumer_server_statics import (
    ConsumerServerStatics,
)
from cep_library.management.statistics.load_helper import get_max_percentage
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import (
    ManagementServerStatisticsAnalyzer,
)
from cep_library.raw.model.raw_settings import RawSettings


class CPSATSTATS:
    max_recorded_load: float = 0.0
    max_recorded_uplink_load: float = 0.0
    max_recorded_downlink_load: float = 0.0
    max_recorded_path_duration: float = 0.0


class CPSATV5FlowOptimizer:
    def __init__(self) -> None:
        pass

    def pipe_init_common_vars(self, workers: dict[str, dict[str, int | float]]) -> None:

        self.model: cp_model.CpModel = cp_model.CpModel()

        self.all_device_ids: List[str] = [d_id for d_id in workers.keys()]

    def optimize_flow(
        self,
        topology: Topology,
        producers: dict[str, dict[str, RawSettings]],
        workers: dict[str, dict[str, int | float]],
        event_consumers: dict[str, dict[str, CEPConsumerSettings]],
        cep_tasks: List[CEPTask],
        rawstats: RawServerStatAnalyzer,
        mssa: ManagementServerStatisticsAnalyzer,
        consumerstats: ConsumerServerStatics,
        old_distribution_history: dict,
        global_optimal_opt: bool = False,
    ):

        cca = CPSATV3CostAdapter()
        cca.adaptCosts(
            producers, rawstats, cep_tasks, mssa, event_consumers, consumerstats
        )
        self.pipe_init_common_vars(workers)

        raw_sources: dict[str, CPSATV3RawSource] = {}
        prd: RawSettings
        for raw_host, raw_settings in producers.items():
            for _, prd in raw_settings.items():
                currently_stored_devices: list[str] = []

                for target_db in prd.output_topic.target_databases:
                    if target_db not in currently_stored_devices:
                        currently_stored_devices.append(target_db)

                p_top_node: TopologyNode = topology.get_producer_node(
                    raw_host, prd.output_topic.output_topic
                )
                if p_top_node.flow_id == 0:
                    raise Exception("Invalid flow id!")

                new_source = CPSATV3RawSource(
                    device_id=prd.producer_name,
                    output_topic=prd.output_topic.output_topic,
                    settings=prd,
                    currently_stored_devices=currently_stored_devices,
                    flow_id=p_top_node.flow_id,
                )

                raw_sources[new_source.get_key()] = new_source

                historical_currently_stored_devices: list[str] = []
                if (
                    "raw_sensors" in old_distribution_history
                    and new_source.get_key() in old_distribution_history["raw_sensors"]
                ):
                    for _, raw_output_topic_devices in old_distribution_history[
                        "raw_sensors"
                    ][new_source.get_key()].items():
                        for rotd_device in raw_output_topic_devices:
                            if rotd_device not in historical_currently_stored_devices:
                                historical_currently_stored_devices.append(rotd_device)

                if len(historical_currently_stored_devices) > 0:
                    new_source.currently_stored_devices = (
                        historical_currently_stored_devices
                    )

                else:
                    print(
                        f"No currently stored device for raw: {new_source.device_id} - {new_source.settings.output_topic.output_topic}"
                    )

        print("Generated raw source information")

        event_information: dict[str, CPSATV3EventInfo] = {}
        exec_n: TopologyNode
        for exec_n in topology.get_topologically_ordered_executor_nodes():

            event_historical_data = {}
            if "event_sensors" in old_distribution_history:
                if len(list(old_distribution_history["event_sensors"].keys())) > 0:
                    for hist_worker_id, hist_events in old_distribution_history[
                        "event_sensors"
                    ].items():
                        if exec_n.name in hist_events:
                            event_historical_data[hist_worker_id] = hist_events[
                                exec_n.name
                            ]

            if not event_historical_data:
                print(f"No historical data found for event: {exec_n.name}")

            exec_cep_task: CEPTask = exec_n.node_data
            new_event_info = CPSATV3EventInfo(
                event_id=exec_n.name,
                cep_task=exec_cep_task,
                historical_data=event_historical_data,
            )

            event_information[new_event_info.get_key()] = new_event_info

        print("Generated event information")

        cav = CPSATV4AssignmentVariables(
            worker_device_ids=self.all_device_ids,
            events=event_information,
            raws=raw_sources,
            model=self.model,
            consumers=event_consumers,
            executable_device_limit=configs.ALLOWED_PARALLEL_EXECUTION_COUNT,
        )

        print("Generated all variables")

        _ = V4ConstraintManager(
            events=event_information,
            model=self.model,
            raws=raw_sources,
            variables=cav,
            all_device_ids=self.all_device_ids,
            consumers=event_consumers,
            topology=topology,
        )

        print("[MainFlow] Finished V4ConstraintManager")

        cpv = CPSATV4DeviceLimits(
            model=self.model,
            model_variables=cav,
            device_ids=self.all_device_ids,
            raw_settings=raw_sources,
            event_information=event_information,
            consumers=event_consumers,
            topology=topology,
            rawstats=rawstats,
            mssa=mssa,
            consumerstats=consumerstats,
        )
        (
            total_link_cost,
            max_link_cost,
            worker_device_uplink_vars,
            worker_device_downlink_vars,
            max_connection_cost,
        ) = cpv.prepareBandwidthLimitBalancer()

        print("Generated device loads and corresponding link costs")

        print("-------------------------------")

        cpm = CPSATV4PathManager(
            raws=raw_sources,
            model=self.model,
            events=event_information,
            variables=cav,
            consumers=event_consumers,
            topology=topology,
            worker_device_ids=self.all_device_ids,
            cpv=cpv,
        )
        obj_val = cpm.objective_func(
            total_link_cost,
            max_link_cost,
            max_connection_cost,
            global_optimal=global_optimal_opt,
        )

        print("[MainFlow] Finished CPSATPathManagerv4")

        opt = CPSATV3Optimizer(self.model)
        solver, solution_found = opt.optimize()

        if not solution_found:
            print(f"No solution found....")
            return [], []

        normalized_bw_limit: float = (
            configs.CUMULATIVE_LOAD_LIMIT / configs.CP_SAT_DIVIDER
        )

        if configs.CP_USE_TWO_STEP_DAG_LINK_LOAD_COST:
            configs.CP_USE_MAX_CONNECTION_LOAD_COST = False
            configs.CP_USE_DAG_LINK_LOAD_COST = True

            max_conn_cost_val: int = solver.Value(max_connection_cost)
            print("===========================")
            print(
                f"Max recorded connection load in the previous step was: {max_conn_cost_val}"
            )
            print(
                f"Max recorded connection load ratio was: {(max_conn_cost_val * 100.0) / (normalized_bw_limit / 2.0)}"
            )
            print("===========================")

            self.model.Add(max_connection_cost <= max_conn_cost_val)
            obj_val = cpm.objective_func(
                total_link_cost,
                max_link_cost,
                max_connection_cost,
                global_optimal=global_optimal_opt,
            )

            print("[MainFlow] Finished CPSATPathManagerv4")

            opt = CPSATV3Optimizer(self.model)
            solver, solution_found = opt.optimize()

            if not solution_found:
                print(f"No solution found....")
                return [], []

            configs.CP_USE_MAX_CONNECTION_LOAD_COST = True
            configs.CP_USE_DAG_LINK_LOAD_COST = False

        print("================================================")
        print("Diff value: ", solver.Value(obj_val))
        print("Total Link Cost: ", solver.Value(total_link_cost))
        print("Max link cost: ", solver.Value(max_link_cost))
        print("BW limit: ", normalized_bw_limit)
        print("Response stats:", solver.ResponseStats())
        print("================================")
        print("DOWNLINKS")
        uplink_loads: dict[str, float] = {}
        for d_id, d_loads in worker_device_uplink_vars.items():
            total = 0.0
            n_links = 0
            for d_load in d_loads:
                load_val = solver.Value(d_load)
                total += load_val
                n_links += 1 if load_val > 0 else 0
            over_limit: bool = total > normalized_bw_limit / 2.0

            CPSATSTATS.max_recorded_uplink_load = max(
                CPSATSTATS.max_recorded_uplink_load, total
            )

            print(
                f"Device: {d_id}, count: {len(d_loads)} external link cost total: {total}, iis over limit: {over_limit}, link count: {n_links}"
            )

            uplink_loads[d_id] = total

        print("================================")
        print("UPLINKS")
        downlink_loads: dict[str, float] = {}
        for d_id, d_loads in worker_device_downlink_vars.items():
            total = 0.0
            n_links = 0
            for d_load in d_loads:
                load_val = solver.Value(d_load)
                total += load_val
                n_links += 1 if load_val > 0 else 0
            over_limit: bool = total > normalized_bw_limit / 2.0

            CPSATSTATS.max_recorded_downlink_load = max(
                CPSATSTATS.max_recorded_downlink_load, total
            )

            print(
                f"Device: {d_id}, count: {len(d_loads)} external link cost total: {total}, iis over limit: {over_limit}, link count: {n_links}"
            )

            downlink_loads[d_id] = total

        print("------------------------------------------------")
        for psm, psm_values in cpm.path_sums.items():
            total = 0.0
            for vs in psm_values:
                total += solver.Value(vs)
            if configs.LOGS_ACTIVE:
                print(f"Path, cost sum:{total} path: {psm}")
            CPSATSTATS.max_recorded_path_duration = max(
                CPSATSTATS.max_recorded_path_duration, total
            )

        print(f"Maximum recoded load was: {CPSATSTATS.max_recorded_load}")
        print(f"Maximum recoded uplink load was: {CPSATSTATS.max_recorded_uplink_load}")
        print(
            f"Maximum recoded downlink load was: {CPSATSTATS.max_recorded_downlink_load}"
        )
        print(
            f"Maximum recoded path duration was: {CPSATSTATS.max_recorded_path_duration}"
        )
        print("================================================")

        dist = CPSATV4Distribuion(
            all_device_ids=self.all_device_ids,
            events=event_information,
            model=self.model,
            previous_dist_history=old_distribution_history,
            raws=raw_sources,
            solver=solver,
            variables=cav,
            consumers=event_consumers,
        )
        (
            event_alterations,
            raw_updates,
            distribution_history,
            __,
            consumer_alterations,
        ) = dist.determine_distributions()

        if not event_alterations:
            print("No event alterations are generated!")
            return [], [], {}

        if not raw_updates:
            print("No raw updates are generated!")
            return [], [], {}

        print("CP SAT optimization is completed.")

        max_load_perc: float = get_max_percentage(uplink_loads, downlink_loads)

        return (
            event_alterations,
            raw_updates,
            consumer_alterations,
            distribution_history,
            max_load_perc,
        )
