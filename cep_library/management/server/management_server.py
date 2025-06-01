from datetime import datetime, timezone
import math
import pickle
from threading import Lock, Thread
import time
from typing import Any, Callable, List, Tuple

from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_topic_stats import ConsumerTopicStats
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
from cep_library.data import data_helper

from cep_library.management.cp_v5.v4.cp_sat_opt_v4 import CPSATV5FlowOptimizer
from cep_library.management.distribution_algorithms.consumer_preferred import (
    consumer_preferred,
)
from cep_library.management.distribution_algorithms.genetic_algorithm import (
    genetic_algorithm,
)
from cep_library.management.distribution_algorithms.producer_preferred import (
    producer_preferred,
)
from cep_library.management.distribution_algorithms.random_distribution import (
    random_distribution,
)
from fastapi import FastAPI, Request
import requests
from cep_library.management.distribution_algorithms.relaxation import relaxation
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.cep.model.cep_task import CEPTask
from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.task_alteration import (
    TaskAlterationBulk,
    TaskAlterationModel,
)
from cep_library.management.statistics.consumer_server_statics import (
    ConsumerServerStatics,
)
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import (
    ManagementServerStatisticsAnalyzer,
)
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer
from cep_library.raw.model.raw_settings import RawSettings, RawStatisticsBook
from cep_library.raw.model.raw_update_event import RawUpdateEvent
import cep_library.configs as configs
from fastapi.responses import FileResponse
import networkx as nx
import matplotlib.pyplot as plt


class ServerStateManager:
    def __init__(self) -> None:
        self.killed = False


class ServerStatisticsManager:
    def __init__(self) -> None:
        self.prepareStatisticsFiles()

        self.valid_stats_count = 0

        self.previous_alterations = []
        self.previous_producer_updates = []

        self.record_id_tracker = 1
        self.raw_distribution_row_tracker = 1
        self.event_distribution_row_tracker = 1

        self.stat_write_available = True

    def addOrUpdateHostStats(self, resource_consumption: ResourceUsageRecord) -> None:

        current_server_time = datetime.now().strftime("%m.%d.%Y %H.%M.%S.%f")

        device_stats: List[str] = resource_consumption.get_device_stats(
            self.record_id_tracker
        )
        device_stats.append(current_server_time)
        self.print_device_stats(",".join(device_stats))

        for task_stat in resource_consumption.get_task_stats(self.record_id_tracker):
            crr_task_stat: List[str] = task_stat
            crr_task_stat.append(current_server_time)
            self.print_task_stats(",".join(crr_task_stat))

        for task_topic_stat in resource_consumption.get_task_topics_stats(
            self.record_id_tracker
        ):
            crr_task_topic_stat = task_topic_stat
            crr_task_topic_stat.append(current_server_time)
            self.print_task_topic_stats(",".join(crr_task_topic_stat))

        self.record_id_tracker += 1

    def prepareStatisticsFiles(self) -> None:
        stat_headers = ResourceUsageRecord()

        self.stats_file = "stats/device_statistics.csv"
        with open(self.stats_file, "w") as outfile:
            outfile.write(",".join(stat_headers.get_device_stat_header()) + "\n")
        outfile.close()

        self.task_stats_file = "stats/task_statistics.csv"
        with open(self.task_stats_file, "w") as outfile:
            outfile.write(",".join(stat_headers.get_task_stats_header()) + "\n")
        outfile.close()

        self.task_topic_stats_file = "stats/task_topic_stats_file.csv"
        with open(self.task_topic_stats_file, "w") as outfile:
            outfile.write(
                ",".join(stat_headers.get_task_topic_stats_file_header()) + "\n"
            )
        outfile.close()

        self.server_stats_file = "stats/server_statistics.csv"
        with open(self.server_stats_file, "w") as outfile:
            outfile.write(",".join(stat_headers.get_server_stat_header()) + "\n")
        outfile.close()

        self.raw_data_distributions_file = "stats/raw_data_distributions.csv"
        with open(self.raw_data_distributions_file, "w") as outfile:
            outfile.write(
                ",".join(stat_headers.get_raw_production_distribution_header()) + "\n"
            )
        outfile.close()

        self.event_distributions_file = "stats/event_distributions.csv"
        with open(self.event_distributions_file, "w") as outfile:
            outfile.write(
                ",".join(stat_headers.get_event_distributions_header()) + "\n"
            )
        outfile.close()

        self.execution_distribution_file = "stats/execution_distribution_file.csv"
        execution_distribution_headers = [
            "row_id",
            "current_date",
            "sim_date",
            "host_name",
            "n_distribution",
            "n_output_target",
        ]
        with open(self.execution_distribution_file, "w") as outfile:
            outfile.write(",".join(execution_distribution_headers) + "\n")
        outfile.close()

    def reset_stat_files(self):
        print("Resetting statistic files for future simulations...")
        self.stat_write_available = False

        time.sleep(10.0)

        stat_headers = ResourceUsageRecord()

        with open(self.stats_file, "w") as outfile:
            outfile.write(",".join(stat_headers.get_device_stat_header()) + "\n")
        outfile.close()

        with open(self.task_stats_file, "w") as outfile:
            outfile.write(",".join(stat_headers.get_task_stats_header()) + "\n")
        outfile.close()

        with open(self.task_topic_stats_file, "w") as outfile:
            outfile.write(
                ",".join(stat_headers.get_task_topic_stats_file_header()) + "\n"
            )
        outfile.close()

        with open(self.server_stats_file, "w") as outfile:
            outfile.write(",".join(stat_headers.get_server_stat_header()) + "\n")
        outfile.close()

        with open(self.raw_data_distributions_file, "w") as outfile:
            outfile.write(
                ",".join(stat_headers.get_raw_production_distribution_header()) + "\n"
            )
        outfile.close()

        with open(self.event_distributions_file, "w") as outfile:
            outfile.write(
                ",".join(stat_headers.get_event_distributions_header()) + "\n"
            )
        outfile.close()

        print("Worker device statistics are cleared.")

        self.distribution_history = {}
        self.valid_stats_count = 0
        self.previous_alterations = []
        self.previous_producer_updates = []

        self.raw_distribution_row_tracker = 1
        self.event_distribution_row_tracker = 1
        self.stat_write_available = True

    def print_device_stats(self, json_str: str) -> None:
        if self.stat_write_available == False:
            return
        with open(self.stats_file, "a") as outfile:
            outfile.write(json_str + "\n")
        outfile.close()

    def print_task_stats(self, json_str: str) -> None:
        if self.stat_write_available == False:
            return
        with open(self.task_stats_file, "a") as outfile:
            outfile.write(json_str + "\n")
        outfile.close()

    def print_task_topic_stats(self, json_str: str) -> None:
        if self.stat_write_available == False:
            return
        with open(self.task_topic_stats_file, "a") as outfile:
            outfile.write(json_str + "\n")
        outfile.close()

    def print_server_stats(self, json_str: str) -> None:
        if self.stat_write_available == False:
            return
        with open(self.server_stats_file, "a") as outfile:
            outfile.write(json_str + "\n")
        outfile.close()

    def print_raw_production_distributions(
        self, producer_updates: List[RawUpdateEvent]
    ):
        row_id = self.raw_distribution_row_tracker
        current_date = datetime.now(tz=timezone.utc).strftime("%m.%d.%Y %H.%M.%S.%f")

        for producer_update in producer_updates:
            for target_database in producer_update.output_topic.target_databases:
                data_to_be_written = [
                    str(row_id),
                    current_date,
                    configs.env_host_name,
                    producer_update.raw_name,
                    producer_update.output_topic.output_topic,
                    producer_update.producer_name,
                    target_database,
                ]

                with open(self.raw_data_distributions_file, "a") as outfile:
                    outfile.write(",".join(data_to_be_written) + "\n")
                outfile.close()

        self.raw_distribution_row_tracker += 1

    def print_event_distributions(
        self,
        alteration_requests: List[TaskAlterationModel],
        producer_updates: List[RawUpdateEvent],
        consumer_requests: List[CEPConsumerUpdateEvent],
    ) -> None:
        row_id = self.event_distribution_row_tracker
        current_date = datetime.now(tz=timezone.utc).strftime("%m.%d.%Y %H.%M.%S.%f")

        with open(self.event_distributions_file, "a") as outfile:
            outfile.write(f"{current_date} ++++++++++++++++++++++++++++++++++++++\n")

            outfile.write(f"Procuder targets -----------------------------------\n")
            for prod_alter in producer_updates:
                data_to_be_written = [
                    str(row_id),
                    current_date,
                    prod_alter.producer_name,
                    prod_alter.raw_name,
                    prod_alter.output_topic.output_topic,
                ]
                for td in prod_alter.output_topic.target_databases:
                    data_to_be_written.append(td)
                outfile.write(",".join(data_to_be_written) + "\n")

            outfile.write(f"Event activations -----------------------------------\n")
            for alteration_request in alteration_requests:
                if not alteration_request.activate:
                    continue

                for (
                    input_topic
                ) in alteration_request.cep_task.settings.required_sub_tasks:
                    data_to_be_written = [
                        str(row_id),
                        current_date,
                        alteration_request.host,
                        alteration_request.cep_task.settings.action_name,
                        input_topic.input_topic,
                    ] + input_topic.subscription_topics
                    outfile.write(",".join(data_to_be_written) + "\n")

                data_to_be_written = (
                    [
                        str(row_id),
                        current_date,
                        alteration_request.host,
                        alteration_request.cep_task.settings.action_name,
                        alteration_request.cep_task.settings.output_topic.output_topic,
                    ]
                    + alteration_request.cep_task.settings.output_topic.target_databases
                )
                outfile.write(",".join(data_to_be_written) + "\n")

            outfile.write(f"Consumer targets -----------------------------------\n")
            for cons_alter in consumer_requests:
                data_to_be_written: List[str] = [
                    str(row_id),
                    current_date,
                    cons_alter.host,
                    cons_alter.topic_name,
                ] + cons_alter.topics
                outfile.write(",".join(data_to_be_written) + "\n")

            outfile.close()

        self.event_distribution_row_tracker += 1

    def print_distributions(
        self,
        alteration_requests: List[TaskAlterationModel],
        producer_updates: List[RawUpdateEvent],
        consumer_requests: List[CEPConsumerUpdateEvent],
    ) -> None:
        print("-----------------------------------------------------------")
        row_id = self.record_id_tracker
        current_date = datetime.now(tz=timezone.utc).strftime("%m.%d.%Y %H.%M.%S.%f")

        with open(self.execution_distribution_file, "a") as outfile:
            outfile.write("---------------------------------\n")
            hosts: dict[str, dict[str, int]] = {}
            alteration_request: TaskAlterationModel
            for alteration_request in alteration_requests:
                if alteration_request.activate:

                    if alteration_request.host in hosts:
                        if "n_distribution" in hosts[alteration_request.host]:
                            hosts[alteration_request.host]["n_distribution"] += 1
                        else:
                            hosts[alteration_request.host]["n_distribution"] = 1
                    else:
                        hosts[alteration_request.host] = {}
                        hosts[alteration_request.host]["n_distribution"] = 1

                    for (
                        target_db
                    ) in (
                        alteration_request.cep_task.settings.output_topic.target_databases
                    ):
                        if target_db in hosts:
                            if "n_output_target" in hosts[target_db]:
                                hosts[target_db]["n_output_target"] += 1
                            else:
                                hosts[target_db]["n_output_target"] = 1
                        else:
                            hosts[target_db] = {}
                            hosts[target_db]["n_output_target"] = 1

            producer_request: RawUpdateEvent
            for producer_request in producer_updates:
                for target_db in producer_request.output_topic.target_databases:
                    if target_db in hosts:
                        if "n_output_target" in hosts[target_db]:
                            hosts[target_db]["n_output_target"] += 1
                        else:
                            hosts[target_db]["n_output_target"] = 1
                    else:
                        hosts[target_db] = {}
                        hosts[target_db]["n_output_target"] = 1

            if hosts:
                for host in hosts:
                    crr_n_dist = 0
                    if "n_distribution" in hosts[host]:
                        crr_n_dist = hosts[host]["n_distribution"]
                    crr_n_out = 0
                    if "n_output_target" in hosts[host]:
                        crr_n_out = hosts[host]["n_output_target"]

                    sim_tim: str = datetime.now(tz=timezone.utc).strftime(
                        "%m.%d.%Y_%H.%M.%S.%f"
                    )
                    to_out: List[str] = [
                        str(row_id),
                        current_date,
                        sim_tim,
                        host,
                        str(crr_n_dist),
                        str(crr_n_out),
                    ]
                    outfile.write(",".join(to_out) + "\n")
        outfile.close()


class ServerEndpointManager:
    def __init__(
        self,
        app: FastAPI,
        stats_manager: ServerStatisticsManager,
        stop_management: Callable,
    ) -> None:
        self.app: FastAPI = app
        self.server_ready = False
        self.stats_manager: ServerStatisticsManager = stats_manager
        self.stop_management = stop_management

        self.w_ix = 1
        self.workers: dict[str, dict[str, int | float]] = {}
        self.worker_lock = Lock()

        self.p_ix = 1
        self.producers: dict[str, dict[str, RawSettings]] = {}

        self.c_ix = 1
        self.event_consumers: dict[str, dict[str, CEPConsumerSettings]] = {}

    def set_server_as_ready(self) -> None:
        self.server_ready = True

    def register_endpoints(self):
        @self.app.get("/download_device_stats/")
        def _():
            return FileResponse(
                path=self.stats_manager.stats_file, filename="device_stats.csv"
            )

        @self.app.get("/download_task_stats/")
        def _():
            return FileResponse(
                path=self.stats_manager.task_stats_file, filename="task_stats.csv"
            )

        @self.app.get("/download_server_stats/")
        def _():
            return FileResponse(
                path=self.stats_manager.server_stats_file, filename="server_stats.csv"
            )

        @self.app.get("/get_server_action_file/")
        def _(file_path: str):
            file_name = file_path.split("/")[-1] + ".py"
            file_path = file_path.replace(".", "/") + ".py"
            return FileResponse(path=file_path, filename=file_name)

        @self.app.post("/raw_producer/")
        async def _(raw_settings_request: Request):
            request_data: bytes = await raw_settings_request.body()
            raw_settings: RawSettings = pickle.loads(request_data)
            if raw_settings.producer_name not in self.producers:
                print(
                    f"Registering a raw data host: {raw_settings.producer_name}, raw name: {raw_settings.raw_data_name}, topic: {raw_settings.output_topic.output_topic} producer with new id: {self.p_ix}."
                )

                self.worker_lock.acquire()
                self.producers[raw_settings.producer_name] = {}
                self.producers[raw_settings.producer_name][
                    raw_settings.output_topic.output_topic
                ] = raw_settings

                self.p_ix += 1
                print(f"Raw producer registered with id: {self.p_ix}")
                self.worker_lock.release()
            else:
                if (
                    raw_settings.output_topic.output_topic
                    not in self.producers[raw_settings.producer_name]
                ):
                    print(
                        f"Registering to existing raw data host: {raw_settings.producer_name}, raw name: {raw_settings.raw_data_name}, topic name: {raw_settings.output_topic.output_topic} producer with new id: {self.p_ix}."
                    )
                    self.p_ix += 1

                self.worker_lock.acquire()

                self.producers[raw_settings.producer_name][
                    raw_settings.output_topic.output_topic
                ] = raw_settings

                self.worker_lock.release()

            return self.server_ready

        @self.app.post("/event_consumer/")
        async def _(event_consumer_request: Request):
            request_data: bytes = await event_consumer_request.body()
            event_consumer_settings: CEPConsumerSettings = pickle.loads(request_data)
            if event_consumer_settings.host_name not in self.event_consumers:
                print(
                    f"[CONSUMER REGISTRATION] Registering a consumer host {event_consumer_settings.host_name}, topic name: {event_consumer_settings.topic_name}, producer with new id: {self.c_ix}."
                )

                self.worker_lock.acquire()
                self.event_consumers[event_consumer_settings.host_name] = {}
                self.event_consumers[event_consumer_settings.host_name][
                    event_consumer_settings.topic_name
                ] = event_consumer_settings

                self.c_ix += 1
                print(
                    f"[CONSUMER REGISTRATION] Registered a consumer host with id: {self.c_ix}"
                )
                self.worker_lock.release()
            else:
                if (
                    event_consumer_settings.topic_name
                    not in self.event_consumers[event_consumer_settings.host_name]
                ):
                    print(
                        f"[CONSUMER REGISTRATION] Registering to existing event consumer host: {event_consumer_settings.host_name}, topic name: {event_consumer_settings.topic_name}, producer with new id: {self.c_ix}."
                    )
                    self.c_ix += 1

                self.worker_lock.acquire()

                self.event_consumers[event_consumer_settings.host_name][
                    event_consumer_settings.topic_name
                ] = event_consumer_settings

                self.worker_lock.release()

            return self.server_ready

        @self.app.get("/server_ready/")
        def _(
            worker_name: str,
            worker_port: int,
            worker_ram_limit: float = 5000000,
            host_x: int = 100,
            host_y: int = 100,
        ):

            self.worker_lock.acquire()

            if worker_name not in self.workers:
                self.workers[worker_name] = {
                    "worker_port": worker_port,
                    "w_ix": self.w_ix,
                    "worker_ram_limit": worker_ram_limit,
                    "host_x": host_x,
                    "host_y": host_y,
                }
                print("Added new worker: ", worker_name, " with id: ", self.w_ix)
                self.w_ix += 1

            self.worker_lock.release()
            return self.server_ready

        @self.app.get("/get_topology/")
        def _(
            file_path: str = "visuals/task_topology.png",
            filename: str = "task_topology.png",
        ):
            return FileResponse(path=file_path, filename=filename)

        @self.app.get("/get_topology_text/")
        def _(
            file_path: str = "visuals/graph_output.txt",
            filename: str = "graph_output.txt",
        ):
            return FileResponse(path=file_path, filename=filename)

        @self.app.get("startup")
        async def _():
            print("starting up the web server...")

        @self.app.get("shutdown")
        def _():
            print("Shutting down")
            self.stop_management()

        @self.app.get("/download_all_statistics/")
        def _():

            return FileResponse(
                path="current_run_results.zip", filename="current_run_results.zip"
            )


class ServerEventManager:
    def __init__(self, host_name: str) -> None:
        self._host_name: str = host_name

    def setup(self, process_resource_data: Callable) -> None:

        self.alteration_publisher = MQTTProducer(
            client_id=f"server_producer_{configs.kafka_resource_topics[0]}_{self._host_name}"
        )

        self._resource_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_resource_topics[0]}_{self._host_name}",
            core_topic=configs.kafka_resource_topics[0],
            topic_names=configs.kafka_resource_topics,
            target=process_resource_data,
            target_args=None,
            shared_subscription=False,
        )

    def stop_management(self):
        self._resource_consumer.close()
        self.alteration_publisher.close()

    def run(self):

        Thread(daemon=True, target=self.alteration_publisher.run).start()
        Thread(daemon=True, target=self._resource_consumer.run).start()

    def ack(self, _, __):
        pass


class ServerCEPOptimizationManager:
    def __init__(
        self,
        em: ServerEndpointManager,
        sm: ServerStatisticsManager,
        eventm: ServerEventManager,
        process_resource_data,
        cepsm: ServerStateManager,
    ) -> None:
        self.tasks: List[CEPTask] = []

        self.distribution_history = {}
        self.last_min_cost = 1000000

        self.cepsm: ServerStateManager = cepsm

        self.em: ServerEndpointManager = em
        self.sm: ServerStatisticsManager = sm
        self.eventm: ServerEventManager = eventm
        self.process_resource_data = process_resource_data

        self.mssa = ManagementServerStatisticsAnalyzer()
        self.rawstats = RawServerStatAnalyzer()
        self.consumerstats = ConsumerServerStatics()

        self.previous_alterations: List[TaskAlterationModel] = []
        self.previous_producer_updates: List[RawUpdateEvent] = []
        self.previous_consumer_requests: List[CEPConsumerUpdateEvent] = []

    def register_task_for_management(self, task: CEPTask) -> None:
        self.tasks.append(task)

    def get_distribution_action(
        self,
    ) -> Callable[
        [
            Topology,
            dict[str, dict[str, RawSettings]],
            dict[str, dict[str, int | float]],
            dict[str, dict[str, CEPConsumerSettings]],
            List[CEPTask],
            RawServerStatAnalyzer,
            ManagementServerStatisticsAnalyzer,
            ConsumerServerStatics,
            Any,
        ],
        tuple[
            List[TaskAlterationModel],
            List[RawUpdateEvent],
            List[CEPConsumerUpdateEvent],
            Any,
            float,
        ],
    ]:
        match configs.env_distribution_type:
            case 36:
                return random_distribution
            case 38:
                return genetic_algorithm
            case 39:
                return producer_preferred
            case 41:
                return relaxation
            case 42:
                return consumer_preferred
            case 48:
                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow

            case 49:
                configs.RELAX_USE_MAX_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 20
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 50:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 20
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 51:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = True
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 20
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 52:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = True
                configs.RELAX_SPRING_ITERATION_COUNT: int = 20
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 53:
                configs.RELAX_USE_MAX_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 54:
                configs.RELAX_USE_MAX_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 20
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = False
                return relaxation

            case 55:
                configs.GA_USE_MAX_BW_USAGE: bool = True
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False
                return genetic_algorithm
            case 56:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = True
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False
                return genetic_algorithm
            case 57:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = True
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False
                return genetic_algorithm
            case 58:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = True
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False
                return genetic_algorithm
            case 59:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = True
                return genetic_algorithm

            case 60:
                configs.CP_USE_DAG_LINK_LOAD_COST: bool = True
                configs.CP_USE_MAX_CONNECTION_LOAD_COST: bool = False
                configs.CP_USE_MAX_LINK_LOAD_COST: bool = False
                configs.CP_USE_SUM_LINK_LOAD_COST: bool = False
                configs.CP_OPT_MAX_DEVICE_LOAD: bool = False
                configs.CP_SAT_WRITE_AT_EXECUTION_LOC: bool = False
                configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 2.0

                configs.CP_SAT_ABSOLUTE_GAP: float = 0.0001
                configs.CP_SAT_WORKER_COUNT: int = 3
                configs.CP_RUNTIME_SECONDS: int = 15
                configs.CP_SAT_PRESOLVE: bool = True
                configs.CP_SAT_PRESOLVE_ITERATIONS: int = 1

                configs.CP_SAT_BW_FIRST_TH: float = 0.8
                configs.CP_SAT_ABOVE_BW_MULTP: int = 100

                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow
            case 61:
                configs.CP_USE_DAG_LINK_LOAD_COST: bool = False
                configs.CP_USE_MAX_CONNECTION_LOAD_COST: bool = True
                configs.CP_USE_MAX_LINK_LOAD_COST: bool = False
                configs.CP_USE_SUM_LINK_LOAD_COST: bool = False
                configs.CP_OPT_MAX_DEVICE_LOAD: bool = False
                configs.CP_SAT_WRITE_AT_EXECUTION_LOC: bool = True
                configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 2.0
                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow
            case 62:
                configs.CP_USE_DAG_LINK_LOAD_COST: bool = False
                configs.CP_USE_MAX_CONNECTION_LOAD_COST: bool = False
                configs.CP_USE_MAX_LINK_LOAD_COST: bool = True
                configs.CP_USE_SUM_LINK_LOAD_COST: bool = False
                configs.CP_OPT_MAX_DEVICE_LOAD: bool = False
                configs.CP_SAT_WRITE_AT_EXECUTION_LOC: bool = True
                configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 2.0
                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow
            case 63:
                configs.CP_USE_DAG_LINK_LOAD_COST: bool = False
                configs.CP_USE_MAX_CONNECTION_LOAD_COST: bool = False
                configs.CP_USE_MAX_LINK_LOAD_COST: bool = False
                configs.CP_USE_SUM_LINK_LOAD_COST: bool = True
                configs.CP_OPT_MAX_DEVICE_LOAD: bool = False
                configs.CP_SAT_WRITE_AT_EXECUTION_LOC: bool = True
                configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 2.0
                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow
            case 64:
                configs.CP_USE_DAG_LINK_LOAD_COST: bool = False
                configs.CP_USE_MAX_CONNECTION_LOAD_COST: bool = False
                configs.CP_USE_MAX_LINK_LOAD_COST: bool = False
                configs.CP_USE_SUM_LINK_LOAD_COST: bool = False
                configs.CP_OPT_MAX_DEVICE_LOAD: bool = True
                configs.CP_SAT_WRITE_AT_EXECUTION_LOC: bool = True
                configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 2.0
                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow

            case 66:
                configs.CP_USE_TWO_STEP_DAG_LINK_LOAD_COST: bool = True
                configs.CP_USE_MAX_CONNECTION_LOAD_COST: bool = True
                configs.CP_USE_DAG_LINK_LOAD_COST: bool = False
                configs.CP_USE_MAX_LINK_LOAD_COST: bool = False
                configs.CP_USE_SUM_LINK_LOAD_COST: bool = False
                configs.CP_OPT_MAX_DEVICE_LOAD: bool = False
                configs.CP_SAT_WRITE_AT_EXECUTION_LOC: bool = False
                configs.CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 2.0

                configs.CP_SAT_ABSOLUTE_GAP: float = 0.0001
                configs.CP_SAT_WORKER_COUNT: int = 3
                configs.CP_RUNTIME_SECONDS: int = 10
                configs.CP_SAT_PRESOLVE: bool = True
                configs.CP_SAT_PRESOLVE_ITERATIONS: int = 1

                configs.CP_SAT_BW_FIRST_TH: float = 1.0
                configs.CP_SAT_ABOVE_BW_MULTP: int = 10

                sfov5 = CPSATV5FlowOptimizer()
                return sfov5.optimize_flow

            case 67:
                configs.GA_USE_MAX_BW_USAGE: bool = True
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 25
                configs.GA_PARAMS_MUTATION_SIZE = 15
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 68:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = True
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 25
                configs.GA_PARAMS_MUTATION_SIZE = 15
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 69:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = True
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 25
                configs.GA_PARAMS_MUTATION_SIZE = 15
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 70:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = True
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 25
                configs.GA_PARAMS_MUTATION_SIZE = 15
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 71:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = True

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 25
                configs.GA_PARAMS_MUTATION_SIZE = 15
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm

            case 72:
                configs.RELAX_USE_MAX_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = False
                return relaxation
            case 73:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = False
                return relaxation
            case 74:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = True
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = False
                return relaxation
            case 75:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = True
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = False
                return relaxation

            case 77:
                configs.RELAX_USE_MAX_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 78:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = True
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 79:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = True
                configs.RELAX_USE_MAX_LINK_USAGE: bool = False
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation
            case 80:
                configs.RELAX_USE_MAX_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_BW_USAGE: bool = False
                configs.RELAX_USE_SUM_LINK_USAGE: bool = False
                configs.RELAX_USE_MAX_LINK_USAGE: bool = True
                configs.RELAX_SPRING_ITERATION_COUNT: int = 10
                configs.RELAX_INCLUDE_SWAPPING_STEP: bool = True
                return relaxation

            case 81:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = True
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 100
                configs.GA_PARAMS_MUTATION_SIZE = 40
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 82:
                configs.GA_USE_MAX_BW_USAGE: bool = True
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 100
                configs.GA_PARAMS_MUTATION_SIZE = 40
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 83:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = True
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 100
                configs.GA_PARAMS_MUTATION_SIZE = 40
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 84:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = True
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = False

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 100
                configs.GA_PARAMS_MUTATION_SIZE = 40
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case 85:
                configs.GA_USE_MAX_BW_USAGE: bool = False
                configs.GA_USE_SUM_BW_USAGE: bool = False
                configs.GA_USE_MAX_LINK_USAGE: bool = False
                configs.GA_USE_SUM_LINK_USAGE: bool = False
                configs.GA_USE_END_TO_END_LINK_MAX: bool = True

                configs.GA_PARAMS_MAX_GENERATION = 10
                configs.GA_PARAMS_MAX_POPULATION = 100
                configs.GA_PARAMS_MUTATION_SIZE = 40
                configs.GA_PARAMS_PARENT_SELECTION = 5
                return genetic_algorithm
            case _:
                raise Exception("Invalid configuration selected!")

        raise Exception("Invalid distribution type entered in configs file.")

    def generate_data_task_graph(self) -> Topology | None:

        self.topology: Topology = Topology()

        head = TopologyNode(
            self.topology.get_next_node_id(),
            node_type=NodeType.HEAD,
            node_data=None,
            name="start",
        )
        self.topology.add_node(head)
        self.topology.set_source(head)

        number_of_raw_data_added = 0
        producer_settings: dict[str, RawSettings]
        for producer_name, producer_settings in self.em.producers.items():
            for topic_name, producer_setting in producer_settings.items():
                producer_node = TopologyNode(
                    self.topology.get_next_node_id(),
                    node_type=NodeType.RAWOUTPUT,
                    node_data=producer_setting,
                    name=f"{producer_name}_{topic_name}",
                )

                event_count: int = math.ceil(
                    self.rawstats.get_expected_event_count(producer_name, topic_name)
                )

                if configs.LOCK_PROD_CTR:
                    event_count = configs.LOCK_PROD_CTR_VAL

                producer_node.set_expected_execution_count(event_count)

                self.topology.add_node(producer_node)
                e_added = self.topology.add_edge(head, producer_node, None)
                if e_added:
                    number_of_raw_data_added += 1

        task: CEPTask
        number_of_event_node_added = 0
        executor_nodes = {}
        for task in self.tasks:

            for rt in task.settings.required_sub_tasks:
                rt.subscription_topics = []

            task.settings.output_topic.target_databases = []

            executor_node = TopologyNode(
                self.topology.get_next_node_id(),
                node_type=NodeType.EVENTEXECUTION,
                node_data=task,
                name=task.settings.action_name,
            )

            self.topology.add_node(executor_node)
            number_of_event_node_added += 1
            executor_nodes[task.settings.action_name] = executor_node

        number_of_consumers_added = 0
        for consumer_id, consumer_topics in self.em.event_consumers.items():
            for topic_id, topic_setting in consumer_topics.items():
                consumer_node = TopologyNode(
                    self.topology.get_next_node_id(),
                    node_type=NodeType.CONSUMER,
                    node_data=topic_setting,
                    name=f"{consumer_id}_{topic_id}",
                )

                self.topology.add_node(consumer_node)
                number_of_consumers_added += 1

        task: CEPTask
        number_of_event_edge_added = 0
        for task in self.tasks:
            executor_node = executor_nodes[task.settings.action_name]

            input_topics = [
                topic.input_topic for topic in task.settings.required_sub_tasks
            ]

            for topic in input_topics:
                source_nodes = self.topology.get_nodes_from_input_topic(topic)
                for source_node in source_nodes:
                    if source_node.node_type == NodeType.RAWOUTPUT:
                        if (
                            source_node.flow_id != 0
                            and source_node.flow_id != task.settings.flow_id
                        ):
                            raise Exception("Invalid flow assignment detected!!!")
                        source_node.flow_id = task.settings.flow_id

                    e_added = self.topology.add_edge(source_node, executor_node, topic)
                    if e_added:
                        number_of_event_edge_added += 1

            target_nodes = self.topology.get_nodes_from_out_topic(
                task.settings.output_topic.output_topic
            )
            for target_node in target_nodes:
                e_added = self.topology.add_edge(
                    executor_node, target_node, task.settings.output_topic.output_topic
                )
                if e_added:
                    number_of_event_edge_added += 1

        sink = TopologyNode(
            self.topology.get_next_node_id(),
            node_type=NodeType.SINK,
            node_data=None,
            name="end",
        )
        self.topology.add_node(sink)

        number_of_sink_edge_added = 0
        for u, _ in self.topology.G.nodes(data=True):
            if not self.topology.get_successors(u) and u is not sink:
                e_added = self.topology.add_edge(u, sink, topic=None)
                if e_added:
                    number_of_sink_edge_added += 1

        self.topology.update_expected_flow_counts(
            self.mssa, self.rawstats, self.consumerstats
        )

        is_weakly_connected = nx.is_weakly_connected(self.topology.G)
        print("Is graph weakly connected: ", is_weakly_connected)
        is_directed = self.topology.G.is_directed()
        print("Is graph directed: ", is_directed)
        is_acyclic = nx.is_directed_acyclic_graph(self.topology.G)
        print("Is graph directed acyclic: ", is_acyclic)

        self.topology.is_weak = (
            not is_directed or not is_acyclic or not is_weakly_connected
        )

        valid_topology = self.topology.validate_all_paths_start_from_sink_end_at_head()

        if not valid_topology:
            print(
                "NOT ALL NODES START FROM HEAD TO SINK, MAYBE SOME RAW PRODUCERS ARE NOT REGISTERED YET!!"
            )
            return None

        if not is_directed or not is_acyclic or not is_weakly_connected:
            print(
                "Graph needs to be directed acyclic and all nodes should connect to each other!"
            )
            raise Exception(
                "Graph needs to be directed acyclic and all nodes should connect to each other!"
            )

        print(f"number_of_raw_data_added: {number_of_raw_data_added}")
        print(f"number_of_event_node_added: {number_of_event_node_added}")
        print(f"number_of_event_edge_added: {number_of_event_edge_added}")
        print(f"number_of_consumers_added: {number_of_consumers_added}")
        print(f"number_of_sink_edge_added: {number_of_sink_edge_added}")

        colors = []
        labels = {}
        u: TopologyNode
        for u, _ in self.topology.G.nodes(data=True):
            if u.node_type in [NodeType.SINK]:
                colors.append("red")
                labels[u] = "SINK"
            elif u.node_type in [NodeType.HEAD]:
                colors.append("black")
                labels[u] = "HEAD"
            elif u.node_type == NodeType.RAWOUTPUT:
                colors.append("blue")
                labels[u] = u.name
            elif u.node_type == NodeType.EVENTEXECUTION:
                colors.append("purple")
                labels[u] = u.name
            elif u.node_type == NodeType.CONSUMER:
                colors.append("gray")
                labels[u] = u.name
        nx.draw_networkx(
            self.topology.G, labels=labels, with_labels=True, node_color=colors
        )
        plt.savefig("visuals/task_topology.png")
        plt.clf()

        return self.topology

    def run(self):

        Thread(daemon=True, target=self.periodic_evaluation).start()

    def stop(self):
        self.cepsm.killed = True

    def periodic_evaluation(self):
        evaluation_period = configs.evaluation_period

        while not self.cepsm.killed:
            if configs.EVAL_ACTIVE == 0:
                time.sleep(0.5)
            else:
                if not configs.USE_FIXED_EVAL:
                    self.periodic_evaluation_detail()
                    time.sleep(evaluation_period)
                else:
                    st = datetime.now()
                    self.periodic_evaluation_detail()
                    ellapsed_seconds: float = (datetime.now() - st).total_seconds()
                    remaining_seconds: float = evaluation_period - ellapsed_seconds
                    if remaining_seconds > 0:
                        time.sleep(remaining_seconds)

        print("[+] server killing periodic evaluation...")

    def periodic_evaluation_detail(self):

        if configs.EVAL_ACTIVE == 0:
            print("Evaluation is not active yet...")
            return

        if not self.em.producers or not self.em.workers:
            print("A single worker & producer is required to run optimizations...")
            return

        current_server_time = data_helper.get_start_time()

        self.em.worker_lock.acquire()

        self.fetch_producer_stats()
        processed_tasks: List[str] = self.fetch_event_stats()
        self.fetch_consumer_stats()

        print(
            f"Current valid evaluation count: {self.sm.valid_stats_count}, evaluation stops after: {configs.USE_HISTORIC_DIST_AFTER_N}"
        )
        if (
            self.sm.valid_stats_count >= configs.USE_HISTORIC_DIST_AFTER_N
            and configs.USE_HISTORIC_DIST_AFTER_N > 0
        ):
            print("Evaluation completed USING PREVIOUS ITEMS")
            cep_evaluation_time: float = data_helper.get_elapsed_ms(current_server_time)
            self.sm.print_distributions(
                self.previous_alterations,
                self.previous_producer_updates,
                self.previous_consumer_requests,
            )
            self.sm.print_server_stats(
                datetime.now(tz=timezone.utc).strftime("%m.%d.%Y %H.%M.%S.%f")
                + ","
                + str(cep_evaluation_time)
                + ","
                + str(0.0)
            )
            self.em.worker_lock.release()
            return

        topology = self.generate_data_task_graph()

        if topology is None or self.topology.is_weak:
            print("Topology is not valid...")
            self.em.worker_lock.release()
            return

        dist_action = self.get_distribution_action()

        print("-------------------------------------------------")
        print(f"[SERVER] Processed task count: {len(processed_tasks)}")
        print(f"[SERVER] Processed tasks: {processed_tasks}")
        print("-------------------------------------------------")

        current_server_time_for_algorithm_only = data_helper.get_start_time()

        if configs.env_distribution_type in [
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            59,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
            71,
            72,
            73,
            74,
            75,
            76,
            77,
            78,
            79,
            80,
            81,
            82,
            83,
            84,
            85,
        ]:
            (
                alteration_requests,
                producer_updates,
                consumer_requests,
                self.distribution_history,
                max_link_load,
            ) = dist_action(
                topology,
                self.em.producers,
                self.em.workers,
                self.em.event_consumers,
                self.tasks,
                self.rawstats,
                self.mssa,
                self.consumerstats,
                self.distribution_history,
            )
        else:
            raise Exception("Invalid distribution type selected!")

        current_evaluation_time_for_algorithm_only: float = data_helper.get_elapsed_ms(
            current_server_time_for_algorithm_only
        )

        distinct_activated_events: list[str] = []
        activated_event_count: int = 0
        alteration_req: TaskAlterationModel
        for alteration_req in alteration_requests:
            if alteration_req.activate:
                activated_event_count += 1
            if (
                alteration_req.activate
                and alteration_req.job_name not in distinct_activated_events
            ):
                distinct_activated_events.append(alteration_req.job_name)

        if len(self.topology.get_executor_nodes()) != len(distinct_activated_events):
            print(
                "Not all tasks can be activated at the moment, activated task count: ",
                len(distinct_activated_events),
            )
            self.em.worker_lock.release()
            return

        self.process_post_decision_validations(
            producer_updates, alteration_requests, consumer_requests
        )

        if len(self.topology.get_executor_nodes()) != activated_event_count:
            raise Exception(
                "Number of tasks expected to be activated does not match the number of tasks!"
            )

        self.em.worker_lock.release()

        if configs.USE_DROP_PREVENTION:
            alteration_requests.reverse()

            fixed_alteration_requests: List[TaskAlterationModel] = []
            fixed_activation_requests: List[TaskAlterationModel] = []
            fixed_deactivation_requests: List[TaskAlterationModel] = []
            for a_r in alteration_requests:

                if not self.previous_alterations:
                    fixed_alteration_requests.append(a_r)

                    if a_r.activate:
                        if configs.LOGS_ACTIVE:
                            print(f"Adding activation for {a_r.job_name}")
                        fixed_activation_requests.append(a_r)
                    continue

                for p_r in self.previous_alterations:
                    if a_r.host == p_r.host and a_r.job_name == p_r.job_name:

                        if not a_r.activate and not p_r.activate:
                            continue

                        fixed_alteration_requests.append(a_r)

                        if a_r.activate:
                            if configs.LOGS_ACTIVE:
                                print(f"Adding activation for {a_r.job_name}")
                            fixed_activation_requests.append(a_r)
                        else:
                            if configs.LOGS_ACTIVE:
                                print(f"Adding de-activation for {a_r.job_name}")
                            fixed_deactivation_requests.append(a_r)

            print(f"Number of all alterations: {len(alteration_requests)}")
            print(
                f"Number of non-redundant alterations: {len(fixed_alteration_requests)}"
            )
            print(f"Max load percentage usage: {max_link_load}")

            bulk_activation, bulk_deactivation = self.publish_event_activations(
                fixed_alteration_requests
            )

            self.publish_requests(fixed_activation_requests)

            time.sleep(configs.WAIT_BEFORE_DEACTIVATIONS)
            self.publish_requests(fixed_deactivation_requests)

            print(f"{datetime.now(timezone.utc)} Finished sending alterations")
        else:

            self.publish_producer_updates(producer_updates)

            self.publish_event_updates(alteration_requests)

            self.publish_consumer_requests(consumer_requests)

        self.previous_alterations: List[TaskAlterationModel] = alteration_requests
        self.previous_producer_updates: List[RawUpdateEvent] = producer_updates
        self.previous_consumer_requests: List[CEPConsumerUpdateEvent] = (
            consumer_requests
        )

        print(f"[SERVER] {datetime.now(timezone.utc)} Evaluation completed...")
        cep_evaluation_time = data_helper.get_elapsed_ms(current_server_time)
        self.sm.print_distributions(
            alteration_requests, producer_updates, consumer_requests
        )
        self.sm.print_raw_production_distributions(producer_updates)
        self.sm.print_event_distributions(
            alteration_requests, producer_updates, consumer_requests
        )
        self.sm.print_server_stats(
            datetime.now(tz=timezone.utc).strftime("%m.%d.%Y %H.%M.%S.%f")
            + ","
            + str(cep_evaluation_time)
            + ","
            + str(current_evaluation_time_for_algorithm_only)
            + ","
            + str(max_link_load)
        )

    def process_post_decision_validations(
        self,
        producer_updates: List[RawUpdateEvent],
        alteration_requests: List[TaskAlterationModel],
        consumer_requests: List[CEPConsumerUpdateEvent],
    ) -> None:

        device_expected_per_execution_load: dict[str, float] = {}
        device_expected_execution_count: dict[str, int] = {}
        for d_id, _ in self.em.workers.items():
            device_expected_per_execution_load[d_id] = 0.0
            device_expected_execution_count[d_id] = 0

        if configs.LOGS_ACTIVE:
            print("-------------------------")
        processed_input_topics: dict[str, dict[str, bool]] = {}
        ea: TaskAlterationModel
        for ea in alteration_requests:
            if ea.activate:
                if ea.cep_task.settings.action_name not in processed_input_topics:
                    processed_input_topics[ea.cep_task.settings.action_name] = {}

                device_expected_per_execution_load[
                    ea.host
                ] += self.mssa.event_get_execution_duration_ms_total(
                    ea.cep_task.settings.action_name
                )
                device_expected_execution_count[ea.host] += 1

                for ot in ea.cep_task.settings.required_sub_tasks:
                    if (
                        ot.input_topic
                        not in processed_input_topics[ea.cep_task.settings.action_name]
                    ):
                        processed_input_topics[ea.cep_task.settings.action_name][
                            ot.input_topic
                        ] = False
                    if configs.LOGS_ACTIVE:
                        print(
                            f"{ea.cep_task.settings.action_name}:{ot.subscription_topics}"
                        )
                    if (
                        len(ot.subscription_topics) > 1
                        or len(ot.subscription_topics) != 1
                    ):
                        raise Exception(
                            "An input can be read from one and only one place to prevent duplicated readings!"
                        )

                    if len(ot.subscription_topics) > 0:
                        processed_input_topics[ea.cep_task.settings.action_name][
                            ot.input_topic
                        ] = True

        for _, d_val in device_expected_execution_count.items():
            if d_val > configs.DEVICE_ACTION_LIMIT:
                raise Exception("Device limit cannot be passed!")

        print("---------------")
        print("----LOADS------")
        for d_id, _ in self.em.workers.items():
            print(
                f"[SERVER] Device: {d_id}, number of tasks: {device_expected_execution_count[d_id]}"
            )
        print("---------------")

        for _, event_ts in processed_input_topics.items():
            for _, t_processed in event_ts.items():
                if not t_processed:
                    raise Exception(
                        "One of the activated events does not have a target where it will read its input from."
                    )

    def publish_producer_updates(self, producer_updates: List[RawUpdateEvent]) -> None:
        producer_request: RawUpdateEvent
        for producer_request in producer_updates:
            if len(set(producer_request.output_topic.target_databases)) != len(
                producer_request.output_topic.target_databases
            ):
                print(producer_request.output_topic.target_databases)
                raise Exception("Each db target in the topic details should be unique!")

            msg = pickle.dumps(producer_request)
            self.eventm.alteration_publisher.produce(
                configs.kafka_producer_topics[0], msg
            )

    def publish_event_updates(
        self, alteration_requests: List[TaskAlterationModel]
    ) -> None:

        alteration_request: TaskAlterationModel
        for alteration_request in alteration_requests:
            if alteration_request.activate:
                for ot in alteration_request.cep_task.settings.required_sub_tasks:
                    if len(set(ot.subscription_topics)) != len(ot.subscription_topics):
                        raise Exception(
                            "Each db target in the topic details should be unique!"
                        )
                if len(
                    set(
                        alteration_request.cep_task.settings.output_topic.target_databases
                    )
                ) != len(
                    alteration_request.cep_task.settings.output_topic.target_databases
                ):
                    raise Exception(
                        "Each db target in the topic details should be unique!"
                    )

            msg = pickle.dumps(alteration_request)
            self.eventm.alteration_publisher.produce(
                configs.kafka_alteration_topics[0], msg
            )

    def publish_requests(self, alteration_requests: List[TaskAlterationModel]) -> None:
        print(f"Number of requests: {len(alteration_requests)}")
        for alteration_request in alteration_requests:
            tab = TaskAlterationBulk()
            tab.host = alteration_request.host
            if alteration_request.activate:
                tab.activations = [alteration_request]
                tab.deactivations = []
            else:
                tab.deactivations = [alteration_request]
                tab.activations = []

            msg = pickle.dumps(tab)
            host_listener_topic: str = (
                f"{configs.kafka_alteration_topics[0]}_{tab.host}"
            )
            self.eventm.alteration_publisher.produce(host_listener_topic, msg)

    def publish_bulk_requests(
        self, bulk_requests: dict[str, TaskAlterationBulk]
    ) -> None:
        for _, br_package in bulk_requests.items():
            msg = pickle.dumps(br_package)
            host_listener_topic: str = (
                f"{configs.kafka_alteration_topics[0]}_{br_package.host}"
            )
            self.eventm.alteration_publisher.produce(host_listener_topic, msg)

    def publish_event_activations(
        self, alteration_requests: List[TaskAlterationModel]
    ) -> Tuple[dict[str, TaskAlterationBulk], dict[str, TaskAlterationBulk]]:

        bulk_activation_requests: dict[str, TaskAlterationBulk] = {}
        bulk_de_activation_requests: dict[str, TaskAlterationBulk] = {}
        alteration_request: TaskAlterationModel
        for alteration_request in alteration_requests:
            if alteration_request.host not in bulk_activation_requests:
                bulk_activation_requests[alteration_request.host] = TaskAlterationBulk()
                bulk_activation_requests[alteration_request.host].host = (
                    alteration_request.host
                )
                bulk_activation_requests[alteration_request.host].activations = []
                bulk_activation_requests[alteration_request.host].deactivations = []

                bulk_de_activation_requests[alteration_request.host] = (
                    TaskAlterationBulk()
                )
                bulk_de_activation_requests[alteration_request.host].host = (
                    alteration_request.host
                )
                bulk_de_activation_requests[alteration_request.host].activations = []
                bulk_de_activation_requests[alteration_request.host].deactivations = []

            if alteration_request.activate:
                for ot in alteration_request.cep_task.settings.required_sub_tasks:
                    if len(set(ot.subscription_topics)) != len(ot.subscription_topics):
                        raise Exception(
                            "Each db target in the topic details should be unique!"
                        )
                if len(
                    set(
                        alteration_request.cep_task.settings.output_topic.target_databases
                    )
                ) != len(
                    alteration_request.cep_task.settings.output_topic.target_databases
                ):
                    raise Exception(
                        "Each db target in the topic details should be unique!"
                    )

                bulk_activation_requests[alteration_request.host].activations.append(
                    alteration_request
                )
            else:
                bulk_de_activation_requests[
                    alteration_request.host
                ].deactivations.append(alteration_request)

        return bulk_activation_requests, bulk_de_activation_requests

    def publish_consumer_requests(
        self, consumer_requests: List[CEPConsumerUpdateEvent]
    ) -> None:
        consumer_request: CEPConsumerUpdateEvent
        for consumer_request in consumer_requests:
            if len(set(consumer_request.topics)) != len(consumer_request.topics):
                raise Exception("Each db target in the topic details should be unique!")

            msg = pickle.dumps(consumer_request)
            self.eventm.alteration_publisher.produce(
                configs.kafka_consumer_topics[0], msg
            )

    def fetch_producer_stats(self):
        producer_collected_stats: List[RawStatisticsBook] = []

        for _, producer_node in self.em.producers.items():
            for _, prod_src in producer_node.items():
                retry_count = 0
                while True:
                    try:
                        rawSetting: RawSettings = prod_src
                        producer_url = (
                            "http://"
                            + rawSetting.producer_name
                            + ":"
                            + str(rawSetting.producer_port)
                            + "/raw_statistics_report/"
                            + rawSetting.output_topic.output_topic
                            + "/"
                        )

                        producer_stats = requests.get(url=producer_url, timeout=60.0)
                        prod_stat_msg: RawStatisticsBook = pickle.loads(
                            producer_stats.content
                        )
                        producer_collected_stats.append(prod_stat_msg)

                        break

                    except Exception as e:
                        print("[!!!!!!!!!!!!] ERROR: ", str(e))
                        retry_count += 1
                        if retry_count >= 5:
                            break
                        time.sleep(2)

        self.rawstats.update_raw_stats(producer_collected_stats)

    def fetch_event_stats(self) -> List[str]:
        print("[+] Collecting statistics from workers...")
        event_stats: List[ResourceUsageRecord] = []
        workers_to_be_deleted: list[str] = []
        for worker_conn in self.em.workers:
            retry_count = 0
            while True:
                try:
                    worker_port: int | float = self.em.workers[worker_conn][
                        "worker_port"
                    ]
                    worker_url: str = (
                        "http://"
                        + worker_conn
                        + ":"
                        + str(worker_port)
                        + "/statistics_report/"
                    )
                    resource_consumption: requests.Response = requests.get(
                        url=worker_url, timeout=60.0
                    )
                    resource_stats: ResourceUsageRecord = self.process_resource_data(
                        resource_consumption.content, None
                    )
                    event_stats.append(resource_stats)
                    break

                except Exception as e:
                    print("[!!!!!!!!!!!!] ERROR: ", str(e))
                    retry_count += 1
                    if retry_count >= 5:
                        if worker_conn not in workers_to_be_deleted:
                            print("Current workers: ", [*self.em.workers])
                            workers_to_be_deleted.append(worker_conn)
                        break
                    time.sleep(2)

        processed_events: List[str] = self.mssa.updateStatistics(
            device_statistic_records=event_stats
        )

        if len(processed_events) > 0:
            self.sm.valid_stats_count += 1

        return processed_events

    def fetch_consumer_stats(self) -> None:

        print("---------------------------------")
        print("[SERVER] Processing consumer data")
        consumer_topic_valid_stats: List[ConsumerTopicStats] = []

        event_consumers_to_be_removed: List[str] = []
        for consumer_host, consumer_node in self.em.event_consumers.items():
            for _, consumer_setting in consumer_node.items():
                retry_count = 0
                while True:
                    try:
                        consumer_url: str = (
                            "http://"
                            + consumer_setting.host_name
                            + ":"
                            + str(consumer_setting.host_port)
                            + "/event_consumer/"
                            + consumer_setting.topic_name
                            + "/"
                        )
                        consumer_status: requests.Response = requests.get(
                            url=consumer_url, timeout=60.0
                        )
                        consumer_stats: ConsumerTopicStats = pickle.loads(
                            consumer_status.content
                        )
                        consumer_topic_valid_stats.append(consumer_stats)
                        break
                    except Exception as e:
                        print("[Consumer error occured] ", str(e))
                        print("[!!!!!!!!!!!!] ERROR: ", str(e))
                        retry_count += 1
                        if retry_count >= 5:
                            event_consumers_to_be_removed.append(consumer_host)
                            break
                        time.sleep(2)

        self.consumerstats.update_consumer_stats(consumer_topic_valid_stats)


class ManagementServer:
    def __init__(self, app: FastAPI) -> None:

        self.server_event_manager = ServerEventManager(configs.env_server_name)
        self.server_event_manager.setup(self.process_resource_data)

        self.ssm = ServerStateManager()

        self.server_statistics_manager = ServerStatisticsManager()

        self.server_endpoint_manager = ServerEndpointManager(
            app, self.server_statistics_manager, self.stop_management
        )
        self.server_endpoint_manager.register_endpoints()

        self.server_optimization_manager = ServerCEPOptimizationManager(
            self.server_endpoint_manager,
            self.server_statistics_manager,
            self.server_event_manager,
            self.process_resource_data,
            self.ssm,
        )

        Thread(daemon=True, target=self.run).start()

        self.server_endpoint_manager.set_server_as_ready()

    def run(self) -> None:

        self.server_event_manager.run()
        self.server_optimization_manager.run()
        print("[+] Server Initialized")

    def process_resource_data(self, msg: bytes, _) -> ResourceUsageRecord:
        resource_consumption: ResourceUsageRecord = pickle.loads(msg)

        self.server_statistics_manager.addOrUpdateHostStats(resource_consumption)

        return resource_consumption

    def stop_management(self) -> None:
        self.server_event_manager.stop_management()
        self.server_optimization_manager.stop()

    def register_task_for_management(self, task: CEPTask) -> None:
        self.server_optimization_manager.register_task_for_management(task)
