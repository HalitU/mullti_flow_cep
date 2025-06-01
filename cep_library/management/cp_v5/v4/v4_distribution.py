from copy import deepcopy
from typing import List
from cep_library.cep.model.cep_task import CEPTask
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
import networkx as nx
from ortools.sat.python import cp_model
import numpy as np

from cep_library.management.cp_v5.common.cp_sat_event_info import CPSATV3EventInfo
from cep_library.management.cp_v5.common.cp_sat_raw_source import CPSATV3RawSource
from cep_library.management.cp_v5.v4.v4_assignment_vars import (
    CPSATV4AssignmentVariables,
)
from cep_library.management.model.task_alteration import (
    DataMigrationModel,
    TaskAlterationModel,
)
from cep_library.raw.model.raw_update_event import RawUpdateEvent


class CPSATV4Distribuion:
    def __init__(
        self,
        all_device_ids: List[str],
        model: cp_model.CpModel,
        variables: CPSATV4AssignmentVariables,
        events: dict[str, CPSATV3EventInfo],
        raws: dict[str, CPSATV3RawSource],
        solver: cp_model.CpSolver,
        previous_dist_history: dict,
        consumers: dict[str, dict[str, CEPConsumerSettings]],
    ) -> None:
        self.all_device_ids: List[str] = all_device_ids
        self.model: cp_model.CpModel = model
        self.variables: CPSATV4AssignmentVariables = variables
        self.events: dict[str, CPSATV3EventInfo] = events
        self.raws: dict[str, CPSATV3RawSource] = raws
        self.solver: cp_model.CpSolver = solver
        self.previous_dist_history = previous_dist_history
        self.consumers: dict[str, dict[str, CEPConsumerSettings]] = consumers
        self.topic_targets: dict[str, List[str]] = {}

    def determine_raw_distributions(
        self,
        connection_validation_graph: nx.DiGraph,
        source: int,
        distribution_history: dict[str, dict[str, dict[str, list[str]]]],
        producer_updates: List[RawUpdateEvent],
    ) -> int:
        n_raw_manipulation_created = 0
        for (
            raw_source_id,
            raw_output_topics,
        ) in self.variables.raw_source_targets.items():
            raw_setting: CPSATV3RawSource = self.raws[raw_source_id]

            connection_validation_graph.add_edge(source, raw_source_id)

            for raw_output_topic, raw_topic_devices in raw_output_topics.items():
                raw_source_topic_target_devices: List[str] = [
                    k for k, v in raw_topic_devices.items() if self.solver.Value(v) == 1
                ]

                if not raw_source_topic_target_devices:
                    raise Exception(
                        "All raw data sources are required to have an output location."
                    )

                if raw_source_id not in distribution_history["raw_sensors"]:
                    distribution_history["raw_sensors"][raw_source_id] = {}
                distribution_history["raw_sensors"][raw_source_id][
                    raw_output_topic
                ] = raw_source_topic_target_devices

                for rot in raw_source_topic_target_devices:
                    connection_validation_graph.add_edge(
                        raw_source_id, (rot, raw_output_topic)
                    )

                raw_setting.settings.output_topic.target_databases = (
                    raw_source_topic_target_devices
                )
                producer_target_db_update = RawUpdateEvent()
                producer_target_db_update.raw_name = raw_setting.settings.raw_data_name
                producer_target_db_update.producer_name = (
                    raw_setting.settings.producer_name
                )
                producer_target_db_update.output_topic = (
                    raw_setting.settings.output_topic
                )
                producer_updates.append(producer_target_db_update)

                n_raw_manipulation_created += 1

        print(f"Raw data manipulations are completed: {n_raw_manipulation_created}.")

        return n_raw_manipulation_created

    def determine_execution_distributions(
        self,
        connection_validation_graph: nx.DiGraph,
        alterations: List[TaskAlterationModel],
        distribution_history: dict[
            str, dict[str, dict[str, dict[str, dict[str, list[str]]]]]
        ],
    ) -> tuple[int, int]:

        n_tasks_activated = 0
        n_tasks_deactivated = 0
        for (
            event_id,
            event_executors,
        ) in self.variables.event_execution_location_vars.items():
            event_executor_devices: List[str] = [
                e for e, v in event_executors.items() if self.solver.Value(v) == 1
            ]
            if len(event_executor_devices) != 1:
                raise Exception("Invalid event assignment detected!")
            event_executor_device: str = event_executor_devices[0]

            event_task: CEPTask = deepcopy(self.events[event_id].cep_task)

            if event_executor_device not in distribution_history["event_sensors"]:
                distribution_history["event_sensors"][event_executor_device] = {}

            distribution_history["event_sensors"][event_executor_device][event_id] = {
                "input_topics": {},
                "output_topics": {},
            }

            for rt in event_task.settings.required_sub_tasks:

                reading_choices: List[str] = [
                    e
                    for e, v in self.variables.event_reading_locations[event_id][
                        rt.input_topic
                    ].items()
                    if self.solver.Value(v) == 1
                ]
                print(
                    f"Reading choices for event: {event_id}, topic: {rt.input_topic} are: {reading_choices}"
                )
                reading_choice: str = reading_choices[0]

                device_choices: List[str] = [reading_choice]
                subscription_target: List[str] = [rt.input_topic]

                rt.subscription_topics = subscription_target

                distribution_history["event_sensors"][event_executor_device][event_id][
                    "input_topics"
                ][rt.input_topic] = device_choices

                for eetit in device_choices:
                    connection_validation_graph.add_edge(
                        (eetit, rt.input_topic), (event_executor_device, event_id)
                    )

            output_topic_name: str = event_task.settings.output_topic.output_topic
            activated_target_locations: List[str] = [
                k
                for k, v in self.variables.event_output_location_vars[event_id][
                    output_topic_name
                ].items()
                if self.solver.Value(v) == 1
            ]

            if len(activated_target_locations) == 0:
                raise Exception("Number of activated devices cannot be 0!")

            event_task.settings.output_topic.target_databases = (
                activated_target_locations
            )

            distribution_history["event_sensors"][event_executor_device][event_id][
                "output_topics"
            ][output_topic_name] = activated_target_locations

            for eetot in activated_target_locations:
                connection_validation_graph.add_edge(
                    (event_executor_device, event_id), (eetot, output_topic_name)
                )

            alteration = TaskAlterationModel()
            alteration.host = event_executor_device
            alteration.job_name = event_task.settings.action_name
            alteration.activate = True
            alteration.cep_task = event_task
            alteration.migration_requests = []
            alteration.only_migration = False
            alterations.append(alteration)

            n_tasks_activated += 1

            for dw in self.all_device_ids:
                if dw == event_executor_device:
                    continue

                deactivation = TaskAlterationModel()
                deactivation.host = dw
                deactivation.job_name = event_task.settings.action_name
                deactivation.activate = False
                deactivation.migration_requests = []
                deactivation.only_migration = False
                alterations.append(deactivation)

                n_tasks_deactivated += 1

        print(
            f"Execution assignments are completed, activated: {n_tasks_activated}, deactivated: {n_tasks_deactivated}."
        )

        return n_tasks_activated, n_tasks_deactivated

    def validate_assignment_dag(
        self, connection_validation_graph: nx.DiGraph, source: int, sink: int
    ):

        print("Validating the DAG relationship...")
        number_of_sink_edge_added = 0
        for u in connection_validation_graph.nodes:
            if not list(connection_validation_graph.successors(u)) and u != sink:
                connection_validation_graph.add_edge(u, sink)
                number_of_sink_edge_added += 1

        is_weakly_connected = nx.is_weakly_connected(connection_validation_graph)
        print("Is graph weakly connected: ", is_weakly_connected)
        is_directed = connection_validation_graph.is_directed()
        print("Is graph directed: ", is_directed)
        is_acyclic = nx.is_directed_acyclic_graph(connection_validation_graph)
        print("Is graph directed acyclic: ", is_acyclic)
        print(f"Number of sink connections added: {number_of_sink_edge_added}")

        if (
            len(nx.bfs_tree(connection_validation_graph, source))
            != connection_validation_graph.number_of_nodes()
        ):
            raise Exception(
                f"Not all nodes are between the source and the sink, number of nodes: {connection_validation_graph.number_of_nodes()}, current number of connected nodes: {len(nx.bfs_tree(connection_validation_graph, source))}"
            )

        if not is_directed or not is_acyclic or not is_weakly_connected:
            print(
                "Graph needs to be directed acyclic and all nodes should connect to each other!"
            )
            raise Exception(
                "Graph needs to be directed acyclic and all nodes should connect to each other!"
            )

    def determine_raw_migration_assignments(
        self, alterations: List[TaskAlterationModel]
    ):

        n_migration_requests = 0
        for _, raw_setting in self.raws.items():
            new_devices: List[str] = self.output_topic_records[
                raw_setting.settings.output_topic.output_topic
            ]

            migrated = False
            for csd in raw_setting.currently_stored_devices:
                if migrated:
                    break
                if csd not in new_devices:

                    chosen_device: str = np.random.choice(
                        a=new_devices, size=1, replace=False
                    )[0]

                    migration_request = DataMigrationModel()
                    migration_request.topic = (
                        raw_setting.settings.output_topic.output_topic
                    )
                    migration_request.current_host = csd
                    migration_request.target_host = chosen_device
                    migration_request.data_type = 0
                    migration_request.writer_host = raw_setting.device_id

                    alteration = TaskAlterationModel()
                    alteration.host = csd
                    alteration.job_name = "migration"
                    alteration.activate = False
                    alteration.only_migration = True
                    alteration.migration_requests = [migration_request]
                    alterations.append(alteration)

                    n_migration_requests += 1

                    migrated = True

        print(f"Raw migration assignments are completed.")

        return n_migration_requests

    def determine_event_data_migrations(self, alterations: List[TaskAlterationModel]):

        n_migration_requests = 0
        for _, event_info in self.events.items():
            currently_written_device: list[str] = (
                event_info.get_output_topic_currently_written_devices()
            )
            newly_written_devices: List[str] = self.output_topic_records[
                event_info.cep_task.settings.output_topic.output_topic
            ]

            migrated = False
            for cwd in currently_written_device:
                if migrated:
                    continue
                if cwd not in newly_written_devices:

                    chosen_device: str = np.random.choice(
                        a=newly_written_devices, size=1, replace=False
                    )[0]

                    migration_request = DataMigrationModel()
                    migration_request.topic = (
                        event_info.cep_task.settings.output_topic.output_topic
                    )
                    migration_request.current_host = cwd
                    migration_request.target_host = chosen_device
                    migration_request.data_type = 1
                    migration_request.event_name = (
                        event_info.cep_task.settings.action_name
                    )

                    alteration = TaskAlterationModel()
                    alteration.host = cwd
                    alteration.job_name = event_info.cep_task.settings.action_name
                    alteration.activate = False
                    alteration.only_migration = True
                    alteration.migration_requests = [migration_request]
                    alterations.append(alteration)

                    n_migration_requests += 1

                    migrated = True

        print(f"Event data migration assignments are completed.")

        return n_migration_requests

    def determine_consumer_reading_locations(
        self,
        consumer_alterations: List[CEPConsumerUpdateEvent],
        connection_validation_graph: nx.DiGraph,
    ):
        n_consumer_requests = 0

        for (
            consumer_id,
            consumer_topics,
        ) in self.variables.event_consumer_location_vars.items():
            for topic_id, topic_reading_devices in consumer_topics.items():
                reading_devices: List[str] = [
                    d
                    for d, v in topic_reading_devices.items()
                    if self.solver.Value(v) == 1
                ]

                if len(reading_devices) == 0:
                    raise Exception(
                        "Each consumer has to read its topic data from a device!!!!!!"
                    )
                reading_device: str = reading_devices[0]

                if reading_device == "":
                    raise Exception("Device choice for any consumer cannot be empty!")

                alteration = CEPConsumerUpdateEvent()
                alteration.host = consumer_id
                alteration.topic_name = topic_id
                alteration.topics = [topic_id]

                consumer_alterations.append(alteration)
                n_consumer_requests += 1

                for eetit in reading_devices:

                    connection_validation_graph.add_edge(
                        (eetit, topic_id), (consumer_id, f"{consumer_id}_{topic_id}")
                    )

        print(f"Consumer reading assignments are completed.")

        return n_consumer_requests

    def determine_distributions(self):

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []
        consumer_alterations: List[CEPConsumerUpdateEvent] = []

        connection_validation_graph = nx.DiGraph()
        source = -1
        sink = -2
        connection_validation_graph.add_node(source)
        connection_validation_graph.add_node(sink)

        distribution_history: (
            dict[str, dict[str, dict[str, list[str]]]]
            | dict[str, dict[str, dict[str, dict[str, dict[str, list[str]]]]]]
        ) = {"raw_sensors": {}, "event_sensors": {}}

        n_raw_manipulation_created: int = self.determine_raw_distributions(
            connection_validation_graph, source, distribution_history, producer_updates
        )

        n_tasks_activated, n_tasks_deactivated = self.determine_execution_distributions(
            connection_validation_graph, alterations, distribution_history
        )

        n_consumer_requests = self.determine_consumer_reading_locations(
            consumer_alterations, connection_validation_graph
        )

        self.validate_assignment_dag(connection_validation_graph, source, sink)

        print(
            "Number of tasks activated is: ",
            n_tasks_activated,
            " deactivated: ",
            n_tasks_deactivated,
            ", Number of raw data manipulation: ",
            n_raw_manipulation_created,
            ", Number of consumer requests: ",
            n_consumer_requests,
        )

        return (
            alterations,
            producer_updates,
            distribution_history,
            0,
            consumer_alterations,
        )
