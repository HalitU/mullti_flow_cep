from copy import deepcopy
import sys
from typing import List

import numpy as np

from cep_library import configs
from cep_library.cep.model.cep_task import CEPTask, RequiredInputTopics
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
from cep_library.management.model.task_alteration import TaskAlterationModel
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.management.statistics.consumer_server_statics import ConsumerServerStatics
from cep_library.management.statistics.load_helper import get_max_percentage
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import ManagementServerStatisticsAnalyzer
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent

def get_devices_without_limit_final(
    copied_task:CEPTask,
    mssa:ManagementServerStatisticsAnalyzer,
    topic_targets: dict[str, List[str]],
    event_consumers:dict[str, dict[str, CEPConsumerSettings]],
    consumerstats:ConsumerServerStatics,
    
    choose_random:bool,
    worker_list:dict[str, int],
    worker_external_loads:dict[str, float], 
    worker_uplink_loads:dict[str, float],
    worker_downlink_loads:dict[str, float],
    worker_execution_loads:dict[str, float]) -> List[str]:
    
    print("=== checking device limits ===")
    valid_workers:List[str] = []
    for k, v in worker_list.items():
        
        exec_load_to_delete:dict[str, float] = {}
        external_load_to_delete:dict[str, float] = {}
        uplink_load_to_delete:dict[str, float] = {}
        downlink_load_to_delete:dict[str, float] = {}

        
        execution_host:str = k
        link_load: float = mssa.get_expected_output_topic_load_per_second(
            copied_task.settings.action_name, 
            copied_task.settings.output_topic.output_topic, 
            False
            )
        for di in topic_targets[copied_task.settings.output_topic.output_topic]:
            if di != execution_host:
                worker_external_loads[execution_host] += link_load
                worker_external_loads[di] += link_load

                worker_uplink_loads[execution_host] += link_load
                worker_downlink_loads[di] += link_load
                
                if execution_host not in external_load_to_delete:
                    external_load_to_delete[execution_host] = 0.0
                if di not in external_load_to_delete:
                    external_load_to_delete[di] = 0.0
                if execution_host not in uplink_load_to_delete:
                    uplink_load_to_delete[execution_host] = 0.0
                if di not in downlink_load_to_delete:
                    downlink_load_to_delete[di] = 0.0
                external_load_to_delete[execution_host] += link_load
                external_load_to_delete[di] += link_load
                uplink_load_to_delete[execution_host] += link_load
                downlink_load_to_delete[di] += link_load
                        
        
        for rt in copied_task.settings.required_sub_tasks:
            worker_execution_loads[execution_host] += mssa.get_expected_event_input_counts(copied_task.settings.action_name, rt.input_topic)
            if execution_host not in exec_load_to_delete:
                exec_load_to_delete[execution_host] = 0.0
            exec_load_to_delete[execution_host] += mssa.get_expected_event_input_counts(copied_task.settings.action_name, rt.input_topic)
                
        
        for _, consumer_topics in event_consumers.items():
            for topic_id, consumer_setting in consumer_topics.items():
                link_load: float = consumerstats.get_expected_load_per_second(consumer_setting.host_name, topic_id, False)
                if topic_id == copied_task.settings.output_topic.output_topic and consumer_setting.host_name != execution_host:
                    worker_external_loads[execution_host] += link_load
                    worker_external_loads[consumer_setting.host_name] += link_load

                    worker_uplink_loads[execution_host] += link_load
                    worker_downlink_loads[consumer_setting.host_name] += link_load
        
                    if execution_host not in external_load_to_delete:
                        external_load_to_delete[execution_host] = 0.0
                    if consumer_setting.host_name not in external_load_to_delete:
                        external_load_to_delete[consumer_setting.host_name] = 0.0
                    if execution_host not in uplink_load_to_delete:
                        uplink_load_to_delete[execution_host] = 0.0
                    if consumer_setting.host_name not in downlink_load_to_delete:
                        downlink_load_to_delete[consumer_setting.host_name] = 0.0
                    external_load_to_delete[execution_host] += link_load
                    external_load_to_delete[consumer_setting.host_name] += link_load
                    uplink_load_to_delete[execution_host] += link_load
                    downlink_load_to_delete[consumer_setting.host_name] += link_load
                    
        
        if configs.USE_UPLINK_DOWNLINK_COST:
            is_over_limit: bool = (worker_uplink_loads[k] > (configs.CUMULATIVE_LOAD_LIMIT / 2.0) * 0.75) or (worker_downlink_loads[k] > (configs.CUMULATIVE_LOAD_LIMIT / 2.0) * 0.75) or (worker_execution_loads[k] > configs.DEVICE_EXECUTION_LIMIT)
        else:
            is_over_limit: bool = worker_external_loads[k] > configs.CUMULATIVE_LOAD_LIMIT

        print(f"Current device load: {worker_external_loads[k]}, is over limit: {is_over_limit}")
        if v < configs.DEVICE_ACTION_LIMIT and not is_over_limit:
            valid_workers.append(k)
        
        
        for d_id, d_load in external_load_to_delete.items():
            worker_external_loads[d_id] -= d_load
        for d_id, d_load in uplink_load_to_delete.items():
            worker_uplink_loads[d_id] -= d_load
        for d_id, d_load in downlink_load_to_delete.items():
            worker_downlink_loads[d_id] -= d_load
        for d_id, d_load in exec_load_to_delete.items():
            worker_execution_loads[d_id] -= d_load
    
    
    if choose_random and len(valid_workers) == 0:
        sorted_load_dict:dict[str, float] = {}
        for d_id, d_load in worker_uplink_loads.items():
            sorted_load_dict[d_id] = max(d_load, worker_downlink_loads[d_id])
        sorted_loads = dict(sorted(sorted_load_dict.items(), key=lambda item: item[1], reverse=False))
        device_choice:str = next(iter(sorted_loads))
        valid_workers.append(device_choice)
    
    return valid_workers


def consumer_preferred(
        topology:Topology,
        producers:dict[str, dict[str, RawSettings]],
        workers:dict[str, dict[str, int | float]], 
        event_consumers:dict[str, dict[str, CEPConsumerSettings]],
        cep_tasks:List[CEPTask],
        rawstats:RawServerStatAnalyzer,
        mssa:ManagementServerStatisticsAnalyzer,
        consumerstats:ConsumerServerStatics,
        old_distribution_history:dict    
    ):
    print("Running Consumer Preferred Assignment Algorithm...")

    producer_updates: List[RawUpdateEvent] = []
    alterations: List[TaskAlterationModel] = []
    consumer_alterations: List[CEPConsumerUpdateEvent] = []
    
    
    if not workers:
        return alterations, producer_updates, consumer_alterations, {}, 0.0
    
    worker_device_ids = list(workers.keys())
    worker_device_loads:dict[str, int] = {}
    worker_external_loads:dict[str, float] = {}
    worker_uplink_loads:dict[str, float] = {}
    worker_downlink_loads:dict[str, float] = {}
    worker_execution_loads:dict[str, float] = {}
    
    for wdi in worker_device_ids:
        worker_device_loads[wdi] = 0    
        worker_external_loads[wdi] = 0.0
        worker_uplink_loads[wdi] = 0.0
        worker_downlink_loads[wdi] = 0.0
        worker_execution_loads[wdi] = 0.0

    
    
    for _, raw_settings in producers.items():
        for _, producer in raw_settings.items():
            
            worker_execution_loads[producer.producer_name] += rawstats.get_expected_event_count(producer.producer_name, producer.output_topic.output_topic)

    
    for consumer_id, consumer_topics in event_consumers.items():
        for topic_id, consumer_setting in consumer_topics.items():
            
            worker_execution_loads[consumer_setting.host_name] += consumerstats.get_expected_event_counts(consumer_id, topic_id)

    _, event_to_be_executed_devices = prepare_topic_device_targets(
        topology,
        producers,
        workers,
        event_consumers,
        cep_tasks,
        rawstats,
        mssa,
        consumerstats,
        old_distribution_history,
        worker_device_loads,
        worker_external_loads,
        worker_uplink_loads,
        worker_downlink_loads,
        worker_execution_loads
    )
    

    prod_nodes = topology.get_producer_nodes()
    prod_nodes.sort(key=lambda x: x.name)
    
    n_tasks_activated = 0
    n_tasks_deactivated = 0
    n_consumer_requests = 0
    
    topic_outputs:dict[str, List[str]] = {}
    task_activated_devices:dict[str, List[str]] = {}
    
    
    
    for _, raw_settings in producers.items():
        for _, producer in raw_settings.items():
            
            producer_target_db_update = RawUpdateEvent()
            producer_target_db_update.producer_name = producer.producer_name
            producer_target_db_update.output_topic = producer.output_topic
            producer_target_db_update.raw_name = producer.raw_data_name
            producer.output_topic.target_databases = [producer.producer_name]

            if len(producer.output_topic.target_databases) > 1:
                raise Exception("Consumer approach should be writing an output to a single location only!")

            
            if producer.output_topic.output_topic not in topic_outputs:
                topic_outputs[producer.output_topic.output_topic] = []
            
            target_topic_id: str = f"{producer.output_topic.output_topic}"
            if target_topic_id not in topic_outputs[producer.output_topic.output_topic]:
                topic_outputs[producer.output_topic.output_topic].append(target_topic_id)
            

            print(
                "[BFS] Currently processing producer node: ",
                producer.producer_name,
                " ",
                producer.output_topic.output_topic,
                " ",
                producer.output_topic.target_databases,
            )
            producer_updates.append(producer_target_db_update)

            print(
                f"Assigning production: {producer.raw_data_name}, to: {producer.producer_name}"
            )
    
    print("---------------")
    print("Raw assignments are completed")
    print("---------------")
    
    
    ordered_nodes = topology.get_topologically_ordered_executor_nodes()
    task_node: TopologyNode
    for task_node in ordered_nodes:
        
        task_data: CEPTask = deepcopy(task_node.node_data)
        
        print("----------------------------------------")
        print(f"Processing action: {task_data.settings.action_name}")

        device_choice:str = event_to_be_executed_devices[task_data.settings.action_name]

        print(f"Current device {device_choice}")
        copied_task = deepcopy(task_data)
        
        
        if task_activated_devices and copied_task.settings.action_name in task_activated_devices and device_choice in task_activated_devices[copied_task.settings.action_name]:
            raise Exception("A task can only be activated once at a device!")
        
        for r in copied_task.settings.required_sub_tasks:
            r.subscription_topics = topic_outputs[r.input_topic]
        
        copied_task.settings.output_topic.target_databases = [device_choice]
        
        
        if copied_task.settings.output_topic.output_topic not in topic_outputs:
            topic_outputs[copied_task.settings.output_topic.output_topic] = []
        
        target_topic_id = f"{copied_task.settings.output_topic.output_topic}"
        if target_topic_id not in topic_outputs[copied_task.settings.output_topic.output_topic]:
            topic_outputs[copied_task.settings.output_topic.output_topic].append(target_topic_id)
        
        
        print(f"Assigning execution: {task_data.settings.action_name} at {device_choice}, read: {device_choice}, write: {device_choice}")
        
        alteration = TaskAlterationModel()
        alteration.host = device_choice
        alteration.activate = True
        alteration.job_name = copied_task.settings.action_name
        alteration.cep_task = copied_task
        alteration.migration_requests = []
        alteration.only_migration = False
        alterations.append(alteration)
        n_tasks_activated += 1

        if copied_task.settings.action_name not in task_activated_devices:
            task_activated_devices[copied_task.settings.action_name] = []
        task_activated_devices[copied_task.settings.action_name].append(device_choice)

    
    for td, activated_devices in task_activated_devices.items():
        for w in worker_device_ids:
            if w not in activated_devices:
                deactivation = TaskAlterationModel()
                deactivation.host = w
                deactivation.activate = False
                deactivation.job_name = td
                deactivation.migration_requests = []
                deactivation.only_migration = False
                alterations.append(deactivation)
                n_tasks_deactivated += 1

    
    for consumer_id, consumer_topics in event_consumers.items():
        for topic_id, _ in consumer_topics.items():
            alteration = CEPConsumerUpdateEvent()
            alteration.host = consumer_id
            alteration.topic_name = topic_id
            alteration.topics = topic_outputs[topic_id]
            
            if len(alteration.topics) == 0:
                raise Exception("Invalid consumer assignment detected!")
            
            consumer_alterations.append(alteration)
            n_consumer_requests += 1
            
            print(f"Assigning consumer: {consumer_id}:{topic_id}, to: {worker_device_ids}")

    print(
        "Number of tasks activated is: ",
        n_tasks_activated,
        " deactivated: ",
        n_tasks_deactivated,
        " consumer messages: ",
        n_consumer_requests
    )

    max_load_perc = get_max_percentage(worker_uplink_loads, worker_downlink_loads)

    return alterations, producer_updates, consumer_alterations, old_distribution_history, max_load_perc

def get_topic_to_be_read_devices(topic_to_be_read_devices:dict[str, dict[str, float]]) -> dict[str, List[str]]:
    topic_targets:dict[str, List[str]] = {}
    for topic_id, topic_devices in topic_to_be_read_devices.items():
        topic_targets[topic_id] = []
        for d_id, _ in topic_devices.items():
            if d_id not in topic_targets[topic_id]:
                topic_targets[topic_id].append(d_id)
    return topic_targets

def prepare_topic_device_targets(
        topology:Topology,
        producers:dict[str, dict[str, RawSettings]],
        workers:dict[str, dict[str, int | float]], 
        event_consumers:dict[str, dict[str, CEPConsumerSettings]],
        cep_tasks:List[CEPTask],
        rawstats:RawServerStatAnalyzer,
        mssa:ManagementServerStatisticsAnalyzer,
        consumerstats:ConsumerServerStatics,
        old_distribution_history:dict,
        worker_device_loads:dict[str, int],
        worker_external_loads:dict[str, float],
        worker_uplink_loads:dict[str, float],
        worker_downlink_loads:dict[str, float],
        worker_execution_loads:dict[str, float]
    ):
    topic_to_be_read_devices:dict[str, dict[str, float]] = {}
    event_to_be_executed_devices:dict[str, str] = {}
    
    for consumer_id, consumer_topics in event_consumers.items():
        consumer_total_load:float = 0.0
        for topic_id, _ in consumer_topics.items():
            consumer_total_load += consumerstats.get_expected_load_per_second(consumer_id, topic_id, False)
        for topic_id, _ in consumer_topics.items():
            if topic_id not in list(topic_to_be_read_devices.keys()):
                topic_to_be_read_devices[topic_id] = {}
            if consumer_id not in list(topic_to_be_read_devices[topic_id].keys()):
                topic_to_be_read_devices[topic_id][consumer_id] = consumer_total_load
            else:
                if consumer_total_load > topic_to_be_read_devices[topic_id][consumer_id]:
                    topic_to_be_read_devices[topic_id][consumer_id] = consumer_total_load

    
    ordered_nodes = topology.get_topologically_ordered_executor_nodes()
    
    
    ordered_nodes.reverse()
    
    task_node: TopologyNode
    for task_node in ordered_nodes:
        
        task_data: CEPTask = task_node.node_data

        task_total_load:float = 0.0
        for rti in task_data.settings.required_sub_tasks:
            task_total_load += mssa.get_expected_topic_load_per_second(task_data.settings.action_name, rti.input_topic, False)
            
        
        device_choices:dict[str, float] = topic_to_be_read_devices[task_data.settings.output_topic.output_topic]
        topic_target_dict = get_topic_to_be_read_devices(topic_to_be_read_devices)
        valid_devices = get_devices_without_limit_final(
            task_data,
            mssa,
            topic_target_dict,
            event_consumers,
            consumerstats,

            True,
            worker_device_loads, 
            worker_external_loads, 
            worker_uplink_loads, 
            worker_downlink_loads,
            worker_execution_loads
            )
        available_devices:dict[str, float] = {}
        for d_id, d_val in device_choices.items():
            if d_id in valid_devices:
                available_devices[d_id] = d_val

        
        if len(available_devices) > 0:
            print("Using load choice")
            available_devices = dict(sorted(available_devices.items(), key=lambda item: item[1], reverse=True))
            device_choice:str = next(iter(available_devices))
        
        else:
            print("Using random choice")
            random_device_choices:List[str] = np.random.choice(a=valid_devices, size=1, replace=False)
            device_choice:str = random_device_choices[0]

        event_to_be_executed_devices[task_data.settings.action_name] = device_choice

        
        worker_device_loads[device_choice] += 1

        
        for rti in task_data.settings.required_sub_tasks:
            
            worker_execution_loads[device_choice] += mssa.get_expected_event_input_counts(task_data.settings.action_name, rti.input_topic)
            
            related_producers = topology.get_nodes_from_input_topic(rti.input_topic)
            for related_producer in related_producers:
                if related_producer.node_type == NodeType.RAWOUTPUT:
                    link_load: float = mssa.get_expected_topic_load_per_second(task_data.settings.action_name, rti.input_topic, False)
                    related_raw_data:RawSettings = related_producer.node_data
                    if device_choice != related_raw_data.producer_name:
                        worker_external_loads[device_choice] += link_load
                        worker_external_loads[related_raw_data.producer_name] += link_load

                        worker_uplink_loads[related_raw_data.producer_name] += link_load
                        worker_downlink_loads[device_choice] += link_load

        
        related_event_nodes = topology.get_nodes_from_out_topic(task_data.settings.output_topic.output_topic)
        for related_events in related_event_nodes:
            if related_events.node_type == NodeType.EVENTEXECUTION:
                related_event_data:CEPTask = related_events.node_data
                if related_event_data.settings.action_name in event_to_be_executed_devices:
                    other_device_id:str = event_to_be_executed_devices[related_event_data.settings.action_name]
                    if device_choice != other_device_id:
                        link_load: float = mssa.get_expected_topic_load_per_second(related_event_data.settings.action_name, task_data.settings.output_topic.output_topic, False)
                        worker_external_loads[other_device_id] += link_load
                        worker_external_loads[device_choice] += link_load

                        worker_uplink_loads[other_device_id] += link_load
                        worker_downlink_loads[device_choice] += link_load
                        
            if related_events.node_type == NodeType.CONSUMER:
                related_consumer_data:CEPConsumerSettings = related_events.node_data
                link_load: float = consumerstats.get_expected_load_per_second(related_consumer_data.host_name, task_data.settings.output_topic.output_topic, False)
                if related_consumer_data.host_name != device_choice:
                    worker_external_loads[device_choice] += link_load
                    worker_external_loads[related_consumer_data.host_name] += link_load
                    
                    worker_uplink_loads[device_choice] += link_load
                    worker_downlink_loads[related_consumer_data.host_name] += link_load

        rt:RequiredInputTopics
        for rt in task_data.settings.required_sub_tasks:
            if rt.input_topic not in list(topic_to_be_read_devices.keys()):
                topic_to_be_read_devices[rt.input_topic] = {}
            if device_choice not in list(topic_to_be_read_devices[rt.input_topic].keys()):
                topic_to_be_read_devices[rt.input_topic][device_choice] = task_total_load
            else:
                if task_total_load > topic_to_be_read_devices[rt.input_topic][device_choice]:
                    topic_to_be_read_devices[rt.input_topic][device_choice] = task_total_load

    return topic_to_be_read_devices, event_to_be_executed_devices
