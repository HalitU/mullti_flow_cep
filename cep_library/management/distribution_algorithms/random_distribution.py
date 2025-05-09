from copy import deepcopy
import sys
from typing import List
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
from cep_library.management.model.topology import Topology, TopologyNode
from cep_library.cep.model.cep_task import CEPTask

from cep_library.management.model.task_alteration import TaskAlterationModel
from cep_library.management.statistics.consumer_server_statics import ConsumerServerStatics
from cep_library.management.statistics.load_helper import get_max_percentage
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import ManagementServerStatisticsAnalyzer
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent

from cep_library import configs
import numpy as np

def get_devices_without_limit_final(
    copied_task:CEPTask,
    mssa:ManagementServerStatisticsAnalyzer,
    topic_targets: dict[str, List[str]],
    event_consumers:dict[str, dict[str, CEPConsumerSettings]],
    consumerstats:ConsumerServerStatics,
    
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

        
        related_devices:List[str] = [k]
        execution_host:str = k
        for rt in copied_task.settings.required_sub_tasks:
            link_load: float = mssa.get_expected_topic_load_per_second(copied_task.settings.action_name, rt.input_topic, False)
            for di in topic_targets[rt.input_topic]:
                if di != execution_host:
                    if di not in related_devices:
                        related_devices.append(di)
                    
                    worker_external_loads[di] += link_load
                    worker_external_loads[execution_host] += link_load

                    worker_uplink_loads[di] += link_load
                    worker_downlink_loads[execution_host] += link_load
                    
                    if di not in external_load_to_delete:
                        external_load_to_delete[di] = 0.0
                    if execution_host not in external_load_to_delete:
                        external_load_to_delete[execution_host] = 0.0
                    if di not in uplink_load_to_delete:
                        uplink_load_to_delete[di] = 0.0
                    if execution_host not in downlink_load_to_delete:
                        downlink_load_to_delete[execution_host] = 0.0
                    external_load_to_delete[di] += link_load
                    external_load_to_delete[execution_host] += link_load
                    uplink_load_to_delete[di] += link_load
                    downlink_load_to_delete[execution_host] += link_load
                            
            
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
                    
                    if consumer_setting.host_name not in related_devices:
                        related_devices.append(consumer_setting.host_name)
        
        
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
    
    
    if len(valid_workers) == 0:
        sorted_load_dict:dict[str, float] = {}
        for d_id, d_load in worker_uplink_loads.items():
            sorted_load_dict[d_id] = max(d_load, worker_downlink_loads[d_id])
        sorted_loads = dict(sorted(sorted_load_dict.items(), key=lambda item: item[1], reverse=False))
        device_choice:str = next(iter(sorted_loads))
        valid_workers.append(device_choice)
    
    return valid_workers

def random_distribution(
    topology:Topology,
    producers:dict[str, dict[str, RawSettings]],
    workers:dict[str, dict[str, int | float]], 
    event_consumers:dict[str, dict[str, CEPConsumerSettings]],
    cep_tasks:List[CEPTask],
    rawstats:RawServerStatAnalyzer,
    mssa:ManagementServerStatisticsAnalyzer,
    consumerstats:ConsumerServerStatics,
    old_distribution_history:dict,
    min_count:int=1,
    max_count:int=3,
) -> tuple[List[TaskAlterationModel], List[RawUpdateEvent], List[CEPConsumerUpdateEvent], dict, float]:
    print("Running algorithm number 36: random distribution...")

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
    
    topic_targets: dict[str, List[str]] = {}

    n_tasks_activated = 0
    n_tasks_deactivated = 0
    n_consumer_requests = 0

    task_nodes = topology.get_topologically_ordered_executor_nodes()
    prod_nodes = topology.get_producer_nodes()
    prod_nodes.sort(key=lambda x: x.name)

    
    
    
    for _, raw_settings in producers.items():
        for _, producer in raw_settings.items():    
            producer_target_db_update = RawUpdateEvent()
            producer_target_db_update.producer_name = producer.producer_name
            producer_target_db_update.output_topic = producer.output_topic
            producer_target_db_update.raw_name = producer.raw_data_name
            
            chosen_worker_device_ids:List[str] = np.random.choice(a=worker_device_ids, size=configs.ALLOWED_PARALLEL_EXECUTION_COUNT, replace=False)
            if configs.KEEP_RAWDATA_AT_SOURCE:
                chosen_worker_device_ids = [producer.producer_name]
            
            producer.output_topic.target_databases = chosen_worker_device_ids

            
            worker_execution_loads[producer.producer_name] += rawstats.get_expected_event_count(producer.producer_name, producer.output_topic.output_topic)

            
            if producer.output_topic.output_topic not in topic_targets:
                topic_targets[producer.output_topic.output_topic] = []
            for c in chosen_worker_device_ids:
                if c not in topic_targets[producer.output_topic.output_topic]:
                    topic_targets[producer.output_topic.output_topic].append(c)
            

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

    print("Raw assignments are completed")

    
    for consumer_id, consumer_topics in event_consumers.items():
        for topic_id, consumer_setting in consumer_topics.items():
            
            worker_execution_loads[consumer_setting.host_name] += consumerstats.get_expected_event_counts(consumer_id, topic_id)

    task_node: TopologyNode
    for task_node in task_nodes:
        
        task: CEPTask = deepcopy(task_node.node_data)

        
        chosen_worker_device_ids:List[str] = np.random.choice(a=get_devices_without_limit_final(
            task,
            mssa,
            topic_targets,
            event_consumers,
            consumerstats,
            worker_device_loads, 
            worker_external_loads, 
            worker_uplink_loads, 
            worker_downlink_loads, 
            worker_execution_loads
            ), size=configs.ALLOWED_PARALLEL_EXECUTION_COUNT, replace=False)
                    
        
        for execution_host in chosen_worker_device_ids:
            worker_device_loads[execution_host] += 1
            
            copied_task = deepcopy(task)
            
            
            for it in copied_task.settings.required_sub_tasks:
                it.subscription_topics = topic_targets[it.input_topic]
                
                if len(it.subscription_topics) == 0:
                    raise Exception("Db null exception!")

            
            for rt in copied_task.settings.required_sub_tasks:
                link_load: float = mssa.get_expected_topic_load_per_second(copied_task.settings.action_name, rt.input_topic, False)
                for di in topic_targets[rt.input_topic]:
                    if di != execution_host:
                        worker_external_loads[di] += link_load
                        worker_external_loads[execution_host] += link_load

                        worker_uplink_loads[di] += link_load
                        worker_downlink_loads[execution_host] += link_load
            
                
                worker_execution_loads[execution_host] += mssa.get_expected_event_input_counts(copied_task.settings.action_name, rt.input_topic)

            
            for consumer_id, consumer_topics in event_consumers.items():
                for topic_id, consumer_setting in consumer_topics.items():
                    link_load: float = consumerstats.get_expected_load_per_second(consumer_setting.host_name, topic_id, False)
                    if topic_id == copied_task.settings.output_topic.output_topic and consumer_setting.host_name != execution_host:
                        worker_external_loads[execution_host] += link_load
                        worker_external_loads[consumer_setting.host_name] += link_load

                        worker_uplink_loads[execution_host] += link_load
                        worker_downlink_loads[consumer_setting.host_name] += link_load

            
            copied_task.settings.output_topic.target_databases = [execution_host]

            
            if copied_task.settings.output_topic.output_topic not in topic_targets:
                topic_targets[copied_task.settings.output_topic.output_topic] = []
            for c in copied_task.settings.output_topic.target_databases:
                if c not in topic_targets[copied_task.settings.output_topic.output_topic]:
                    topic_targets[copied_task.settings.output_topic.output_topic].append(c)
            
            
            alteration = TaskAlterationModel()
            alteration.host = execution_host
            alteration.activate = True
            alteration.job_name = copied_task.settings.action_name
            alteration.cep_task = copied_task
            alteration.migration_requests = []
            alteration.only_migration = False
            alterations.append(alteration)
            n_tasks_activated += 1
            
        
        for host in [*workers]:
            if host not in chosen_worker_device_ids:
                deactivation = TaskAlterationModel()
                deactivation.host = host
                deactivation.activate = False
                deactivation.job_name = task_node.name
                deactivation.migration_requests = []
                deactivation.only_migration = False
                alterations.append(deactivation)
                n_tasks_deactivated += 1            

        print(f"Assigning task: {task_node.name}, to: {chosen_worker_device_ids}")

    print("Task assignments are completed")

    
    for consumer_id, consumer_topics in event_consumers.items():
        for topic_id, _ in consumer_topics.items():
            alteration = CEPConsumerUpdateEvent()
            alteration.host = consumer_id
            alteration.topic_name = topic_id
            alteration.topics = topic_targets[topic_id]
            
            if len(alteration.topics) == 0:
                raise Exception("Db targets cannot be null!")
            
            consumer_alterations.append(alteration)
            n_consumer_requests += 1
            
            print(f"Assigning consumer: {consumer_id}:{topic_id}, to: {worker_device_ids}")

    print("Consumer assignments are completed")

    print(
        "Number of tasks activated is: ",
        n_tasks_activated,
        " deactivated: ",
        n_tasks_deactivated,
    )

    max_load_perc = get_max_percentage(worker_uplink_loads, worker_downlink_loads)

    return alterations, producer_updates, consumer_alterations, {}, max_load_perc
