from copy import deepcopy
import pickle
import sys
from threading import Lock, Thread
import time
import traceback
from typing import Any, Callable, List
import uuid

from fastapi import FastAPI
from cep_library.cep.model.cep_task import CEPTask, EventModel, RequiredInputTopics
from cep_library.data import data_helper
from cep_library.data.database_management_service import DatabaseManagementService
from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.statistics_records import SingleStatisticsRecord, StatisticRecord, TopicStatisticDetail, TopicStatisticRecord
from cep_library.management.model.task_alteration import TaskAlterationBulk
import cep_library.configs as configs
from datetime import datetime, timezone, timedelta
import requests
import importlib
from fastapi.responses import FileResponse
from fastapi.responses import Response
import os

from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer
from cep_library.stats_helper.mean_stat_base import MeanStatBase


class CEPEventStateManager:
    def __init__(self, host_name:str, db:DatabaseManagementService) -> None:
        self.killed:bool = False
        self.host_name:str = host_name
        self.db:DatabaseManagementService = db
        
        self.actions: dict[str, Any] = {}
        self.initialized = False

    def reset_actions(self):
        self.actions.clear()

    def stop(self):
        
        for action in self.actions:
            consumer:MQTTConsumer
            for consumer in self.actions[action]["consumer"]:
                consumer.close()
            producer:MQTTProducer
            for producer in self.actions[action]["producer"]:
                producer.close()
            database:DatabaseManagementService
            for database in self.actions[action]["databases"]:
                database.close()

        
        self.db.close()
        
        
        self.killed = True
        
class CEPEventStatisticsManager:
    def __init__(self, cepsm: CEPEventStateManager) -> None:
        self.action_statistics: dict[str, Any] = {}
        self.cepsm:CEPEventStateManager = cepsm
        self.carried_data_count:MeanStatBase = MeanStatBase()
        self.deleted_data_count:MeanStatBase = MeanStatBase()

    def reset_action_statistics(self):
        action_statistic:StatisticRecord
        action_stats_to_remove:List[str] = []
        for action_name, action_statistic in self.action_statistics.items():
            
            if action_name not in self.cepsm.actions:
                
                
                action_stats_to_remove.append(action_name)
            else:
                action_lock:Lock = self.cepsm.actions[action_name]["stat_lock"]
                action_lock.acquire()
                action_statistic.reset_stats()
                action_lock.release()

        
        for stat_to_remove_name in action_stats_to_remove:
            del self.action_statistics[stat_to_remove_name]
        print(f"[CLIENT] {self.cepsm.host_name} Deleted {len(action_stats_to_remove)} leftover stats.")

        self.carried_data_count.reset()
        self.deleted_data_count.reset()

    def reset_all_statistics(self):
        self.action_statistics.clear()
        self.carried_data_count.reset()
        self.deleted_data_count.reset()

    def resource_usage(self)->bytes:
        print("----------------------------------------------------")        
        
        resource_usage = ResourceUsageRecord()
        resource_usage.host = self.cepsm.host_name
        resource_usage.record_date = datetime.now(tz=timezone.utc)
        resource_usage.carried_data_count = self.carried_data_count
        resource_usage.deleted_data_count = self.deleted_data_count
        resource_usage.statistic_records = []

        
        action_name:str
        action_statistic: StatisticRecord
        crr_execution_count:int = 0
        action_name_list: list[str] = []
        for action_name, action_statistic in self.action_statistics.items():
            action_name_list.append(action_name)
            action_statistic.fetched_time = datetime.now(timezone.utc)
            resource_usage.statistic_records.append(action_statistic)
            crr_execution_count += int(action_statistic.crr_task_execution_ns_stats.count)
        
            print(f"[CLIENT] action: {action_name} hit count: {action_statistic.hit_count.count}, miss count: {action_statistic.miss_count.count}")
        print("----------------------------------------------------")
        
        finalized_stats = pickle.dumps(resource_usage)
        return finalized_stats

class CEPEventActionManager:
    def __init__(self, cepsm:CEPEventStateManager, cep_stats_manager:CEPEventStatisticsManager) -> None:
        self.cepsm = cepsm
        self.cep_stats_manager = cep_stats_manager

    
    def execute_action(self, msg, action_msg_params):
        
        action: CEPTask
        stat_lock:Lock
        statistic_record:StatisticRecord
        host_name:str
        action_producer:MQTTProducer
        action, stat_lock, statistic_record, host_name, action_producer, db_query, triggered_topic_name, custom_args = action_msg_params
        
        
        try:
            
            current_time = data_helper.get_start_time()
            
            
            
            data = None
            initDates:List[datetime] = []
            em_request:EventModel = pickle.loads(msg)
            
            
            
            
            
            
            
            
            current_in_topic_records:dict[str, TopicStatisticDetail] = {}
                
            tsd = TopicStatisticDetail()
            
            crr_db_current_in_topic_read_start_time = data_helper.get_start_time()
            
            db_query.query["_id"] = em_request.data_id
            
            if not configs.USE_MQTT_FOR_DATA_TRANSFER:
                current_queried_data = self.cepsm.db.query(
                    db_query,
                    triggered_topic_name,
                    em_request.vsm,
                )
            else:
                current_queried_data = [em_request.data]
            
            
            datasets = {}
            if len(current_queried_data) > 0:
                event_received_duration:float = (datetime.now(timezone.utc) - em_request.event_date).microseconds / 1000.0
                event_received_duration = 10.0 if event_received_duration < 0 else event_received_duration
                if not configs.INCLUDE_EVENT_DELAY_TO_COST:
                    event_received_duration = 0.0
                
                if em_request.vsm == host_name:
                    tsd.local_ns += data_helper.get_elapsed_ms(crr_db_current_in_topic_read_start_time) + event_received_duration
                    tsd.local_byte += data_helper.estimate_size_kbytes(current_queried_data)
                    tsd.local_data_count += len(current_queried_data)
                else:
                    tsd.remote_ns += data_helper.get_elapsed_ms(crr_db_current_in_topic_read_start_time) + event_received_duration
                    tsd.remote_byte += data_helper.estimate_size_kbytes(current_queried_data)
                    tsd.remote_data_count += len(current_queried_data)

                
                current_in_topic_records[triggered_topic_name] = tsd

                value_matrix = {}
                for sub_data in current_queried_data:
                    decoded_data = pickle.loads(sub_data['data'])
                    
                    initDates.append(sub_data['initDateMin'])
                    initDates.append(sub_data['initDateMax'])

                    for key, value in decoded_data.items():
                        if key in value_matrix:
                            if isinstance(value, list):
                                for v in value:
                                    value_matrix[key].append(v)
                            else:
                                value_matrix[key].append(value)
                        else:
                            if isinstance(value, list):
                                value_matrix[key] = value
                            else:
                                value_matrix[key] = [value]
                
                for key, val in value_matrix.items():                        
                    datasets[key] = val
            
            
            else:
                stat_lock.acquire()
                
                print(f"Could not find data at vsm location: {em_request.vsm}, for topic: {triggered_topic_name}: for action: {action.settings.action_name}, with data id: {em_request.data_id}")
                
                
                sst = SingleStatisticsRecord(action.settings.action_name)
                sst.action_name = action.settings.action_name
                sst.in_topic_records = current_in_topic_records
                statistic_record.update_miss_stats(sst)

                stat_lock.release()
                return

            if em_request.data_id != db_query.query["_id"]:
                raise Exception("Invalid data operation detected!!!!")

            

            if len(datasets.keys()) == 0:
                raise Exception("Data cannot be empty at this point!")

            if not initDates or len(initDates) == 0:
                print("Initial data date cannot be null!!!!!!")
                raise Exception("Initial data date cannot be null!!!!!!")

            data = datasets
            

            
            task_execution_start_time = data_helper.get_start_time()
            
            trigger_next:bool
            
            if custom_args:
                (result, trigger_next, _, data_priority) = action.task(data, custom_args)
            else:
                (result, trigger_next, _, data_priority) = action.task(data)

            out_data_byte = data_helper.estimate_size_kbytes(result)
                
            task_execution_time = data_helper.get_elapsed_ms(task_execution_start_time)
            

            

            
            out_local_data_byte:float = 0.0
            out_remote_data_byte:float = 0.0
            
            out_local_elapsed_ms: float = 0.0
            out_remote_elapsed_ms: float = 0.0

            out_local_data_count: float = 0.0
            out_remote_data_count: float = 0.0

            if trigger_next:
                data_id = str(uuid.uuid4())
                to_db = {
                    "_id": data_id,
                    "data": pickle.dumps(result),
                    "initDateMin": min(initDates),
                    "initDateMax": max(initDates),
                    "source": action.settings.action_name,
                    "cep_priority": data_priority
                    }

                
                
                target_hosts:List[str] = deepcopy(action.settings.output_topic.target_hosts)
                target_db_list:List[str] = deepcopy(action.settings.output_topic.target_databases)
                for target_db in target_db_list:
                    crr_db_data_write_start_time = data_helper.get_start_time()
                    
                    core_data_time = datetime.now(tz=timezone.utc)
                    
                    if not configs.USE_DELAY_SINCE_BEGINNING:
                        to_db["createdAt"] = core_data_time
                    else:
                        core_data_time = min(initDates)
                        to_db["createdAt"] = core_data_time
                    
                    if not configs.USE_MQTT_FOR_DATA_TRANSFER:
                        self.cepsm.db.insert_one(
                            to_db,
                            action.settings.output_topic.output_topic,
                            target_db,
                        )
                
                    
                    
                    em = EventModel()
                    em.event_date = core_data_time
                    em.raw_date = em_request.raw_date
                    em.data_id = data_id
                    em.vsm = target_db
                    em.raw_data_tracker = f"{em_request.raw_data_tracker}:{action.settings.output_topic.output_topic}"
                    
                    if configs.USE_MQTT_FOR_DATA_TRANSFER:
                        em.data = to_db

                    payload = pickle.dumps(em)

                    
                    action_producer.currently_working = True
                    if configs.MQTT_USE_PREFIX_FOR_EVENTS:
                        th:str
                        for th in target_hosts:
                            fixed_output_topic: str = f"{action.settings.output_topic.output_topic}_{th}"
                            action_producer.produce(
                                fixed_output_topic,
                                payload
                            )
                    else:        
                        fixed_output_topic: str = action.settings.output_topic.output_topic
                        action_producer.produce(
                            fixed_output_topic,
                            payload
                        )
                    action_producer.currently_working = False

                    event_received_duration:float = (datetime.now(timezone.utc) - core_data_time).microseconds / 1000.0
                    event_received_duration = 10.0 if event_received_duration < 0 else event_received_duration
                    if not configs.INCLUDE_EVENT_DELAY_TO_COST:
                        event_received_duration = 0.0

                    if target_db == host_name:
                        out_local_data_byte += out_data_byte
                        out_local_elapsed_ms += data_helper.get_elapsed_ms(crr_db_data_write_start_time) + event_received_duration
                        out_local_data_count += 1
                    else:
                        out_remote_data_byte += out_data_byte
                        out_remote_elapsed_ms += data_helper.get_elapsed_ms(crr_db_data_write_start_time) + event_received_duration
                        out_remote_data_count += 1
            

            crr_event_date = datetime.now(tz=timezone.utc)
            total_elapsed = (crr_event_date - em_request.event_date).total_seconds()
            execution_elapsed = data_helper.get_elapsed_ms(current_time)

            
            default_clock_sync_delay_s:float = 0.01
            default_clock_sync_delay_ms:float = default_clock_sync_delay_s * 1000.0
            
            raw_elapsed_time_from_init_ms = 0.0
            in_d:datetime
            for in_d in initDates:    
                raw_elapsed_time_crr = (crr_event_date - in_d).total_seconds()
                
                if raw_elapsed_time_crr < 0:
                    raw_elapsed_time_crr = default_clock_sync_delay_s 
                raw_elapsed_time_from_init_ms += (raw_elapsed_time_crr * 1000.0)

            raw_elapsed_max_time_from_init_ms = (crr_event_date - min(initDates)).total_seconds() * 1000.0
            raw_elapsed_min_time_from_init_ms = (crr_event_date - max(initDates)).total_seconds() * 1000.0
            raw_elapsed_time_from_init_ms = raw_elapsed_time_from_init_ms / len(initDates)

            sst = SingleStatisticsRecord(action.settings.action_name)
            sst.action_name = action.settings.action_name
            sst.total_elapsed = default_clock_sync_delay_s if total_elapsed < 0 else total_elapsed
            sst.execution_elapsed_ms = execution_elapsed
            sst.task_execution_time = task_execution_time

            sst.raw_elapsed_time_from_init_ms = default_clock_sync_delay_ms if raw_elapsed_time_from_init_ms < 0 else raw_elapsed_time_from_init_ms
            sst.raw_elapsed_max_time_from_init_ms = default_clock_sync_delay_ms if raw_elapsed_max_time_from_init_ms < 0 else raw_elapsed_max_time_from_init_ms
            sst.raw_elapsed_min_time_from_init_ms = default_clock_sync_delay_ms if raw_elapsed_min_time_from_init_ms < 0 else raw_elapsed_min_time_from_init_ms
            
            sst.in_topic_records = current_in_topic_records
            
            sst.out_local_data_byte = out_local_data_byte
            sst.out_local_elapsed_ms = out_local_elapsed_ms
            sst.out_local_data_count = out_local_data_count
            
            sst.out_remote_data_byte = out_remote_data_byte
            sst.out_remote_elapsed_ms = out_remote_elapsed_ms
            sst.out_remote_data_count = out_remote_data_count
            
            stat_lock.acquire()
            statistic_record.update_stats(sst)
            stat_lock.release()
        except Exception as e:
            if configs.DEBUG_MODE:
                print("[ERROR] Probably action is deactivated before it can be finished: ", e, " ", action.settings.action_name)
                traceback.print_exc()
            if 'process no longer exists' in str(e):
                pass
            else:
                print("[ERROR] Probably action is deactivated before it can be finished: ", e, " ", action.settings.action_name)
                traceback.print_exc()


class CEPEventCodeManager:
    def __init__(self, cepsm:CEPEventStateManager, 
                 cep_stats_manager:CEPEventStatisticsManager, 
                 cep_action_manager:CEPEventActionManager) -> None:
        self.cepsm = cepsm
        self.cep_stats_manager = cep_stats_manager
        self.cep_action_manager = cep_action_manager

    def run(self):
        pass

    def download_script_file(self, action_path)->requests.Response:
        server_url = 'http://'+configs.env_server_name+':'+str(configs.env_server_port)+'/get_server_action_file/'
        
        try:
            params = {
                'file_path':action_path
                }
            return requests.get(server_url, params=params)
        except Exception as e:
            print ("[*] error during get request to server: ", e)
            raise Exception("[*] error during get request to server: ", e)

    
    
    
    
    
    
    
    
    
    def add_action(self, action: CEPTask):
        
        if action.settings.action_name in self.cepsm.actions:
            print(f"[CLIENT] {self.cepsm.host_name} Action {action.settings.action_name} is already active, possibly periodic evaluation is triggered manually!")
            return

        add_action_start = data_helper.get_start_time()

        
        downloaded_module_file:requests.Response = self.download_script_file(action.settings.action_path)
        
        
        
        if downloaded_module_file.status_code != 200:
            raise Exception("File download from server is not successful!!")
        
        if len(downloaded_module_file.content) == 0:
            raise Exception("Downloaded file content is empty!")
        
        print("Downloaded the script file: ", action.settings.action_path)

        with open(action.settings.action_path+'.py', "wb") as f:
            f.write(downloaded_module_file.content)
        
        
        
        splitted_path = action.settings.action_path.split('/')
        module_directory = '/'.join(splitted_path[0:len(splitted_path)-1])
        sys.path.append(module_directory)
        
        module_name = splitted_path[-1]
        module = importlib.import_module(module_name)
        
        imported_method = getattr(module, action.settings.action_name)
        
        sys.path.pop()
        
        if configs.DEBUG_MODE:
            print("[ACTION-ACTIVATION] Imported method is: ", imported_method)
        action.task = imported_method
        
        
        action.task({})
        
        
        
        while True:
            try:
                if configs.DEBUG_MODE:
                    print("Trying to make sure collection is created...")
                all_db_successful:bool = True
                for target_db in action.settings.output_topic.target_databases: 
                    db_succ = self.cepsm.db.create_collection(
                        host_name = target_db,
                        collection_name = action.settings.output_topic.output_topic,
                        ttl_duration = configs.collection_lifetime
                    )
                    all_db_successful = all_db_successful and db_succ
                if all_db_successful:
                    break
            except Exception as e:
                print(e)
                pass
            
            time.sleep(0.5)
        if configs.DEBUG_MODE:
            print("Output topics are created...")
        
        
        
        
        required_sub_task:RequiredInputTopics
        
        
        if action.settings.action_name in self.cep_stats_manager.action_statistics:
            print(f"No stats should be availabe for the newly activated action: {action.settings.action_name}!")
            sys.exit()

        statistic_record = StatisticRecord(action.settings.action_name)
        for t in action.settings.required_sub_tasks:
            statistic_record.in_topic_stats[t.input_topic] = TopicStatisticRecord(t.input_topic)
        self.cep_stats_manager.action_statistics[action.settings.action_name] = statistic_record

        consumer_list = []
        consumer_threads = []
        producer_list = []
        producer_threads = []
        database_list = []
        stat_lock = Lock()
        required_sub_task:RequiredInputTopics
        
        
        if configs.MQTT_SHARED_EVENT_CLIENT_ID:
            publisher_client_id:str = f"producer_{action.settings.action_name}_{action.settings.output_topic.output_topic}"
        else:
            publisher_client_id:str = f"producer_{action.settings.action_name}_{action.settings.output_topic.output_topic}_{self.cepsm.host_name}"
        action_producer = MQTTProducer(client_id=publisher_client_id, host_name=self.cepsm.host_name)
    
        producer_list.append(action_producer)
    
        producer_threads.append(Thread(daemon=True, target=action_producer.run))
        
        for required_sub_task in action.settings.required_sub_tasks:
            
            topic_name:str = required_sub_task.input_topic

            
            
            
            if configs.MQTT_SHARED_EVENT_CLIENT_ID:
                crr_client_id: str=f"{action.settings.action_name}_{topic_name}"
            else:
                crr_client_id: str=f"{action.settings.action_name}_{topic_name}_{self.cepsm.host_name}"

            if configs.MQTT_USE_PREFIX_FOR_EVENTS:
                fixed_topic_name: str = f"{topic_name}_{self.cepsm.host_name}"
            else:
                fixed_topic_name: str = topic_name
                
            db_query = deepcopy(action.settings.query)
            action_consumer = MQTTConsumer(
                client_id=crr_client_id,
                core_topic=required_sub_task.input_topic,
                topic_names=[fixed_topic_name],
                target=self.cep_action_manager.execute_action,
                target_args=(action, stat_lock, statistic_record, self.cepsm.host_name, action_producer, db_query, topic_name, action.settings.arguments),
                shared_subscription=True,
                shared_sub_group=action.settings.action_name,
                host_name=self.cepsm.host_name
            )
            consumer_list.append(action_consumer)
            
            
            consumer_threads.append(Thread(daemon=True, target=action_consumer.run))

        
        self.cepsm.actions[action.settings.action_name] = {
            "action": action,
            "consumer": consumer_list,
            "producer": producer_list,
            "databases": database_list,
            "module_name": module_name,
            "stat_lock": stat_lock,
            "last_update_date": datetime.now(timezone.utc)
        }

        
        add_action_end = data_helper.get_elapsed_ms(add_action_start)
        statistic_record.activation_time_ns.add(add_action_end)
            
        
        for producer_thread in producer_threads:
            producer_thread.start()
        for consumer_thread in consumer_threads:
            consumer_thread.start()

        print(f"[!!!] {self.cepsm.host_name} worker activated action: ", action.settings.action_name, " ", imported_method, ", in: ", add_action_end, " ms")

    
    def remove_action(self, action_name:str, deletion_time:datetime):
        print("---------------------------------------------")
        print(f"[!!] [CEP EXECUTOR] {self.cepsm.host_name} Removing action: ", action_name)

        action = self.cepsm.actions[action_name]
        
        consumer:MQTTConsumer
        for consumer in action["consumer"]:
            consumer.unsubscribe()
            while not consumer.unsubscribed:
                
                time.sleep(0.250)
            
            while consumer.last_msg_date + timedelta(seconds=5) >= datetime.now(timezone.utc):
                
                time.sleep(0.250)

            consumer.close()

        
        
        if self.cepsm.actions[action_name]["last_update_date"] > deletion_time:
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            print("ACTIVATED BEFORE FINISHING EVERYTHING UP!!!!!!!!!!!!!!!!!!!!!!!!")
            consumer:MQTTConsumer
            for consumer in action["consumer"]:
                consumer.establish_connection()
                Thread(daemon=True, target=consumer.run).start()
            return

        producer:MQTTProducer
        for producer in action["producer"]:
            producer.close()

            if producer.currently_working:
                sys.exit()
                

        if configs.DEBUG_MODE:
            print("Pre-validation of module existence: ", action["module_name"], flush=True)
        
        if action["module_name"] not in sys.modules:
            print(f"FATAL ERROR Module should exist here: ", action["module_name"])
            sys.exit()
        
        
        del sys.modules[action["module_name"]]
        
        print("Deleted module: ", action["module_name"], flush=True)
                
        module_name = action["module_name"]
        
        del module_name
                                
        if action["module_name"] in sys.modules:
            print("Module still exists: ", action["module_name"])
            sys.exit()
        
        
        if action["module_name"] in dir():
            print("Module still exists in dir: ", action["module_name"])
            sys.exit()

        
        if action["module_name"] in globals():
            print("Module still exists in globals: ", action["module_name"])
            sys.exit()
        
        if configs.DEBUG_MODE:
            print("Validated module deletion: ", action["module_name"], flush=True)
        
        
        action_delete:CEPTask = action["action"]
        os.remove(action_delete.settings.action_path+'.py')
        
        del self.cepsm.actions[action_name]

        print(f"[!] {self.cepsm.host_name} action {action_name} removed.")
        print("---------------------------------------------")

    
    def process_consumer_alteration(self, msg: bytes, args):
        alteration_request:TaskAlterationBulk = pickle.loads(msg)

        
        if (
            alteration_request.host != ""
            and alteration_request.host == self.cepsm.host_name
        ):
            
            for activation in alteration_request.activations:
                
                if activation.job_name != "" and activation.job_name not in self.cepsm.actions:
                    activation.cep_task.settings.host_name = self.cepsm.host_name
                    self.add_action(activation.cep_task)
                
                
                elif activation.job_name != "" and activation.job_name in self.cepsm.actions:
                    self.cepsm.actions[activation.job_name]["last_update_date"] = datetime.now(timezone.utc)
                    
                    activated_action_to_manipulate:CEPTask = self.cepsm.actions[activation.job_name]["action"]
                    
                    
                    
                    new_output_dbs:List[str] = []
                    
                    new_output_dbs += activation.cep_task.settings.output_topic.target_databases
                    
                    old_output_dbs:List[str] = []
                    old_output_dbs += activated_action_to_manipulate.settings.output_topic.target_databases
                        
                    common_dbs = list(set(new_output_dbs).intersection(old_output_dbs))
                    
                    if len(new_output_dbs) != len(old_output_dbs) or len(common_dbs) != len(old_output_dbs):
                        print(f"[CLIENT] {self.cepsm.host_name} Output target difference detected!")
                    
                    
                    
                    for old_req in activated_action_to_manipulate.settings.required_sub_tasks:
                        new_req = [s for s in activation.cep_task.settings.required_sub_tasks if s.input_topic == old_req.input_topic][0]
                        
                        
                        new_output_dbs:List[str] = old_req.subscription_topics
                        old_output_dbs:List[str] = new_req.subscription_topics
                            
                        common_dbs = list(set(new_output_dbs).intersection(old_output_dbs))
                        
                        if len(new_output_dbs) != len(old_output_dbs) or len(common_dbs) != len(old_output_dbs):
                            print(f"[CLIENT] {self.cepsm.host_name} Input target difference detected!")
                    
                    
                    
                    for new_target_db in activation.cep_task.settings.output_topic.target_databases:
                        self.cepsm.db.create_collection(
                            host_name = new_target_db,
                            collection_name = activated_action_to_manipulate.settings.output_topic.output_topic,
                            ttl_duration = configs.collection_lifetime
                        )

                    
                    activated_action_to_manipulate.settings = activation.cep_task.settings
            
            
            for deactivation in alteration_request.deactivations:
                if deactivation.job_name != "" and deactivation.job_name in self.cepsm.actions:
                    print(f"Triggering removal of action: {deactivation.job_name}")
                    Thread(daemon=True, target=self.remove_action, args=(deactivation.job_name, datetime.now(timezone.utc),)).start()

class CEPEventHTTPEndpointManager:
    def __init__(self, 
                 cepsm:CEPEventStateManager,
                 stop_management:Callable,
                 cep_event_statistics_manager:CEPEventStatisticsManager) -> None:
        self.cepsm = cepsm
        
        self.stop_management = stop_management
        self.cep_event_statistics_manager = cep_event_statistics_manager

    def run(self):
        if configs.HEARTBEATS_ACTIVE:
            Thread(daemon=True, target=self.periodic_heartbeat_to_server).start()

    
    def periodic_heartbeat_to_server(self) -> None:
        while not self.cepsm.killed:
            
            self.send_heartbeat(self.cepsm.host_name)
            time.sleep(1)
        
    def send_heartbeat(self, host_name:str) -> None:
        
        ready = False or configs.env_server_name == 'localhost'
        server_url = 'http://'+configs.env_server_name+':'+str(configs.env_server_port)+'/server_ready/'
        server_ready_params = {
                'worker_name': host_name,
                'worker_port': configs.env_host_port,
                'worker_ram_limit': configs.CP_RAM_LIMIT,
                'host_x': configs.env_host_x,
                'host_y': configs.env_host_y
            }
        
        while not ready:
            try:
                ready = requests.get(server_url, params=server_ready_params)
            except Exception as e:
                print ("[*] error during get request to server: ", e)
            
            time.sleep(1)
        

    def register_action_file_service(self, app: FastAPI):
        @app.get("/get_action_file/")
        def get_action_file(file_path:str, file_name):
            file_path = file_path.replace('.', '/')+'.py'
            return FileResponse(path=file_path, filename=file_name)
        
        @app.get("/clear_statistics/")
        def clear_statistics():
            
            self.cepsm.reset_actions()            
            self.cep_event_statistics_manager.reset_all_statistics()
            return Response(status_code=200)

        @app.get("/stop_execution/")
        def stop_execution():
            self.stop_management()
            return Response(status_code=200)
        
        
        @app.get("/statistics_report/")
        def _():
            sd = datetime.now(tz=timezone.utc)
            print("Sending statistics to management server...")
            
            if not self.cepsm.initialized:
                return Response(status_code=400)
            
            encoded = self.cep_event_statistics_manager.resource_usage()
            self.cep_event_statistics_manager.reset_action_statistics()

            ed = datetime.now(tz=timezone.utc)
            print(f"Took {(ed - sd).seconds} seconds to generate results.")

            return Response(content=encoded, media_type="application/json")


class CEPEventMessageCommunicationManager:
    def __init__(self, cepsm:CEPEventStateManager, 
                 cep_stats_manager:CEPEventStatisticsManager, 
                 cep_code_manager:CEPEventCodeManager) -> None:
        self.cepsm = cepsm
        self.cep_stats_manager = cep_stats_manager
        self.cep_code_manager = cep_code_manager
        self.setup_communication()
    
    def run(self):
        
        Thread(daemon=True, target=self._resource_publisher.run).start()
        Thread(daemon=True, target=self._alteration_consumer.run).start()
    
    def stop(self):
        self._resource_publisher.close()
        self._alteration_consumer.close()
    
    def setup_communication(self):
        self._resource_publisher = MQTTProducer(
            f"producer_{configs.kafka_alteration_topics[0]}_{self.cepsm.host_name}")
        alteration_topic:str = f"{configs.kafka_alteration_topics[0]}_{self.cepsm.host_name}"
        self._alteration_consumer = MQTTConsumer(
            client_id=alteration_topic,
            core_topic=alteration_topic,
            topic_names=[alteration_topic],
            target=self.cep_code_manager.process_consumer_alteration,
            shared_subscription=False
        )

    def ack(self, _, __):
        pass


class CEPManagementService:
    def __init__(self, host_name:str, app: FastAPI, db:DatabaseManagementService) -> None:
        
        self.cepsm = CEPEventStateManager(host_name, db)
        
        
        self.esm = CEPEventStatisticsManager(self.cepsm)
        
        
        self.cepeem = CEPEventHTTPEndpointManager(self.cepsm, self.stop_management, self.esm)
        self.cepeem.register_action_file_service(app)
        self.cepeem.send_heartbeat(host_name)

        
        self.eam = CEPEventActionManager(self.cepsm, self.esm)
                
        
        self.ecm = CEPEventCodeManager(self.cepsm, self.esm, self.eam)

        
        self.cepemcm = CEPEventMessageCommunicationManager(self.cepsm, self.esm, self.ecm)

        
        Thread(daemon=True, target=self.run).start()

    def run(self):
        
        Thread(daemon=True, target=self.cepsm.db.run).start()

        
        Thread(daemon=True, target=self.cepemcm.run).start()

        
        Thread(daemon=True, target=self.ecm.run).start()

        
        Thread(daemon=True, target=self.cepeem.run).start()

        
        print("Client initialized")

    def stop_management(self):
        self.cepemcm.stop()
        self.cepsm.stop()
