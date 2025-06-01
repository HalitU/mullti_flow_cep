import time
from typing import List
import pymongo
from fastapi import FastAPI
from threading import Thread
from datetime import datetime, timedelta, timezone

from cep_library import configs
from cep_library.cep.model.cep_task import MongoQuery
from cep_library.consumer.event_consumer import CEPEventConsumer
from cep_library.consumer.model.alarm_statistics import AlarmStatistics
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.data.database_management_service import DatabaseManagementService
from cep_library.stats_helper.mean_stat_base import MeanStatBase
from cep_library.stats_helper.stats_file_helper import StatsFileHelper

class TrafficAlarmConsumer:
    def __init__(self, app:FastAPI, db:DatabaseManagementService) -> None:
        self.topic_name = "event.traffic_alarm_task"
        self.db = db
        self.initialized = False
        
        self.alarm_stats = AlarmStatistics()
        
        self.ambulance_sfh = StatsFileHelper(
            app=app, 
            header_arr=self.alarm_stats.get_device_stat_header(), 
            endpoint_prefix="alarm_stats",
            endpoint_name=self.topic_name)

        query = self.get_mongo_query()

        cec_settings = CEPConsumerSettings(
            host_name=configs.env_host_name,
            host_port=str(configs.env_host_port),
            topic_name=self.topic_name,
            query=query,
            host_x=configs.env_host_x,
            host_y=configs.env_host_y,
            is_image_read=False
        )

        self.cec = CEPEventConsumer(
            app=app,
            topic_name=self.topic_name,
            action=self.broadcast,
            settings=cec_settings,
            db=self.db
            )
        
        print("Initialized the ambulance alarm consumer.")

    def get_mongo_query(self, read_timedelta = configs.query_with_timedelta, limit:int = configs.MIN_REQUIRED_ACTIVE_VAR):
        mongo_query = MongoQuery()
        mongo_query.columns = {"_id": 1, "data": 1, "initDateMin": 1, "initDateMax": 1}
        mongo_query.sort = [("cep_priority", pymongo.DESCENDING), ("createdAt", pymongo.DESCENDING)]
        # library uses createdAt gte for given timedelta
        mongo_query.query = {}
        mongo_query.within_timedelta = timedelta(
            milliseconds=read_timedelta
        )
        mongo_query.limit = limit
        return mongo_query

    def run(self):
        Thread(daemon=True, target=self.cec.run).start()
    
    def broadcast(self, event_data: dict, raw_data_tracker:str):
        # print("Triggering the broadcast...")
        # print(f"Data is: {event_data}")
        
        if "traffic_alarm_task" not in event_data or len(event_data["traffic_alarm_task"]) == 0:
            raise Exception("[CLIENT] [ERROR] Data should be available for consumer at this point!!!!")        
        
        # Sleep for 250 ms which is the assumed broadcasting time
        if configs.ENABLE_ALARM_BROADCAST:
            time.sleep(configs.ALARM_BROADCAST_DURATION)

        if "traffic_alarm_task" in event_data:
            oldest_date:datetime = event_data["minRawDate"]
            newest_data:datetime = event_data["maxRawDate"]            
            data_exists:bool = False
            
            alarm_exists:bool=False
            for aa in event_data["traffic_alarm_task"]:
                # Write the time it took to process a data to a statistics file
                data_exists = True
                if "alarm_exists" not in aa:
                    raise Exception("Invalid data detected at consumer!")
                alarm_exists = aa["alarm_exists"]
            raw_processing_duration = (datetime.now(tz=timezone.utc) - oldest_date).total_seconds()
            latest_processing_duration = (datetime.now(tz=timezone.utc) - newest_data).total_seconds()

            if latest_processing_duration < 0:
                # print("ambulance_alarm_consumer Latest processing duration is below 0, setting it to 50 ms assumption due to clock async")
                latest_processing_duration = 0.05
            
            if raw_processing_duration < 0:
                # print("ambulance_alarm_consumer Oldest duration is below 0, setting it to 50 ms assumption due to clock async")
                raw_processing_duration = 0.05

            if data_exists:
                rows_to_write:List[List[str]] = []
                for src_name, src_stat in self.cec.consumer_stats.source_hit_count.items():
                    
                    self.alarm_stats.init_alarm_second_stats(src_name)
                    
                    if src_name not in self.alarm_stats.newest_processing_time:
                        self.alarm_stats.newest_processing_time[src_name] = MeanStatBase()
                        self.alarm_stats.oldest_processing_time[src_name] = MeanStatBase()
                    if src_name == raw_data_tracker:
                        self.alarm_stats.newest_processing_time[src_name].add(latest_processing_duration)
                        self.alarm_stats.oldest_processing_time[src_name].add(raw_processing_duration)
                        self.alarm_stats.update_oldest_second_hit(src_name, raw_processing_duration)
                    
                    miss_cnt = self.cec.consumer_stats.source_miss_count[src_name]
                    rows_to_write.append([
                        str(datetime.now(tz = timezone.utc)),
                        str(configs.env_host_name),
                        str(self.topic_name),
                        str(oldest_date),
                        str(alarm_exists),
                        str(f"traffic_alarm_task"),
                        str(self.alarm_stats.oldest_processing_time[src_name].mean),
                        str(self.alarm_stats.newest_processing_time[src_name].mean),
                        str(self.alarm_stats.oldest_processing_time[src_name].count),
                        str(self.alarm_stats.newest_processing_time[src_name].count),
                        str(self.cec.consumer_stats.miss_count.count),
                        str(self.cec.consumer_stats.hit_count.count),
                        str(src_name),
                        str(src_stat.count),
                        str(miss_cnt.count),
                        str(self.alarm_stats.oldest_1_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_2_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_3_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_4_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_5_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_6_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_7_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_8_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_9_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_10_second_hit_count[src_name].count),
                        str(self.alarm_stats.oldest_10_plus_second_hit_count[src_name].count)
                    ])
                self.ambulance_sfh.print_device_stats(rows_to_write)   
