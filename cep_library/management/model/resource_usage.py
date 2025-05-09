from typing import List
from datetime import datetime

from cep_library.management.model.statistics_records import StatisticRecord, TopicStatisticRecord
from cep_library.stats_helper.mean_stat_base import MeanStatBase

class ResourceUsageRecord:
    def __init__(self):
        self.host: str
        self.record_date: datetime
        self.carried_data_count: MeanStatBase
        self.deleted_data_count: MeanStatBase
        self.statistic_records: List[StatisticRecord]

    def get_raw_production_distribution_header(self):
        return [
            "record_id",
            "date",
            "host",
            "raw_name",
            "output_topic",
            "production_devices",
            "storage_devices"
        ]

    def get_event_distributions_header(self):
        return [
            "record_id",
            "date",
            "host",
            "event_name",
            "output_topic",
            "execution_devices",
            "storage_devices"
        ]

    def get_server_stat_header(self):
        return [
            "current_server_time_utc",
            "cep_evaluation_time_ns",
            "algorithm_eval_time_ms",
            "max_link_load_percentage"
        ]

    def get_device_stat_header(self):
        return [
            "record_id",
            "host",
            "record_date",
            "carried_data_count",
            "deleted_data_count",
            "current_server_time"
        ]

    def get_device_stats(self, record_id: int):
        return [
            str(record_id),
            str(self.host),
            str(self.record_date),
            str(self.carried_data_count.total()),
            str(self.deleted_data_count.total())
        ]

    def get_task_topic_stats_file_header(self):
        return [
            "record_id",
            "host",
            "record_date",
            
            "action_name",
            "topic_name",
            
            "in_topic_local_read_byte.count",
            "in_topic_local_read_byte.mean",
            "in_topic_local_read_ns.count",
            "in_topic_local_read_ns.mean",
            "in_topic_remote_read_byte.count",
            "in_topic_remote_read_byte.mean",
            "in_topic_remote_read_ns.count",
            "in_topic_remote_read_ns.mean",
            
            "initial_time",
            "fetched_time",
            
            "current_server_time"
        ]

    def get_task_topics_stats(self, record_id: int):
        results = []
        
        stat_record: StatisticRecord
        for stat_record in self.statistic_records:
            topic_stat_record: TopicStatisticRecord
            for _, topic_stat_record in stat_record.in_topic_stats.items():
                results.append([
                    str(record_id),
                    str(self.host),
                    str(self.record_date),
                    str(stat_record.action_name),
                    str(topic_stat_record.topic_name),
                    str(topic_stat_record.in_topic_local_read_byte.count),
                    str(topic_stat_record.in_topic_local_read_byte.mean),
                    str(topic_stat_record.in_topic_local_read_ns.count),
                    str(topic_stat_record.in_topic_local_read_ns.mean),
                    str(topic_stat_record.in_topic_remote_read_byte.count),
                    str(topic_stat_record.in_topic_remote_read_byte.mean),
                    str(topic_stat_record.in_topic_remote_read_ns.count),
                    str(topic_stat_record.in_topic_remote_read_ns.mean),
                    str(stat_record.initial_time),
                    str(stat_record.fetched_time)
                ])
        return results

    def get_task_stats_header(self):
        return [
            "record_id",
            "host",
            "record_date",
            "action_name",
            
            "crr_total_elapsed_s_stats.mean",
            "crr_total_elapsed_s_stats.count",
            "crr_task_execution_ns_stats.mean",
            "crr_task_execution_ns_stats.count",
            "crr_execution_elapsed_ms_stats.mean",
            "crr_execution_elapsed_ms_stats.count",
            "crr_raw_elapsed_time_from_init_ms.mean",
            "crr_raw_elapsed_time_from_init_ms.count",
            "crr_raw_elapsed_max_time_from_init_ms.mean",
            "crr_raw_elapsed_max_time_from_init_ms.count",
            "crr_raw_elapsed_min_time_from_init_ms.mean",
            "crr_raw_elapsed_min_time_from_init_ms.count",
            "crr_out_local_data_byte_stats.mean",
            "crr_out_local_data_byte_stats.count",
            "crr_out_local_data_ns_stats.mean",
            "crr_out_local_data_ns_stats.count",
            "crr_out_remote_data_byte_stats.mean",
            "crr_out_remote_data_byte_stats.count",
            "crr_out_remote_data_ns_stats.mean",
            "crr_out_remote_data_ns_stats.count",

            "hit_count",
            "miss_count",
            
            "initial_time",
            "fetched_time",
            
            "current_server_time",
        ]

    def get_task_stats(self, record_id: int) -> List[List[str]]:
        result:List[List[str]] = []
        stat_record: StatisticRecord
        for stat_record in self.statistic_records:
            result.append(
                [
                    str(record_id),
                    str(self.host),
                    str(self.record_date),
                    str(stat_record.action_name),
                    
                    str(stat_record.crr_total_elapsed_s_stats.mean),
                    str(stat_record.crr_total_elapsed_s_stats.count),
                    str(stat_record.crr_task_execution_ns_stats.mean),
                    str(stat_record.crr_task_execution_ns_stats.count),
                    str(stat_record.crr_execution_elapsed_ms_stats.mean),
                    str(stat_record.crr_execution_elapsed_ms_stats.count),
                    str(stat_record.crr_raw_elapsed_time_from_init_ms.mean),
                    str(stat_record.crr_raw_elapsed_time_from_init_ms.count),
                    str(stat_record.crr_raw_elapsed_max_time_from_init_ms.mean),
                    str(stat_record.crr_raw_elapsed_max_time_from_init_ms.count),
                    str(stat_record.crr_raw_elapsed_min_time_from_init_ms.mean),
                    str(stat_record.crr_raw_elapsed_min_time_from_init_ms.count),
                    str(stat_record.crr_out_local_data_byte_stats.mean),
                    str(stat_record.crr_out_local_data_byte_stats.count),
                    str(stat_record.crr_out_local_data_ns_stats.mean),
                    str(stat_record.crr_out_local_data_ns_stats.count),
                    str(stat_record.crr_out_remote_data_byte_stats.mean),
                    str(stat_record.crr_out_remote_data_byte_stats.count),
                    str(stat_record.crr_out_remote_data_ns_stats.mean),
                    str(stat_record.crr_out_remote_data_ns_stats.count),

                    str(stat_record.hit_count.total()),
                    str(stat_record.miss_count.total()),
                    
                    str(stat_record.initial_time),
                    str(stat_record.fetched_time)
                ]
            )
        return result
