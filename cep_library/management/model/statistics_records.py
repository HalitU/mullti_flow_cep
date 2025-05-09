from datetime import datetime, timezone
from cep_library.stats_helper.mean_stat_base import MeanStatBase

class TopicStatisticDetail:
    def __init__(self):
        self.remote_byte: float = 0.0
        self.remote_ns: float = 0.0
        self.remote_data_count: float = 0.0

        self.local_byte: float = 0.0
        self.local_ns: float = 0.0
        self.local_data_count: float = 0.0
        
        self.raw_source_name: str = ""

class SingleStatisticsRecord:
    def __init__(self, action_name:str):
        self.action_name = action_name

        self.total_elapsed = 0.0
        self.task_execution_time = 0.0
        self.execution_elapsed_ms = 0.0
        
        self.raw_elapsed_time_from_init_ms = 0.0
        self.raw_elapsed_max_time_from_init_ms = 0.0
        self.raw_elapsed_min_time_from_init_ms = 0.0
        
        self.in_topic_records:dict[str, TopicStatisticDetail] = {}
        
        self.out_local_data_byte = 0.0
        self.out_local_elapsed_ms: float = 0.0
        self.out_local_data_count = 0.0
        
        self.out_remote_data_byte = 0.0
        self.out_remote_elapsed_ms: float = 0.0
        self.out_remote_data_count = 0.0

class TopicStatisticRecord:
    def __init__(self, topic_name:str) -> None:
        self.topic_name = topic_name
        self.in_topic_local_read_byte = MeanStatBase()
        self.in_topic_local_read_ns = MeanStatBase()
        self.in_topic_local_data_count = MeanStatBase()
        
        self.in_topic_remote_read_byte = MeanStatBase()
        self.in_topic_remote_read_ns = MeanStatBase()
        self.in_topic_remote_data_count = MeanStatBase()

    def reset(self):
        self.in_topic_local_read_byte.reset()
        self.in_topic_local_read_ns.reset()
        self.in_topic_local_data_count.reset()
        
        self.in_topic_remote_read_byte.reset()
        self.in_topic_remote_read_ns.reset()
        self.in_topic_remote_data_count.reset()

class StatisticRecord:
    def __init__(self, action_name:str):
        self.action_name: str = action_name
        self.miss_count:MeanStatBase = MeanStatBase()
        self.hit_count:MeanStatBase = MeanStatBase()
        self.activation_time_ns: MeanStatBase = MeanStatBase()

        self.crr_total_elapsed_s_stats = MeanStatBase()
        self.crr_task_execution_ns_stats = MeanStatBase()
        self.crr_execution_elapsed_ms_stats = MeanStatBase()

        self.crr_raw_elapsed_time_from_init_ms = MeanStatBase()
        self.crr_raw_elapsed_max_time_from_init_ms = MeanStatBase()
        self.crr_raw_elapsed_min_time_from_init_ms = MeanStatBase()
        
        self.in_topic_stats:dict[str, TopicStatisticRecord] = {}
        
        self.crr_out_local_data_byte_stats = MeanStatBase()
        self.crr_out_local_data_ns_stats = MeanStatBase()
        self.crr_out_local_data_count = MeanStatBase()
        
        self.crr_out_remote_data_byte_stats = MeanStatBase()
        self.crr_out_remote_data_ns_stats = MeanStatBase()
        self.crr_out_remote_data_count = MeanStatBase()
        
        self.initial_time: datetime = datetime.now(timezone.utc)
        self.fetched_time: datetime = datetime.now(timezone.utc)

    def reset_stats(self):
        self.miss_count.reset()
        self.hit_count.reset()
        self.activation_time_ns.reset()
        
        self.crr_total_elapsed_s_stats.reset()
        self.crr_task_execution_ns_stats.reset()
        self.crr_execution_elapsed_ms_stats.reset()

        self.crr_raw_elapsed_time_from_init_ms.reset()
        self.crr_raw_elapsed_max_time_from_init_ms.reset()
        self.crr_raw_elapsed_min_time_from_init_ms.reset()

        val:TopicStatisticRecord
        for _, val in self.in_topic_stats.items():
            val.reset()

        self.crr_out_local_data_byte_stats.reset()
        self.crr_out_local_data_ns_stats.reset()
        self.crr_out_local_data_count.reset()
        
        self.crr_out_remote_data_byte_stats.reset()
        self.crr_out_remote_data_ns_stats.reset()
        self.crr_out_remote_data_count.reset()
        
        self.initial_time = datetime.now(timezone.utc)

    def update_miss_stats(self, ssr:SingleStatisticsRecord):
        self.miss_count.add(1)
        self.action_name = ssr.action_name
        
        vals:TopicStatisticDetail
        for topic_name, vals in ssr.in_topic_records.items():
            if topic_name in self.in_topic_stats:
                self.in_topic_stats[topic_name].in_topic_local_read_byte.add(vals.local_byte)
                self.in_topic_stats[topic_name].in_topic_local_read_ns.add(vals.local_ns)
                self.in_topic_stats[topic_name].in_topic_local_data_count.add(vals.local_data_count)
                
                self.in_topic_stats[topic_name].in_topic_remote_read_byte.add(vals.remote_byte)
                self.in_topic_stats[topic_name].in_topic_remote_read_ns.add(vals.remote_ns)
                self.in_topic_stats[topic_name].in_topic_remote_data_count.add(vals.remote_data_count)

    def update_stats(self, ssr: SingleStatisticsRecord):
        self.hit_count.add(1)
        self.action_name = ssr.action_name
        
        vals:TopicStatisticDetail
        for topic_name, vals in ssr.in_topic_records.items():
            if topic_name in self.in_topic_stats:
                self.in_topic_stats[topic_name].in_topic_local_read_byte.add(vals.local_byte)
                self.in_topic_stats[topic_name].in_topic_local_read_ns.add(vals.local_ns)
                self.in_topic_stats[topic_name].in_topic_local_data_count.add(vals.local_data_count)
                
                self.in_topic_stats[topic_name].in_topic_remote_read_byte.add(vals.remote_byte)
                self.in_topic_stats[topic_name].in_topic_remote_read_ns.add(vals.remote_ns)
                self.in_topic_stats[topic_name].in_topic_remote_data_count.add(vals.remote_data_count)

        self.crr_total_elapsed_s_stats.add(ssr.total_elapsed) 
        self.crr_task_execution_ns_stats.add(ssr.task_execution_time)
        self.crr_execution_elapsed_ms_stats.add(ssr.execution_elapsed_ms)
        
        self.crr_out_local_data_byte_stats.add(ssr.out_local_data_byte)
        self.crr_out_local_data_ns_stats.add(ssr.out_local_elapsed_ms)
        self.crr_out_local_data_count.add(ssr.out_local_data_count)
        
        self.crr_out_remote_data_byte_stats.add(ssr.out_remote_data_byte)
        self.crr_out_remote_data_ns_stats.add(ssr.out_remote_elapsed_ms)
        self.crr_out_remote_data_count.add(ssr.out_remote_data_count)
        
        self.crr_raw_elapsed_time_from_init_ms.add(ssr.raw_elapsed_time_from_init_ms) 
        self.crr_raw_elapsed_max_time_from_init_ms.add(ssr.raw_elapsed_max_time_from_init_ms)
        self.crr_raw_elapsed_min_time_from_init_ms.add(ssr.raw_elapsed_min_time_from_init_ms)
