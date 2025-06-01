from cep_library.stats_helper.mean_stat_base import MeanStatBase


class AlarmStatistics:
    def __init__(self) -> None:
        self.oldest_processing_time: dict[str, MeanStatBase] = {}
        self.newest_processing_time: dict[str, MeanStatBase] = {}
        self.oldest_1_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_2_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_3_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_4_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_5_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_6_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_7_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_8_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_9_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_10_second_hit_count: dict[str, MeanStatBase] = {}
        self.oldest_10_plus_second_hit_count: dict[str, MeanStatBase] = {}

    def init_alarm_second_stats(self, source_name: str) -> None:
        if source_name not in self.oldest_1_second_hit_count:
            self.oldest_1_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_2_second_hit_count:
            self.oldest_2_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_3_second_hit_count:
            self.oldest_3_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_4_second_hit_count:
            self.oldest_4_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_5_second_hit_count:
            self.oldest_5_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_6_second_hit_count:
            self.oldest_6_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_7_second_hit_count:
            self.oldest_7_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_8_second_hit_count:
            self.oldest_8_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_9_second_hit_count:
            self.oldest_9_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_10_second_hit_count:
            self.oldest_10_second_hit_count[source_name] = MeanStatBase()
        if source_name not in self.oldest_10_plus_second_hit_count:
            self.oldest_10_plus_second_hit_count[source_name] = MeanStatBase()

    def update_oldest_second_hit(self, source_name: str, duration_s: float) -> None:
        if duration_s <= 1.0:
            self.oldest_1_second_hit_count[source_name].add(1)
        if duration_s <= 2.0:
            self.oldest_2_second_hit_count[source_name].add(1)
        if duration_s <= 3.0:
            self.oldest_3_second_hit_count[source_name].add(1)
        if duration_s <= 4.0:
            self.oldest_4_second_hit_count[source_name].add(1)
        if duration_s <= 5.0:
            self.oldest_5_second_hit_count[source_name].add(1)
        if duration_s <= 6.0:
            self.oldest_6_second_hit_count[source_name].add(1)
        if duration_s <= 7.0:
            self.oldest_7_second_hit_count[source_name].add(1)
        if duration_s <= 8.0:
            self.oldest_8_second_hit_count[source_name].add(1)
        if duration_s <= 9.0:
            self.oldest_9_second_hit_count[source_name].add(1)
        if duration_s <= 10.0:
            self.oldest_10_second_hit_count[source_name].add(1)
        if duration_s > 10.0:
            self.oldest_10_plus_second_hit_count[source_name].add(1)

    def get_device_stat_header(self):
        return [
            "record_id",
            "record_date",
            "host_name",
            "topic_name",
            "raw_date",
            "alarm_exists",
            "data_name",
            "oldest_processing_time",
            "newest_processing_time",
            "oldest_processing_time_count",
            "newest_processing_time_count",
            "miss_count",
            "hit_count",
            "src_name",
            "src_stat.count",
            "miss_cnt.coun",
            "oldest_1_second_hit_count",
            "oldest_2_second_hit_count",
            "oldest_3_second_hit_count",
            "oldest_4_second_hit_count",
            "oldest_5_second_hit_count",
            "oldest_6_second_hit_count",
            "oldest_7_second_hit_count",
            "oldest_8_second_hit_count",
            "oldest_9_second_hit_count",
            "oldest_10_second_hit_count",
            "oldest_10_plus_second_hit_count",
        ]
