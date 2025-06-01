import math
from typing import List
from cep_library import configs
from cep_library.consumer.model.consumer_topic_stats import ConsumerTopicStats
from cep_library.stats_helper.topic_stat_base import TopicStatBase


class ConsumerServerStatics:
    def __init__(self) -> None:
        self.consumer_stats: dict[str, dict[str, ConsumerTopicStats]] = {}
        self.expected_event_counts: dict[str, dict[str, float]] = {}

        self.desired_cost_lower: float = configs.STAT_NORMALIZER_LOW
        self.desired_cost_upper: float = configs.STAT_NORMALIZER_HIGH
        self.minc: float = 0.0
        self.maxc: float = 0.0

    def normalize(
        self, actual_lower: float, actual_upper: float, value: float
    ) -> float:
        if configs.NORMALIZATION_APPROACH == 0:
            if actual_lower == 0 or actual_upper == 0 or actual_lower >= actual_upper:
                raise Exception(
                    "Invalid value encountered during min-max normalization!"
                )
            if value > actual_upper or value < actual_lower:
                raise Exception(
                    f"Value is not between the actual upper and lower limits: {value}, {actual_lower}-{actual_upper}!!!!!"
                )
            return self.desired_cost_lower + (value - actual_lower) * (
                self.desired_cost_upper - self.desired_cost_lower
            ) / (actual_upper - actual_lower)
        elif configs.NORMALIZATION_APPROACH == 1:
            return math.log2(value + 1)
        else:
            raise Exception("Invalid normalization approach detected!")

    def get_expected_event_counts(self, host_name: str, topic_name: str) -> float:
        return self.expected_event_counts[host_name][topic_name]

    def increment_expected_event_counts(
        self, host_name: str, topic_name: str, count: float
    ) -> None:
        if host_name not in self.expected_event_counts:
            self.expected_event_counts[host_name] = {}
        if topic_name not in self.expected_event_counts[host_name]:
            self.expected_event_counts[host_name][topic_name] = 0.0
        self.expected_event_counts[host_name][topic_name] += count

    def update_consumer_stats(self, consumer_stats: List[ConsumerTopicStats]) -> None:
        for cs in consumer_stats:
            if cs.calculated():
                if (
                    configs.UPDATE_LATEST_STATS
                    and cs.host_name in self.consumer_stats
                    and cs.topic_name in self.consumer_stats[cs.host_name]
                ):
                    self.consumer_stats[cs.host_name][cs.topic_name].update_with_stats(
                        cs.remote_reading_byte,
                        cs.remote_reading_ns,
                        cs.remote_read_data_count,
                        cs.local_reading_byte,
                        cs.local_reading_ns,
                        cs.local_read_data_count,
                    )
                else:
                    if cs.host_name not in self.consumer_stats:
                        self.consumer_stats[cs.host_name] = {}
                    self.consumer_stats[cs.host_name][cs.topic_name] = cs

    def set_stat_min_max(
        self, host_name: str, topic_name: str, minx: float, maxx: float
    ) -> None:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            self.consumer_stats[host_name][topic_name].minx = minx
            self.consumer_stats[host_name][topic_name].maxx = maxx

    def normalized_get_reading_cp_sat_cost(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][
                topic_name
            ].normalized_get_reading_cp_sat_cost(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_reading_cp_sat_cost(remote))
        return default_val.get_default_val()

    def get_reading_cp_sat_cost(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][topic_name].get_reading_cp_sat_cost(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_reading_cp_sat_cost(remote))
        return default_val.get_default_val()

    def normalize_get_reading_ns_over_byte(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][
                topic_name
            ].normalize_get_reading_ns_over_byte(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalize_get_reading_ns_over_byte(remote))
        return default_val.get_default_val()

    def normalize_get_expected_load_per_second(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if configs.EXPECTED_LOAD_TYPE == 1:
            value: float = self.get_expected_load_per_second(
                host_name, topic_name, remote
            )
            return self.normalize(self.minc, self.maxc, value)
        else:
            raise Exception("Invalid load type detected!")

    def get_expected_load_per_second(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if configs.EXPECTED_LOAD_TYPE == 1:
            expected_count_per_sec = self.get_expected_event_counts(
                host_name, topic_name
            )
            byte_load_per_data = self.get_byte_per_data(host_name, topic_name, remote)
            return expected_count_per_sec * byte_load_per_data
        elif configs.EXPECTED_LOAD_TYPE == 2:
            return self.get_byte_per_data(host_name, topic_name, remote)
        else:
            raise Exception("Invalid load type detected!")

    def get_byte_per_data(self, host_name: str, topic_name: str, remote: bool) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][topic_name].get_byte_per_data(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_byte_per_data(remote))
        return default_val.get_default_val()

    def get_reading_ns_over_byte(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][topic_name].get_reading_ns_over_byte(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_reading_ns_over_byte(remote))
        return default_val.get_default_val()

    def normalize_get_reading_byte_mul_ns(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][
                topic_name
            ].normalize_get_reading_byte_mul_ns(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalize_get_reading_byte_mul_ns(remote))
        return default_val.get_default_val()

    def get_reading_byte_mul_ns(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][topic_name].get_reading_byte_mul_ns(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_reading_byte_mul_ns(remote))
        return default_val.get_default_val()

    def normalize_get_reading_byte_mul_ns_square(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][
                topic_name
            ].normalize_get_reading_byte_mul_ns_square(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(
                    stat.normalize_get_reading_byte_mul_ns_square(remote)
                )
        return default_val.get_default_val()

    def get_reading_byte_mul_ns_square(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            host_name in self.consumer_stats
            and topic_name in self.consumer_stats[host_name]
        ):
            return self.consumer_stats[host_name][
                topic_name
            ].get_reading_byte_mul_ns_square(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.consumer_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_reading_byte_mul_ns_square(remote))
        return default_val.get_default_val()
