import math
from typing import List
from cep_library import configs
from cep_library.raw.model.raw_settings import RawStatisticsBook
from cep_library.stats_helper.topic_stat_base import TopicStatBase


class RawServerStatAnalyzer:
    def __init__(self) -> None:
        self.raw_stats: dict[str, dict[str, RawStatisticsBook]] = {}
        self.expected_event_count: float = 1.0

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
            expected_count_per_sec = self.get_expected_event_count(
                host_name, topic_name
            )
            byte_load_per_data = self.get_byte_per_data(host_name, topic_name, remote)
            return expected_count_per_sec * byte_load_per_data
        elif configs.EXPECTED_LOAD_TYPE == 2:
            return self.get_byte_per_data(host_name, topic_name, remote)
        else:
            raise Exception("Invalid load type detected!")

    def get_expected_event_count(self, host_name: str, topic_name: str) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            if self.raw_stats[host_name][topic_name].get_calculated():
                return self.raw_stats[host_name][
                    topic_name
                ].get_per_second_expected_event_count()
        return 5.0

    def update_raw_stats(self, rawstats: List[RawStatisticsBook]) -> None:
        for rs in rawstats:
            if rs.get_calculated():
                if (
                    configs.UPDATE_LATEST_STATS
                    and rs.producer_name in self.raw_stats
                    and rs.topic_name in self.raw_stats[rs.producer_name]
                ):
                    self.raw_stats[rs.producer_name][rs.topic_name].update_with_stats(
                        rs.local_write_time_ns_total,
                        rs.local_write_size_byte_total,
                        rs.local_write_data_count,
                        rs.remote_write_time_ns_total,
                        rs.remote_write_time_byte_total,
                        rs.remote_write_data_count,
                    )
                else:
                    if rs.producer_name not in self.raw_stats:
                        self.raw_stats[rs.producer_name] = {}
                    self.raw_stats[rs.producer_name][rs.topic_name] = rs

    def set_stat_min_max(
        self, host_name: str, topic_name: str, minx: float, maxx: float
    ) -> None:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            self.raw_stats[host_name][topic_name].minx = minx
            self.raw_stats[host_name][topic_name].maxx = maxx

    def get_avg_byte_mul_ns(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_avg_byte_mul_ns(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_avg_byte_mul_ns(remote))
        return default_val.get_default_val()

    def normalized_get_avg_byte_mul_ns(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].normalized_get_avg_byte_mul_ns(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_avg_byte_mul_ns(remote))
        return default_val.get_default_val()

    def get_avg_byte_mul_ns_square(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_avg_byte_mul_ns_square(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_avg_byte_mul_ns_square(remote))
        return default_val.get_default_val()

    def normalized_get_avg_byte_mul_ns_square(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][
                topic_name
            ].normalized_get_avg_byte_mul_ns_square(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(
                    stat.normalized_get_avg_byte_mul_ns_square(remote)
                )
        return default_val.get_default_val()

    def normalized_get_avg_byte_over_n(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].normalized_get_avg_byte_over_n(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_avg_byte_over_n(remote))
        return default_val.get_default_val()

    def get_avg_byte_over_n(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_avg_byte_over_n(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_avg_byte_over_n(remote))
        return default_val.get_default_val()

    def normalized_get_avg_byte_over_ns(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][
                topic_name
            ].normalized_get_avg_byte_over_ns(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_avg_byte_over_ns(remote))
        return default_val.get_default_val()

    def get_avg_byte_over_ns(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_avg_byte_over_ns(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_avg_byte_over_ns(remote))
        return default_val.get_default_val()

    def normalized_get_cp_sat_cost(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].normalized_get_cp_sat_cost(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_cp_sat_cost(remote))
        return default_val.get_default_val()

    def get_cp_sat_cost(self, host_name: str, topic_name: str, remote: bool) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_cp_sat_cost(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_cp_sat_cost(remote))
        return default_val.get_default_val()

    def normalized_get_avg_ns_over_byte(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][
                topic_name
            ].normalized_get_avg_ns_over_byte(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_avg_ns_over_byte(remote))
        return default_val.get_default_val()

    def get_byte_per_data(self, host_name: str, topic_name: str, remote: bool) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_byte_per_data(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_byte_per_data(remote))
        return default_val.get_default_val()

    def get_avg_ns_over_byte(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_avg_ns_over_byte(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_avg_ns_over_byte(remote))
        return default_val.get_default_val()

    def normalized_get_avg_ns_over_n(
        self, host_name: str, topic_name: str, remote: bool
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].normalized_get_avg_ns_over_n(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_avg_ns_over_n(remote))
        return default_val.get_default_val()

    def get_avg_ns_over_n(self, host_name: str, topic_name: str, remote: bool) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_avg_ns_over_n(remote)

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_avg_ns_over_n(remote))
        return default_val.get_default_val()

    def normalized_get_write_size_byte_total(
        self, host_name: str, topic_name: str
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][
                topic_name
            ].normalized_get_write_size_byte_total()

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_write_size_byte_total())
        return default_val.get_default_val()

    def get_write_size_byte_total(self, host_name: str, topic_name: str) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_write_size_byte_total()

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_write_size_byte_total())
        return default_val.get_default_val()

    def get_remote_write_time_byte_mean(self, host_name: str, topic_name: str) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][
                topic_name
            ].get_remote_write_time_byte_mean()

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_remote_write_time_byte_mean())
        return default_val.get_default_val()

    def normalized_get_write_time_ns_total(
        self, host_name: str, topic_name: str
    ) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][
                topic_name
            ].normalized_get_write_time_ns_total()

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.normalized_get_write_time_ns_total())
        return default_val.get_default_val()

    def get_write_time_ns_total(self, host_name: str, topic_name: str) -> float:
        if host_name in self.raw_stats and topic_name in self.raw_stats[host_name]:
            return self.raw_stats[host_name][topic_name].get_write_time_ns_total()

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.raw_stats.items():
            for _, stat in ts.items():
                default_val.add_value(stat.get_write_time_ns_total())
        return default_val.get_default_val()
