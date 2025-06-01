from typing import List
from cep_library.management.model.resource_usage import ResourceUsageRecord
from cep_library.management.model.statistics_records import (
    StatisticRecord,
    TopicStatisticRecord,
)
from cep_library import configs
import math

from cep_library.stats_helper.topic_stat_base import TopicStatBase


class ServerStatisticsBase:
    def __init__(self) -> None:
        self.desired_cost_lower: float = configs.STAT_NORMALIZER_LOW
        self.desired_cost_upper: float = configs.STAT_NORMALIZER_HIGH
        self.minx: float
        self.maxx: float

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

        raise Exception("Invalid normalization approach!")

    def convert_to_int(self, value: float) -> int:
        return math.ceil(value)


class CPEventTopicStatistic(ServerStatisticsBase):
    def __init__(self) -> None:
        super().__init__()

        self.stat: TopicStatisticRecord

    def data_exists(self):
        return (
            self.stat.in_topic_local_data_count.count
            + self.stat.in_topic_remote_data_count.count
            > 0
        )

    def add_stat(self, new_stat: TopicStatisticRecord):
        self.stat.in_topic_local_read_byte.add_multiple(
            new_stat.in_topic_local_read_byte.mean,
            new_stat.in_topic_local_read_byte.count,
        )
        self.stat.in_topic_local_read_ns.add_multiple(
            new_stat.in_topic_local_read_ns.mean, new_stat.in_topic_local_read_ns.count
        )
        self.stat.in_topic_remote_read_byte.add_multiple(
            new_stat.in_topic_remote_read_byte.mean,
            new_stat.in_topic_remote_read_byte.count,
        )
        self.stat.in_topic_remote_read_ns.add_multiple(
            new_stat.in_topic_remote_read_ns.mean,
            new_stat.in_topic_remote_read_ns.count,
        )
        self.stat.in_topic_local_data_count.add_multiple(
            new_stat.in_topic_local_data_count.mean,
            new_stat.in_topic_local_data_count.count,
        )
        self.stat.in_topic_remote_data_count.add_multiple(
            new_stat.in_topic_remote_data_count.mean,
            new_stat.in_topic_remote_data_count.count,
        )

    def get_total_byte(self) -> float:
        return (
            self.stat.in_topic_remote_read_byte.total()
            + self.stat.in_topic_local_read_byte.total()
        )

    def normalized_get_input_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_USE_NORMALIZED_COST == False:
            return self.get_input_cp_sat_cost(remote)

        return self.normalize(self.minx, self.maxx, self.get_input_cp_sat_cost(remote))

    def get_input_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_COST_TYPE == 0:
            return self.normalized_get_avg_ns_over_byte(remote)
        elif configs.CP_SAT_COST_TYPE == 1:
            return self.get_avg_ns_over_byte(remote)
        elif configs.CP_SAT_COST_TYPE == 2:
            return self.get_byte_per_data(remote)
        elif configs.CP_SAT_COST_TYPE == 3:
            return self.get_avg_byte_mul_ns_square(remote)
        elif configs.CP_SAT_COST_TYPE == 4:
            return self.get_avg_byte_mul_ns(remote) * 1000.0 * 1024.0
        else:
            raise Exception("Invalid cost type!")

    def get_byte_per_data(self, remote: bool) -> float:
        if configs.DATA_LOAD_TYPE == 2:
            return (
                1024.0
                * (
                    self.stat.in_topic_local_read_byte.total()
                    + self.stat.in_topic_remote_read_byte.total()
                )
                / (
                    self.stat.in_topic_local_data_count.count
                    + self.stat.in_topic_remote_data_count.count
                )
            )

        if not configs.USE_CUMULATIVE_LOAD:
            size = (
                1024.0
                * (
                    self.stat.in_topic_local_read_byte.total()
                    + self.stat.in_topic_remote_read_byte.total()
                )
                / (
                    self.stat.in_topic_local_data_count.count
                    + self.stat.in_topic_remote_data_count.count
                )
            )

            if remote:
                return size * configs.CP_SAT_STATISTICS_REMOTE_MULTIPLIER
            else:
                return size
        else:
            return (
                1024.0
                * (
                    self.stat.in_topic_local_read_byte.total()
                    + self.stat.in_topic_remote_read_byte.total()
                )
                / configs.evaluation_period
            )

    def normalized_get_avg_ns_over_byte(self, remote: bool):
        return self.normalize(self.minx, self.maxx, self.get_avg_ns_over_byte(remote))

    def get_avg_ns_over_byte(self, remote: bool) -> float:
        if remote:
            if self.stat.in_topic_remote_read_ns.count == 0:
                return (
                    self.stat.in_topic_local_read_ns.mean
                    / self.stat.in_topic_local_read_byte.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.stat.in_topic_remote_read_ns.mean
                / self.stat.in_topic_remote_read_byte.mean
            )
        else:
            if self.stat.in_topic_local_read_ns.count == 0:
                return (
                    self.stat.in_topic_remote_read_ns.mean
                    / self.stat.in_topic_remote_read_byte.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.stat.in_topic_local_read_ns.mean
                / self.stat.in_topic_local_read_byte.mean
            )

    def get_avg_byte_mul_ns_square(self, remote: bool) -> float:
        if not configs.USE_CUMULATIVE_COST:
            if remote:
                if self.stat.in_topic_remote_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_local_read_ns.mean / 1000.0) ** 2
                        * self.stat.in_topic_local_read_byte.mean
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_remote_read_ns.mean / 1000.0
                ) ** 2 * self.stat.in_topic_remote_read_byte.mean
            else:
                if self.stat.in_topic_local_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_remote_read_ns.mean / 1000.0) ** 2
                        * self.stat.in_topic_remote_read_byte.mean
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_local_read_ns.mean / 1000.0
                ) ** 2 * self.stat.in_topic_local_read_byte.mean
        else:
            if remote:
                if self.stat.in_topic_remote_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_local_read_ns.total() / 1000.0) ** 2
                        * self.stat.in_topic_local_read_byte.total()
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_remote_read_ns.total() / 1000.0
                ) ** 2 * self.stat.in_topic_remote_read_byte.total()
            else:
                if self.stat.in_topic_local_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_remote_read_ns.total() / 1000.0) ** 2
                        * self.stat.in_topic_remote_read_byte.total()
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_local_read_ns.total() / 1000.0
                ) ** 2 * self.stat.in_topic_local_read_byte.total()

    def get_avg_byte_mul_ns(self, remote: bool) -> float:
        if not configs.USE_CUMULATIVE_COST:
            if remote:
                if self.stat.in_topic_remote_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_local_read_ns.mean / 1000.0)
                        * self.stat.in_topic_local_read_byte.mean
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_remote_read_ns.mean / 1000.0
                ) * self.stat.in_topic_remote_read_byte.mean
            else:
                if self.stat.in_topic_local_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_remote_read_ns.mean / 1000.0)
                        * self.stat.in_topic_remote_read_byte.mean
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_local_read_ns.mean / 1000.0
                ) * self.stat.in_topic_local_read_byte.mean
        else:
            if remote:
                if self.stat.in_topic_remote_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_local_read_ns.total() / 1000.0)
                        * self.stat.in_topic_local_read_byte.total()
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_remote_read_ns.total() / 1000.0
                ) * self.stat.in_topic_remote_read_byte.total()
            else:
                if self.stat.in_topic_local_read_ns.count == 0:
                    return (
                        (self.stat.in_topic_remote_read_ns.total() / 1000.0)
                        * self.stat.in_topic_remote_read_byte.total()
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.stat.in_topic_local_read_ns.total() / 1000.0
                ) * self.stat.in_topic_local_read_byte.total()


class CPEventStatistic(ServerStatisticsBase):
    def __init__(self) -> None:
        super().__init__()

        self.sr: StatisticRecord
        self.topic_stats: dict[str, CPEventTopicStatistic] = {}
        self.event_stat_count: dict[str, int] = {}

    def data_exists(self):
        return self.sr.crr_task_execution_ns_stats.count > 0

    def add_stat(self, new_stat: StatisticRecord):
        self.sr.miss_count.add_multiple(
            new_stat.miss_count.mean, new_stat.miss_count.count
        )
        self.sr.hit_count.add_multiple(
            new_stat.hit_count.mean, new_stat.hit_count.count
        )
        self.sr.activation_time_ns.add_multiple(
            new_stat.activation_time_ns.mean, new_stat.activation_time_ns.count
        )
        self.sr.crr_total_elapsed_s_stats.add_multiple(
            new_stat.crr_total_elapsed_s_stats.mean,
            new_stat.crr_total_elapsed_s_stats.count,
        )
        self.sr.crr_task_execution_ns_stats.add_multiple(
            new_stat.crr_task_execution_ns_stats.mean,
            new_stat.crr_task_execution_ns_stats.count,
        )
        self.sr.crr_execution_elapsed_ms_stats.add_multiple(
            new_stat.crr_execution_elapsed_ms_stats.mean,
            new_stat.crr_execution_elapsed_ms_stats.count,
        )
        self.sr.crr_raw_elapsed_time_from_init_ms.add_multiple(
            new_stat.crr_raw_elapsed_time_from_init_ms.mean,
            new_stat.crr_raw_elapsed_time_from_init_ms.count,
        )
        self.sr.crr_raw_elapsed_max_time_from_init_ms.add_multiple(
            new_stat.crr_raw_elapsed_max_time_from_init_ms.mean,
            new_stat.crr_raw_elapsed_max_time_from_init_ms.count,
        )
        self.sr.crr_raw_elapsed_min_time_from_init_ms.add_multiple(
            new_stat.crr_raw_elapsed_min_time_from_init_ms.mean,
            new_stat.crr_raw_elapsed_min_time_from_init_ms.count,
        )
        self.sr.crr_out_local_data_byte_stats.add_multiple(
            new_stat.crr_out_local_data_byte_stats.mean,
            new_stat.crr_out_local_data_byte_stats.count,
        )
        self.sr.crr_out_local_data_ns_stats.add_multiple(
            new_stat.crr_out_local_data_ns_stats.mean,
            new_stat.crr_out_local_data_ns_stats.count,
        )
        self.sr.crr_out_remote_data_byte_stats.add_multiple(
            new_stat.crr_out_remote_data_byte_stats.mean,
            new_stat.crr_out_remote_data_byte_stats.count,
        )
        self.sr.crr_out_remote_data_ns_stats.add_multiple(
            new_stat.crr_out_remote_data_ns_stats.mean,
            new_stat.crr_out_remote_data_ns_stats.count,
        )
        self.sr.crr_out_local_data_count.add_multiple(
            new_stat.crr_out_local_data_count.mean,
            new_stat.crr_out_local_data_count.count,
        )
        self.sr.crr_out_remote_data_count.add_multiple(
            new_stat.crr_out_remote_data_count.mean,
            new_stat.crr_out_remote_data_count.count,
        )

        if self.data_exists():
            if new_stat.action_name not in self.event_stat_count:
                self.event_stat_count[new_stat.action_name] = 1
            else:
                self.event_stat_count[new_stat.action_name] += 1

    def get_migration_cost(self):
        return (
            self.sr.crr_out_local_data_byte_stats.total()
            + self.sr.crr_out_remote_data_byte_stats.total()
        )

    def get_cep_entire_execution_elapsed_ms_stats_avg(self):
        return self.sr.crr_execution_elapsed_ms_stats.mean

    def get_execution_duration_ms_avg(self):
        return self.sr.crr_task_execution_ns_stats.mean

    def get_execution_duration_ms_total(self):
        return self.sr.crr_task_execution_ns_stats.total() / (
            1.0 * self.event_stat_count[self.sr.action_name]
        )

    def normalized_execution_get_avg_ns_over_byte(self, remote: bool):
        return self.normalize(
            self.minx, self.maxx, self.execution_get_avg_ns_over_byte(remote)
        )

    def execution_get_avg_ns_over_byte(self, activation: bool) -> float:
        out_cost = (
            self.sr.crr_out_local_data_byte_stats.total()
            + self.sr.crr_out_remote_data_byte_stats.total()
        )
        in_cost = sum(
            [
                s.stat.in_topic_local_read_byte.total()
                + s.stat.in_topic_remote_read_byte.total()
                for _, s in self.topic_stats.items()
            ]
        )
        exec_time_cost = self.sr.crr_task_execution_ns_stats.total()

        eventual_execution_cost: float
        if configs.STATISTICS_EVENT_ACTIVATION_COST_TYPE == 0:
            eventual_execution_cost = exec_time_cost / (in_cost + out_cost)

        elif configs.STATISTICS_EVENT_ACTIVATION_COST_TYPE == 1:
            eventual_execution_cost = exec_time_cost * (in_cost + out_cost)

        elif configs.STATISTICS_EVENT_ACTIVATION_COST_TYPE == 2:
            eventual_execution_cost = in_cost + out_cost

        else:
            raise Exception("Invalid STATISTICS_EVENT_ACTIVATION_COST_TYPE choice!!!")

        if eventual_execution_cost == 0:
            raise Exception("Cost cannot be 0 here!")

        if activation:
            return eventual_execution_cost * configs.EVENT_ACTIVATION_MULTIPLIER
        return eventual_execution_cost

    def normalized_get_output_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_USE_NORMALIZED_COST == False:
            return self.get_output_cp_sat_cost(remote)

        return self.normalize(self.minx, self.maxx, self.get_output_cp_sat_cost(remote))

    def get_output_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_COST_TYPE == 0:
            return self.normalized_output_get_avg_ns_over_byte(remote)
        elif configs.CP_SAT_COST_TYPE == 1:
            return self.output_get_avg_ns_over_byte(remote)
        elif configs.CP_SAT_COST_TYPE == 2:
            return self.get_byte_per_data(remote)
        elif configs.CP_SAT_COST_TYPE == 3:
            return self.output_get_avg_byte_mul_ns_square(remote)
        elif configs.CP_SAT_COST_TYPE == 4:
            return self.output_get_avg_byte_mul_ns(remote) * 1000.0 * 1024.0
        else:
            raise Exception("Invalid cost type!")

    def get_byte_per_data(self, remote: bool) -> float:
        if configs.DATA_LOAD_TYPE == 2:
            return (
                1024.0
                * (
                    self.sr.crr_out_local_data_byte_stats.total()
                    + self.sr.crr_out_remote_data_byte_stats.total()
                )
                / (
                    self.sr.crr_out_local_data_count.count
                    + self.sr.crr_out_remote_data_count.count
                )
            )

        if not configs.USE_CUMULATIVE_LOAD:
            size = (
                1024.0
                * (
                    self.sr.crr_out_local_data_byte_stats.total()
                    + self.sr.crr_out_remote_data_byte_stats.total()
                )
                / (
                    self.sr.crr_out_local_data_count.count
                    + self.sr.crr_out_remote_data_count.count
                )
            )

            if remote:
                return size * configs.CP_SAT_STATISTICS_REMOTE_MULTIPLIER
            else:
                return size
        else:
            return (
                1024.0
                * (
                    self.sr.crr_out_local_data_byte_stats.total()
                    + self.sr.crr_out_remote_data_byte_stats.total()
                )
                / configs.evaluation_period
            )

    def normalized_output_get_avg_ns_over_byte(self, remote: bool):
        return self.normalize(
            self.minx, self.maxx, self.output_get_avg_ns_over_byte(remote)
        )

    def output_get_avg_ns_over_byte(self, remote: bool) -> float:
        if remote:
            if self.sr.crr_out_remote_data_ns_stats.count == 0:
                return (
                    self.sr.crr_out_local_data_ns_stats.mean
                    / self.sr.crr_out_local_data_byte_stats.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.sr.crr_out_remote_data_ns_stats.mean
                / self.sr.crr_out_remote_data_byte_stats.mean
            )
        else:
            if self.sr.crr_out_local_data_ns_stats.count == 0:
                return (
                    self.sr.crr_out_remote_data_ns_stats.mean
                    / self.sr.crr_out_remote_data_byte_stats.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.sr.crr_out_local_data_ns_stats.mean
                / self.sr.crr_out_local_data_byte_stats.mean
            )

    def output_get_avg_byte_mul_ns_square(self, remote: bool) -> float:
        if not configs.USE_CUMULATIVE_COST:
            if remote:
                if self.sr.crr_out_remote_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_local_data_ns_stats.mean / 1000.0) ** 2
                        * self.sr.crr_out_local_data_byte_stats.mean
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_remote_data_ns_stats.mean / 1000.0
                ) ** 2 * self.sr.crr_out_remote_data_byte_stats.mean
            else:
                if self.sr.crr_out_local_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_remote_data_ns_stats.mean / 1000.0) ** 2
                        * self.sr.crr_out_remote_data_byte_stats.mean
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_local_data_ns_stats.mean / 1000.0
                ) ** 2 * self.sr.crr_out_local_data_byte_stats.mean
        else:
            if remote:
                if self.sr.crr_out_remote_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_local_data_ns_stats.total() / 1000.0) ** 2
                        * self.sr.crr_out_local_data_byte_stats.total()
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_remote_data_ns_stats.total() / 1000.0
                ) ** 2 * self.sr.crr_out_remote_data_byte_stats.total()
            else:
                if self.sr.crr_out_local_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_remote_data_ns_stats.total() / 1000.0) ** 2
                        * self.sr.crr_out_remote_data_byte_stats.total()
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_local_data_ns_stats.total() / 1000.0
                ) ** 2 * self.sr.crr_out_local_data_byte_stats.total()

    def output_get_avg_byte_mul_ns(self, remote: bool) -> float:
        if not configs.USE_CUMULATIVE_COST:
            if remote:
                if self.sr.crr_out_remote_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_local_data_ns_stats.mean / 1000.0)
                        * self.sr.crr_out_local_data_byte_stats.mean
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_remote_data_ns_stats.mean / 1000.0
                ) * self.sr.crr_out_remote_data_byte_stats.mean
            else:
                if self.sr.crr_out_local_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_remote_data_ns_stats.mean / 1000.0)
                        * self.sr.crr_out_remote_data_byte_stats.mean
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_local_data_ns_stats.mean / 1000.0
                ) * self.sr.crr_out_local_data_byte_stats.mean
        else:
            if remote:
                if self.sr.crr_out_remote_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_local_data_ns_stats.total() / 1000.0)
                        * self.sr.crr_out_local_data_byte_stats.total()
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_remote_data_ns_stats.total() / 1000.0
                ) * self.sr.crr_out_remote_data_byte_stats.total()
            else:
                if self.sr.crr_out_local_data_ns_stats.count == 0:
                    return (
                        (self.sr.crr_out_remote_data_ns_stats.total() / 1000.0)
                        * self.sr.crr_out_remote_data_byte_stats.total()
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                return (
                    self.sr.crr_out_local_data_ns_stats.total() / 1000.0
                ) * self.sr.crr_out_local_data_byte_stats.total()


class ManagementServerStatisticsAnalyzer:
    def __init__(self) -> None:
        self.event_statistics: dict[str, CPEventStatistic] = {}
        self.expected_event_input_counts: dict[str, dict[str, float]] = {}
        self.expected_event_output_counts: dict[str, dict[str, float]] = {}

        self.desired_cost_lower: float = configs.STAT_NORMALIZER_LOW
        self.desired_cost_upper: float = configs.STAT_NORMALIZER_HIGH
        self.minc: float = 0.0
        self.maxc: float = 0.0

    def get_expected_event_input_counts(self, host_name: str, topic_name: str) -> float:
        if (
            host_name in self.expected_event_input_counts
            and topic_name in self.expected_event_input_counts[host_name]
        ):
            return self.expected_event_input_counts[host_name][topic_name]
        return 5.0

    def increment_expected_event_input_counts(
        self, host_name: str, topic_name: str, count: float
    ) -> None:
        if host_name not in self.expected_event_input_counts:
            self.expected_event_input_counts[host_name] = {}
        if topic_name not in self.expected_event_input_counts[host_name]:
            self.expected_event_input_counts[host_name][topic_name] = 0.0
        self.expected_event_input_counts[host_name][topic_name] += count

    def get_expected_event_ouput_counts(self, host_name: str, topic_name: str) -> float:
        if (
            host_name in self.expected_event_output_counts
            and topic_name in self.expected_event_output_counts[host_name]
        ):
            return self.expected_event_output_counts[host_name][topic_name]
        return 5.0

    def increment_expected_event_output_counts(
        self, host_name: str, topic_name: str, count: float
    ) -> None:
        if host_name not in self.expected_event_output_counts:
            self.expected_event_output_counts[host_name] = {}
        if topic_name not in self.expected_event_output_counts[host_name]:
            self.expected_event_output_counts[host_name][topic_name] = 0.0
        self.expected_event_output_counts[host_name][topic_name] += count

    def updateStatistics(
        self, device_statistic_records: List[ResourceUsageRecord]
    ) -> List[str]:
        print("**********===========*********************")
        print("Updating statistics with collected values")

        valid_events: dict[str, List[StatisticRecord]] = {}
        valid_input_topics: dict[str, dict[str, List[TopicStatisticRecord]]] = {}

        for device_record in device_statistic_records:

            statistic_record: StatisticRecord
            for statistic_record in device_record.statistic_records:

                if (
                    statistic_record.hit_count.count == 0
                    and statistic_record.miss_count.count > 0
                ):
                    print(
                        f"[SERVER] {statistic_record.action_name}, no hits but miss data exists."
                    )

                if statistic_record.action_name not in valid_events:
                    valid_events[statistic_record.action_name] = []
                valid_events[statistic_record.action_name].append(statistic_record)

                for topic_name, topic_stat in statistic_record.in_topic_stats.items():
                    if statistic_record.action_name not in valid_input_topics:
                        valid_input_topics[statistic_record.action_name] = {}
                    if (
                        topic_name
                        not in valid_input_topics[statistic_record.action_name]
                    ):
                        valid_input_topics[statistic_record.action_name][
                            topic_name
                        ] = []
                    valid_input_topics[statistic_record.action_name][topic_name].append(
                        topic_stat
                    )

        return self.process_valid_stats(valid_events, valid_input_topics)

    def process_valid_stats(
        self,
        valid_events: dict[str, List[StatisticRecord]],
        valid_input_topics: dict[str, dict[str, List[TopicStatisticRecord]]],
    ) -> List[str]:
        processed_tasks: list[str] = []

        for event_name, event_stat_list in valid_events.items():
            processed_tasks.append(event_name)

            if configs.UPDATE_LATEST_STATS and event_name in self.event_statistics:
                event_stats = self.event_statistics[event_name]
            else:
                event_stats = CPEventStatistic()
                event_stats.sr = StatisticRecord(event_name)

            for event_stat_list_item in event_stat_list:
                event_stats.add_stat(event_stat_list_item)

            for valid_topic_name, valid_topic_stat_list in valid_input_topics[
                event_name
            ].items():

                if (
                    configs.UPDATE_LATEST_STATS
                    and valid_topic_name in event_stats.topic_stats
                ):
                    topic_stat = event_stats.topic_stats[valid_topic_name]
                else:
                    topic_stat = CPEventTopicStatistic()
                    topic_stat.stat = TopicStatisticRecord(valid_topic_name)

                for valid_topic_stat in valid_topic_stat_list:
                    topic_stat.add_stat(valid_topic_stat)

                event_stats.topic_stats[valid_topic_name] = topic_stat

            self.event_statistics[event_name] = event_stats

        print("Finished processing event statistics")
        print(f"Processed events are: {processed_tasks}")
        print("==========*****************==============")
        return processed_tasks

    def set_event_stat_min_max(self, event_name: str, minx: float, maxx: float) -> None:
        if event_name in self.event_statistics:
            self.event_statistics[event_name].minx = minx
            self.event_statistics[event_name].maxx = maxx

    def set_topic_stat_min_max(
        self, event_name: str, topic_name: str, minx: float, maxx: float
    ) -> None:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            self.event_statistics[event_name].topic_stats[topic_name].minx = minx
            self.event_statistics[event_name].topic_stats[topic_name].maxx = maxx

    def event_get_migration_cost(self, event_name: str) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].get_migration_cost()

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.get_migration_cost())
        return default_val.get_default_val()

    def event_get_cep_entire_execution_elapsed_ms_stats_avg(
        self, event_name: str
    ) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[
                event_name
            ].get_cep_entire_execution_elapsed_ms_stats_avg()

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(
                    ts.get_cep_entire_execution_elapsed_ms_stats_avg()
                )
        return default_val.get_default_val()

    def event_get_execution_duration_ms_avg(self, event_name: str) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].get_execution_duration_ms_avg()

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.get_execution_duration_ms_avg())
        return default_val.get_default_val()

    def event_get_execution_duration_ms_total(self, event_name: str) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].get_execution_duration_ms_total()

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.get_execution_duration_ms_total())
        return default_val.get_default_val()

    def event_normalized_execution_get_avg_ns_over_byte(
        self, event_name: str, activation: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[
                event_name
            ].normalized_execution_get_avg_ns_over_byte(activation)

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(
                    ts.normalized_execution_get_avg_ns_over_byte(activation)
                )
        return default_val.get_default_val()

    def event_execution_get_avg_ns_over_byte(
        self, event_name: str, activation: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].execution_get_avg_ns_over_byte(
                activation
            )

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.execution_get_avg_ns_over_byte(activation))
        return default_val.get_default_val()

    def event_normalized_get_output_cp_sat_cost(
        self, event_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].normalized_get_output_cp_sat_cost(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.normalized_get_output_cp_sat_cost(remote))
        return default_val.get_default_val()

    def event_get_output_cp_sat_cost(self, event_name: str, remote: bool) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].get_output_cp_sat_cost(remote)

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.get_output_cp_sat_cost(remote))
        return default_val.get_default_val()

    def event_normalized_output_get_avg_ns_over_byte(
        self, event_name: str, remote: bool
    ):
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[
                event_name
            ].normalized_output_get_avg_ns_over_byte(remote)

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.normalized_output_get_avg_ns_over_byte(remote))
        return default_val.get_default_val()

    def event_output_get_avg_ns_over_byte(self, event_name: str, remote: bool) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].output_get_avg_ns_over_byte(remote)

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.output_get_avg_ns_over_byte(remote))
        return default_val.get_default_val()

    def event_output_get_avg_byte_mul_ns_square(
        self, event_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].output_get_avg_byte_mul_ns_square(
                remote
            )

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.output_get_avg_byte_mul_ns_square(remote))
        return default_val.get_default_val()

    def event_output_get_avg_byte_mul_ns(self, event_name: str, remote: bool) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].output_get_avg_byte_mul_ns(remote)

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.output_get_avg_byte_mul_ns(remote))
        return default_val.get_default_val()

    def event_get_byte_per_data(self, event_name: str, remote: bool) -> float:
        if (
            event_name in self.event_statistics
            and self.event_statistics[event_name].data_exists()
        ):
            return self.event_statistics[event_name].get_byte_per_data(remote)

        default_val: TopicStatBase = TopicStatBase()
        for other_event_name, ts in self.event_statistics.items():
            if self.event_statistics[other_event_name].data_exists():
                default_val.add_value(ts.get_byte_per_data(remote))
        return default_val.get_default_val()

    def topic_get_total_byte(self, event_name: str, topic_name: str) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .get_total_byte()
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.get_total_byte())
        return default_val.get_default_val()

    def topic_normalized_get_input_cp_sat_cost(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .normalized_get_input_cp_sat_cost(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.normalized_get_input_cp_sat_cost(remote))
        return default_val.get_default_val()

    def normalize_get_expected_output_topic_load_per_second(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if configs.EXPECTED_LOAD_TYPE == 1:
            value: float = self.get_expected_output_topic_load_per_second(
                event_name, topic_name, remote
            )
            return self.normalize(self.minc, self.maxc, value)
        else:
            raise Exception("Invalid cost type detected!")

    def get_expected_output_topic_load_per_second(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if configs.EXPECTED_LOAD_TYPE == 1:
            expected_topic_load: float = self.event_get_byte_per_data(
                event_name, remote
            )
            expected_topic_count: float = self.get_expected_event_ouput_counts(
                event_name, topic_name
            )
            return expected_topic_load * expected_topic_count
        elif configs.EXPECTED_LOAD_TYPE == 2:
            return self.event_get_byte_per_data(event_name, remote)
        else:
            raise Exception("Invalid cost type detected!")

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

    def normalied_get_expected_topic_load_per_second(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if configs.EXPECTED_LOAD_TYPE == 1:
            return self.normalize(
                self.minc,
                self.maxc,
                self.get_expected_topic_load_per_second(event_name, topic_name, remote),
            )
        else:
            raise Exception("Invalid cost type detected!")

    def get_expected_topic_load_per_second(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if configs.EXPECTED_LOAD_TYPE == 1:
            expected_topic_load: float = self.topic_get_byte_per_data(
                event_name, topic_name, remote
            )
            expected_topic_count: float = self.get_expected_event_input_counts(
                event_name, topic_name
            )
            return expected_topic_load * expected_topic_count
        elif configs.EXPECTED_LOAD_TYPE == 2:
            return self.topic_get_byte_per_data(event_name, topic_name, remote)
        else:
            raise Exception("Invalid cost type detected!")

    def topic_get_byte_per_data(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .get_byte_per_data(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.get_byte_per_data(remote))
        return default_val.get_default_val()

    def topic_get_input_cp_sat_cost(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .get_input_cp_sat_cost(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.get_input_cp_sat_cost(remote))
        return default_val.get_default_val()

    def topic_normalized_get_avg_ns_over_byte(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .normalized_get_avg_ns_over_byte(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.normalized_get_avg_ns_over_byte(remote))
        return default_val.get_default_val()

    def topic_get_avg_ns_over_byte(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .get_avg_ns_over_byte(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.get_avg_ns_over_byte(remote))
        return default_val.get_default_val()

    def topic_get_avg_byte_mul_ns_square(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .get_avg_byte_mul_ns_square(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.get_avg_byte_mul_ns_square(remote))
        return default_val.get_default_val()

    def topic_get_avg_byte_mul_ns(
        self, event_name: str, topic_name: str, remote: bool
    ) -> float:
        if (
            event_name in self.event_statistics
            and topic_name in self.event_statistics[event_name].topic_stats
            and self.event_statistics[event_name].topic_stats[topic_name].data_exists()
        ):
            return (
                self.event_statistics[event_name]
                .topic_stats[topic_name]
                .get_avg_byte_mul_ns(remote)
            )

        default_val: TopicStatBase = TopicStatBase()
        for _, ts in self.event_statistics.items():
            for _, stat in ts.topic_stats.items():
                if stat.data_exists():
                    default_val.add_value(stat.get_avg_byte_mul_ns(remote))
        return default_val.get_default_val()
