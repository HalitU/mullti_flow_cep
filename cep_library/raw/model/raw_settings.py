import datetime
import math
from cep_library import configs
from cep_library.cep.model.cep_task import RequiredOutputTopics
from cep_library.stats_helper.mean_stat_base import MeanStatBase


class RawSettings:
    def __init__(
        self,
        raw_data_name: str,
        producer_name: str,
        producer_port: int,
        consumer_group: str,
        output_topic: RequiredOutputTopics,
        host_x: float,
        host_y: float,
    ) -> None:
        self.raw_data_name: str = raw_data_name
        self.producer_name: str = producer_name
        self.producer_port: int = producer_port
        self.host_x: float = host_x
        self.host_y: float = host_y
        self.consumer_group: str = consumer_group
        self.output_topic: RequiredOutputTopics = output_topic


class RawStatisticsBook:
    def __init__(self, producer_name: str, raw_name: str, topic_name: str) -> None:
        self.producer_name: str = producer_name
        self.raw_name: str = raw_name
        self.topic_name: str = topic_name

        self.local_write_time_ns_total: MeanStatBase = MeanStatBase()
        self.local_write_size_byte_total: MeanStatBase = MeanStatBase()
        self.local_write_data_count: MeanStatBase = MeanStatBase()

        self.remote_write_time_ns_total: MeanStatBase = MeanStatBase()
        self.remote_write_time_byte_total: MeanStatBase = MeanStatBase()
        self.remote_write_data_count: MeanStatBase = MeanStatBase()

        self.init_sent_time: datetime.datetime = None
        self.last_sent_time: datetime.datetime = None

        self.desired_cost_lower: float = configs.STAT_NORMALIZER_LOW
        self.desired_cost_upper: float = configs.STAT_NORMALIZER_HIGH
        self.minx: float
        self.maxx: float

    def update_with_stats(
        self,
        local_write_time_ns_total: MeanStatBase,
        local_write_size_byte_total: MeanStatBase,
        local_write_data_count: MeanStatBase,
        remote_write_time_ns_total: MeanStatBase,
        remote_write_time_byte_total: MeanStatBase,
        remote_write_data_count: MeanStatBase,
    ):
        self.local_write_time_ns_total.add_multiple(
            local_write_time_ns_total.mean, local_write_time_ns_total.count
        )
        self.local_write_size_byte_total.add_multiple(
            local_write_size_byte_total.mean, local_write_size_byte_total.count
        )
        self.local_write_data_count.add_multiple(
            local_write_data_count.mean, local_write_data_count.count
        )
        self.remote_write_time_ns_total.add_multiple(
            remote_write_time_ns_total.mean, remote_write_time_ns_total.count
        )
        self.remote_write_time_byte_total.add_multiple(
            remote_write_time_byte_total.mean, remote_write_time_byte_total.count
        )
        self.remote_write_data_count.add_multiple(
            remote_write_data_count.mean, remote_write_data_count.count
        )

    def normalize(
        self, actual_lower: float, actual_upper: float, value: float
    ) -> float:
        if configs.NORMALIZATION_APPROACH == 0:
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

    def get_calculated(self) -> float:
        return (
            self.local_write_time_ns_total.count + self.remote_write_time_ns_total.count
        ) > 0

    def normalized_get_avg_byte_mul_ns(self, remote: bool):
        return self.normalize(self.minx, self.maxx, self.get_avg_byte_mul_ns(remote))

    def get_avg_byte_mul_ns(self, remote: bool) -> float:
        if remote:
            if self.remote_write_time_ns_total.count == 0:
                return (
                    (self.local_write_time_ns_total.mean / 1000.0)
                    * self.local_write_size_byte_total.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.remote_write_time_ns_total.mean / 1000.0
            ) * self.remote_write_time_byte_total.mean
        else:
            if self.local_write_time_ns_total.count == 0:
                return (
                    (self.remote_write_time_ns_total.mean / 1000.0)
                    * self.remote_write_time_byte_total.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.local_write_time_ns_total.mean / 1000.0
            ) * self.local_write_size_byte_total.mean

    def normalized_get_avg_byte_mul_ns_square(self, remote: bool):
        return self.normalize(
            self.minx, self.maxx, self.get_avg_byte_mul_ns_square(remote)
        )

    def get_total_event_count(self) -> float:
        return (
            self.local_write_data_count.total() + self.remote_write_data_count.total()
        )

    def get_per_second_expected_event_count(self) -> float:
        secs: float = (self.last_sent_time - self.init_sent_time).total_seconds()

        if secs < 1.0:
            secs = 1.0
        return (
            (self.local_write_data_count.total() + self.remote_write_data_count.total())
            * 1.0
        ) / secs

    def get_avg_byte_mul_ns_square(self, remote: bool) -> float:
        if remote:
            if self.remote_write_time_ns_total.count == 0:
                return (
                    (self.local_write_time_ns_total.mean / 1000.0) ** 2
                    * self.local_write_size_byte_total.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.remote_write_time_ns_total.mean / 1000.0
            ) ** 2 * self.remote_write_time_byte_total.mean
        else:
            if self.local_write_time_ns_total.count == 0:
                return (
                    (self.remote_write_time_ns_total.mean / 1000.0) ** 2
                    * self.remote_write_time_byte_total.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.local_write_time_ns_total.mean / 1000.0
            ) ** 2 * self.local_write_size_byte_total.mean

    def normalized_get_avg_byte_over_n(self, remote: bool):
        return self.normalize(self.minx, self.maxx, self.get_avg_byte_over_n(remote))

    def get_avg_byte_over_n(self, remote: bool) -> float:
        if remote:
            if self.remote_write_time_ns_total.count == 0:
                return (
                    self.local_write_size_byte_total.mean
                    * configs.STATISTICS_REMOTE_MULTIPLIER
                )
            return self.remote_write_time_byte_total.mean
        else:
            if self.local_write_time_ns_total.count == 0:
                return (
                    self.remote_write_time_byte_total.mean
                    / configs.STATISTICS_REMOTE_MULTIPLIER
                )
            return self.local_write_size_byte_total.mean

    def normalized_get_avg_byte_over_ns(self, remote: bool):
        return self.normalize(self.minx, self.maxx, self.get_avg_byte_over_ns(remote))

    def get_avg_byte_over_ns(self, remote: bool) -> float:
        if remote:
            if self.remote_write_time_ns_total.count == 0:
                return (
                    self.local_write_size_byte_total.mean
                    / self.local_write_time_ns_total.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER

            return (
                self.remote_write_time_byte_total.mean
                / self.remote_write_time_ns_total.mean
            )
        else:
            if self.local_write_time_ns_total.count == 0:
                return (
                    self.remote_write_time_byte_total.mean
                    / self.remote_write_time_ns_total.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.local_write_size_byte_total.mean
                / self.local_write_time_ns_total.mean
            )

    def normalized_get_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_USE_NORMALIZED_COST == False:
            return self.get_cp_sat_cost(remote)

        return self.normalize(self.minx, self.maxx, self.get_cp_sat_cost(remote))

    def get_cp_sat_cost(self, remote: bool) -> float:
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
            size = (
                1024.0
                * (
                    self.local_write_size_byte_total.total()
                    + self.remote_write_time_byte_total.total()
                )
                / (
                    self.local_write_data_count.count
                    + self.remote_write_data_count.count
                )
            )
        elif configs.DATA_LOAD_TYPE == 3:
            size = (
                1024.0
                * (
                    self.local_write_size_byte_total.total()
                    + self.remote_write_time_byte_total.total()
                )
                / configs.evaluation_period
            )

        if remote:
            return size * configs.CP_SAT_STATISTICS_REMOTE_MULTIPLIER
        else:
            return size

    def normalized_get_avg_ns_over_byte(self, remote: bool):
        return self.normalize(self.minx, self.maxx, self.get_avg_ns_over_byte(remote))

    def get_avg_ns_over_byte(self, remote: bool) -> float:
        if remote:
            if self.remote_write_time_ns_total.count == 0:
                return (
                    self.local_write_time_ns_total.mean
                    / self.local_write_size_byte_total.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.remote_write_time_ns_total.mean
                / self.remote_write_time_byte_total.mean
            )
        else:
            if self.local_write_time_ns_total.count == 0:
                return (
                    self.remote_write_time_ns_total.mean
                    / self.remote_write_time_byte_total.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            return (
                self.local_write_time_ns_total.mean
                / self.local_write_size_byte_total.mean
            )

    def normalized_get_avg_ns_over_n(self, remote: bool):
        return self.normalize(self.minx, self.maxx, self.get_avg_ns_over_n(remote))

    def get_avg_ns_over_n(self, remote: bool) -> float:
        if remote:
            if self.remote_write_time_ns_total.count == 0:
                return (
                    self.local_write_time_ns_total.mean
                    * configs.STATISTICS_REMOTE_MULTIPLIER
                )
            return self.remote_write_time_ns_total.mean
        else:
            if self.local_write_time_ns_total.count == 0:
                return (
                    self.remote_write_time_ns_total.mean
                    / configs.STATISTICS_REMOTE_MULTIPLIER
                )
            return self.local_write_time_ns_total.mean

    def normalized_get_write_size_byte_total(self):
        return self.normalize(self.minx, self.maxx, self.get_write_size_byte_total())

    def get_write_size_byte_total(self) -> float:
        return (
            self.remote_write_time_byte_total.mean
            + self.local_write_size_byte_total.mean
        )

    def get_remote_write_time_byte_mean(self) -> float:
        return self.remote_write_time_byte_total.mean

    def normalized_get_write_time_ns_total(self):
        return self.normalize(self.minx, self.maxx, self.get_write_time_ns_total())

    def get_write_time_ns_total(self) -> float:
        return (
            self.remote_write_time_ns_total.mean + self.local_write_time_ns_total.mean
        )

    def add_raw_statistic(
        self,
        local_write_time_ns: float,
        local_write_size_byte: float,
        local_write_data_count: float,
        remote_write_time_ns: float,
        remote_write_size_byte: float,
        remote_write_data_count: float,
    ) -> None:

        if self.init_sent_time is None:
            self.init_sent_time = datetime.datetime.now(datetime.timezone.utc)
        self.last_sent_time = datetime.datetime.now(datetime.timezone.utc)

        if local_write_data_count > 0:
            self.local_write_time_ns_total.add(local_write_time_ns)
            self.local_write_size_byte_total.add(local_write_size_byte)
            self.local_write_data_count.add(local_write_data_count)

        if remote_write_data_count > 0:
            self.remote_write_time_ns_total.add(remote_write_time_ns)
            self.remote_write_time_byte_total.add(remote_write_size_byte)
            self.remote_write_data_count.add(remote_write_data_count)

    def reset_stats(self) -> None:
        self.local_write_size_byte_total.reset()
        self.local_write_time_ns_total.reset()
        self.local_write_data_count.reset()

        self.remote_write_time_byte_total.reset()
        self.remote_write_time_ns_total.reset()
        self.remote_write_data_count.reset()

        self.init_sent_time = None
        self.last_sent_time = None
