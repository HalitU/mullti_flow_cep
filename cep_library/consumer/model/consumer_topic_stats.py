import math
from cep_library import configs
from cep_library.stats_helper.mean_stat_base import MeanStatBase


class ConsumerTopicStats:
    def __init__(self, host_name: str, topic_name: str) -> None:
        self.remote_reading_byte: MeanStatBase = MeanStatBase()
        self.remote_reading_ns: MeanStatBase = MeanStatBase()
        self.remote_read_data_count: MeanStatBase = MeanStatBase()

        self.local_reading_byte: MeanStatBase = MeanStatBase()
        self.local_reading_ns: MeanStatBase = MeanStatBase()
        self.local_read_data_count: MeanStatBase = MeanStatBase()

        self.source_hit_count: dict[str, MeanStatBase] = {}
        self.source_miss_count: dict[str, MeanStatBase] = {}

        self.hit_count: MeanStatBase = MeanStatBase()
        self.miss_count: MeanStatBase = MeanStatBase()
        self.miss_read_count: MeanStatBase = MeanStatBase()

        self.host_name: str = host_name
        self.topic_name: str = topic_name

        self.desired_cost_lower: float = configs.STAT_NORMALIZER_LOW
        self.desired_cost_upper: float = configs.STAT_NORMALIZER_HIGH
        self.minx: float
        self.maxx: float

    def update_with_stats(
        self,
        remote_reading_byte: MeanStatBase,
        remote_reading_ns: MeanStatBase,
        remote_read_data_count: MeanStatBase,
        local_reading_byte: MeanStatBase,
        local_reading_ns: MeanStatBase,
        local_read_data_count: MeanStatBase,
    ):
        self.remote_reading_byte.add_multiple(
            remote_reading_byte.mean, remote_reading_byte.count
        )
        self.remote_reading_ns.add_multiple(
            remote_reading_ns.mean, remote_reading_ns.count
        )
        self.remote_read_data_count.add_multiple(
            remote_read_data_count.mean, remote_read_data_count.count
        )
        self.local_reading_byte.add_multiple(
            local_reading_byte.mean, local_reading_byte.count
        )
        self.local_reading_ns.add_multiple(
            local_reading_ns.mean, local_reading_ns.count
        )
        self.local_read_data_count.add_multiple(
            local_read_data_count.mean, local_read_data_count.count
        )

    def calculated(self) -> bool:
        return (self.remote_reading_byte.count + self.remote_reading_ns.count) > 0 or (
            self.local_reading_byte.count + self.local_reading_ns.count
        ) > 0

    def reset_stats(self) -> None:
        self.remote_reading_byte.reset()
        self.remote_reading_ns.reset()
        self.remote_read_data_count.reset()

        self.local_reading_byte.reset()
        self.local_reading_ns.reset()
        self.local_read_data_count.reset()

    def normalize(self, actual_lower, actual_upper, value) -> float:
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

    def normalized_get_reading_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_USE_NORMALIZED_COST == False:
            return self.get_reading_cp_sat_cost(remote)

        return self.normalize(
            self.minx, self.maxx, self.get_reading_cp_sat_cost(remote)
        )

    def get_reading_cp_sat_cost(self, remote: bool) -> float:
        if configs.CP_SAT_COST_TYPE == 0:
            return self.normalize_get_reading_ns_over_byte(remote)
        elif configs.CP_SAT_COST_TYPE == 1:
            return self.get_reading_ns_over_byte(remote)
        elif configs.CP_SAT_COST_TYPE == 2:
            return self.get_byte_per_data(remote)
        elif configs.CP_SAT_COST_TYPE == 3:
            return self.get_reading_byte_mul_ns_square(remote)
        elif configs.CP_SAT_COST_TYPE == 4:
            return self.get_reading_byte_mul_ns(remote) * 1000.0 * 1024.0
        else:
            raise Exception("Invalid cost type!")

    def get_byte_per_data(self, remote: bool) -> float:
        if configs.DATA_LOAD_TYPE == 2:
            return (
                1024.0
                * (self.local_reading_byte.total() + self.remote_reading_byte.total())
                / (self.local_read_data_count.count + self.remote_read_data_count.count)
            )

        if not configs.USE_CUMULATIVE_LOAD:
            size = (
                1024.0
                * (self.local_reading_byte.total() + self.remote_reading_byte.total())
                / (self.local_read_data_count.count + self.remote_read_data_count.count)
            )

            if remote:
                return size * configs.CP_SAT_STATISTICS_REMOTE_MULTIPLIER
            else:
                return size
        else:
            return (
                1024.0
                * (self.local_reading_byte.total() + self.remote_reading_byte.total())
                / configs.evaluation_period
            )

    def normalize_get_reading_ns_over_byte(self, remote: bool):
        return self.normalize(
            self.minx, self.maxx, self.get_reading_ns_over_byte(remote)
        )

    def get_reading_ns_over_byte(self, remote: bool) -> float:
        if remote:
            if self.remote_reading_byte.count == 0:
                return (
                    self.local_reading_ns.mean / self.local_reading_byte.mean
                ) * configs.STATISTICS_REMOTE_MULTIPLIER
            else:
                return self.remote_reading_ns.mean / self.remote_reading_byte.mean
        else:
            if self.local_reading_byte.count == 0:
                return (
                    self.remote_reading_ns.mean / self.remote_reading_byte.mean
                ) / configs.STATISTICS_REMOTE_MULTIPLIER
            else:
                return self.local_reading_ns.mean / self.local_reading_byte.mean

    def normalize_get_reading_byte_mul_ns(self, remote: bool):
        return self.normalize(
            self.minx, self.maxx, self.get_reading_byte_mul_ns(remote)
        )

    def get_reading_byte_mul_ns(self, remote: bool) -> float:
        if not configs.USE_CUMULATIVE_COST:
            if remote:
                if self.remote_reading_byte.count == 0:
                    return (
                        (self.local_reading_ns.mean / 1000.0)
                        * self.local_reading_byte.mean
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.remote_reading_ns.mean / 1000.0
                    ) * self.remote_reading_byte.mean
            else:
                if self.local_reading_byte.count == 0:
                    return (
                        (self.remote_reading_ns.mean / 1000.0)
                        * self.remote_reading_byte.mean
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.local_reading_ns.mean / 1000.0
                    ) * self.local_reading_byte.mean
        else:
            if remote:
                if self.remote_reading_byte.count == 0:
                    return (
                        (self.local_reading_ns.total() / 1000.0)
                        * self.local_reading_byte.total()
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.remote_reading_ns.total() / 1000.0
                    ) * self.remote_reading_byte.total()
            else:
                if self.local_reading_byte.count == 0:
                    return (
                        (self.remote_reading_ns.total() / 1000.0)
                        * self.remote_reading_byte.total()
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.local_reading_ns.total() / 1000.0
                    ) * self.local_reading_byte.total()

    def normalize_get_reading_byte_mul_ns_square(self, remote: bool):
        return self.normalize(
            self.minx, self.maxx, self.get_reading_byte_mul_ns_square(remote)
        )

    def get_reading_byte_mul_ns_square(self, remote: bool) -> float:
        if not configs.USE_CUMULATIVE_COST:
            if remote:
                if self.remote_reading_byte.count == 0:
                    return (
                        (self.local_reading_ns.mean / 1000.0) ** 2
                        * self.local_reading_byte.mean
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.remote_reading_ns.mean / 1000.0
                    ) ** 2 * self.remote_reading_byte.mean
            else:
                if self.local_reading_byte.count == 0:
                    return (
                        (self.remote_reading_ns.mean / 1000.0) ** 2
                        * self.remote_reading_byte.mean
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.local_reading_ns.mean / 1000.0
                    ) ** 2 * self.local_reading_byte.mean
        else:
            if remote:
                if self.remote_reading_byte.count == 0:
                    return (
                        (self.local_reading_ns.total() / 1000.0) ** 2
                        * self.local_reading_byte.total()
                    ) * configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.remote_reading_ns.total() / 1000.0
                    ) ** 2 * self.remote_reading_byte.total()
            else:
                if self.local_reading_byte.count == 0:
                    return (
                        (self.remote_reading_ns.total() / 1000.0) ** 2
                        * self.remote_reading_byte.total()
                    ) / configs.STATISTICS_REMOTE_MULTIPLIER
                else:
                    return (
                        self.local_reading_ns.total() / 1000.0
                    ) ** 2 * self.local_reading_byte.total()
