import math
import sys
from typing import List
from cep_library import configs
from cep_library.cep.model.cep_task import CEPTask
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.statistics.consumer_server_statics import (
    ConsumerServerStatics,
)
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import (
    ManagementServerStatisticsAnalyzer,
)
from cep_library.raw.model.raw_settings import RawSettings

n_equal_min_max = 0


class CPSATV3CostAdapter:
    def __init__(self) -> None:
        self.evaluation_period_s = configs.evaluation_period
        self.desired_cost_lower: float = configs.STAT_NORMALIZER_LOW
        self.desired_cost_upper: float = configs.STAT_NORMALIZER_HIGH

    def adaptCosts(
        self,
        producer_info: dict[str, dict[str, RawSettings]],
        producer_stats: RawServerStatAnalyzer,
        cep_tasks: List[CEPTask],
        mssa: ManagementServerStatisticsAnalyzer,
        consumer_info: dict[str, dict[str, CEPConsumerSettings]],
        consumer_stats: ConsumerServerStatics,
    ):
        global n_equal_min_max

        min_cost: float = sys.maxsize
        max_cost: float = 0.0

        raw_s: RawSettings
        for _, raw_host_settings in producer_info.items():
            for _, raw_s in raw_host_settings.items():
                load_cost: float = producer_stats.get_expected_load_per_second(
                    raw_s.producer_name, raw_s.output_topic.output_topic, remote=False
                )

                min_cost = min(min_cost, load_cost)
                max_cost = max(max_cost, load_cost)

        for task in cep_tasks:

            load_cost: float = mssa.get_expected_output_topic_load_per_second(
                task.settings.action_name,
                task.settings.output_topic.output_topic,
                False,
            )
            min_cost = min(min_cost, load_cost)
            max_cost = max(max_cost, load_cost)

            for rst in task.settings.required_sub_tasks:
                load_cost: float = mssa.get_expected_topic_load_per_second(
                    task.settings.action_name, rst.input_topic, False
                )
                min_cost = min(min_cost, load_cost)
                max_cost = max(max_cost, load_cost)

        for consumer_host, consumer_topics in consumer_info.items():
            for topic_name, topic_settings in consumer_topics.items():
                load_cost: float = consumer_stats.get_expected_load_per_second(
                    consumer_host, topic_name, remote=False
                )

                min_cost = min(min_cost, load_cost)
                max_cost = max(max_cost, load_cost)

        min_cost = min(min_cost, configs.CUMULATIVE_LOAD_LIMIT)
        max_cost = max(max_cost, configs.CUMULATIVE_LOAD_LIMIT)

        if min_cost == max_cost:
            max_cost = min_cost * 1.25
            n_equal_min_max += 1
            print(f"Number of invalid min max: {n_equal_min_max}")

        mssa.minc = min_cost
        mssa.maxc = max_cost

        consumer_stats.minc = min_cost
        consumer_stats.maxc = max_cost

        producer_stats.minc = min_cost
        producer_stats.maxc = max_cost

        for _, raw_host_settings in producer_info.items():
            for _, raw_s in raw_host_settings.items():
                producer_stats.set_stat_min_max(
                    raw_s.producer_name,
                    raw_s.output_topic.output_topic,
                    min_cost,
                    max_cost,
                )

        for task in cep_tasks:
            mssa.set_event_stat_min_max(task.settings.action_name, min_cost, max_cost)

            for rts in task.settings.required_sub_tasks:
                mssa.set_topic_stat_min_max(
                    task.settings.action_name, rts.input_topic, min_cost, max_cost
                )

        for consumer_host, consumer_topics in consumer_info.items():
            for topic_name, _ in consumer_topics.items():
                consumer_stats.set_stat_min_max(
                    consumer_host, topic_name, min_cost, max_cost
                )

        print("-----------------------------------------------------------")
        print(
            f"[SERVER] Minimum and maximum costs were: {min_cost}-{max_cost}, normalized between: {self.desired_cost_lower}-{self.desired_cost_upper}"
        )
        print(
            f"[SERVER] Min is normalized to: {self.normalize(min_cost, max_cost, min_cost)}"
        )
        print(
            f"[SERVER] Max is normalized to: {self.normalize(min_cost, max_cost, max_cost)}"
        )
        print("-----------------------------------------------------------")

    def normalize(
        self, actual_lower: float, actual_upper: float, value: float
    ) -> float:
        if configs.NORMALIZATION_APPROACH == 0:
            print("Normalizing using min-max approach")
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
