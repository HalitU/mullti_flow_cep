import sys

from cep_library import configs
from cep_library.stats_helper.mean_stat_base import MeanStatBase


class TopicStatBase:
    def __init__(self) -> None:
        self.min = sys.maxsize
        self.max = 0.0
        self.mean: MeanStatBase = MeanStatBase()
        self.exists: bool = False

    def add_value(self, val: float) -> None:
        self.mean.add(val)
        self.min = min(self.min, val)
        self.max = max(self.max, val)
        self.exists = True

    def get_default_val(self) -> float:
        if not self.exists:
            return configs.STAT_DEFAULT_VAR

        if self.exists and self.max == 0:
            raise Exception("Stat value cannot be 0 here!")

        if configs.DEFAULT_STAT_PREFERENCE == 0:
            return self.min
        elif configs.DEFAULT_STAT_PREFERENCE == 1:
            return self.mean.mean
        elif configs.DEFAULT_STAT_PREFERENCE == 2:
            return self.max
        else:
            raise Exception("Invalid preference config detected!")
