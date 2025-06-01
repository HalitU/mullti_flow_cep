import json
from typing import Any
from cep_library import configs

class SensorDataPattern:
    def __init__(self) -> None:
        pass
    
    def read_json(self, data_pattern_file:str):
        with open(data_pattern_file) as json_file:
            return json.load(json_file)


class SensorDataParameters:
    def __init__(self, data_pattern:dict, raw_type:str) -> None:
        self.raw_type:str = raw_type
        self.topic_name:str = data_pattern["output_topic"]
        self.data_name:str = data_pattern["data_name"]
        # self.default_duration:float = float(data_pattern["durationS"])
        self.default_duration:float = configs.simulation_duration # int(data_pattern["durationS"]) #configs.simulation_duration
        self.default_delay_s:float = float(data_pattern["delayS"])
        self.msg_per_delay:int =  int(data_pattern["count_per_delay"])
        self.alarming_data_time_intervals:dict[str, dict[str, Any]] = data_pattern["alarming_data_time_intervals"]

    def get_topic(self) -> str:
        return self.topic_name
    
    def get_data_name(self) -> str:
        return self.data_name

    def get_default_duration(self) -> float:
        return self.default_duration
    
    def get_default_delay_s(self) -> float:
        return self.default_delay_s
    
    def get_default_count_per_delay(self) -> int:
        return self.msg_per_delay

    def get_current_duration(self, time:float):
        pass
        
    def get_current_delay(self, time:float):
        pass
