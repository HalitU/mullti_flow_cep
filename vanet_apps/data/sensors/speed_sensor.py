import time

from fastapi import FastAPI
from cep_library import configs
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer
import numpy as np

from vanet_apps.data.base.base_data_sensor import BaseDataSensor
from vanet_apps.data.dataconfigs.sensor_data_pattern import SensorDataParameters

class SpeedSensor(BaseDataSensor):
    def __init__(self, 
                 sdp:SensorDataParameters,
                 producer:RawDataProducer, 
                 app:FastAPI, 
                 settings:RawSettings
                 ) -> None:
        print(f"[CarSpeedDataSensor] Initializing with params: {sdp.msg_per_delay}, {sdp.default_delay_s}")
        super().__init__(app, settings, sdp)
        self.producer = producer

    def run(self):    
        print(f"Starting {self.settings.raw_data_name} sampling")
        msg_count = 0
        speed_min = 10.0 
        speed_max = 130.0
        
        crr_time:float = 0.0
        count_per_delay:int
        delayS:float
        alarming:bool
        stop:bool
        count_per_delay, delayS, alarming, stop = self.get_next_step_production(crr_time)
        
        while not stop:
            count_per_delay = count_per_delay * configs.MULTIPLY_DATA_PRODUCTION_COUNT
            
            # Do this M times for this step
            for _ in range(count_per_delay):
                if self.killed: 
                    print(f"Stopping {self.settings.raw_data_name} sampling") 
                    return True        
                
                if alarming:
                    pass                
                
                data = dict()
                data[self.settings.raw_data_name] = [(speed_min - speed_max) * np.random.random_sample() + speed_max]
                if configs.scenario_case == 2 and configs.server_query_type == 8:
                    data["load"] = bytearray(1000)
                elif configs.scenario_case == 2 and configs.server_query_type == 9:
                    data["load"] = bytearray(250)
                
                if not alarming:
                    data["load"] = bytearray(4 * configs.MULTIPLY_DATA_SIZE_ALARMING)
                else:
                    data["load"] = bytearray(1 * configs.MULTIPLY_DATA_SIZE_NORMAL)
                
                self.producer.send(data, raw_data_tracker="speed_data")
                
                msg_count += 1
                crr_sleep_duration = (delayS * 1.0) / count_per_delay
                
                self.print_raw_output_stats()
                
                time.sleep(crr_sleep_duration)
            
            # Next step
            crr_time += delayS
            count_per_delay, delayS, alarming, stop = self.get_next_step_production(crr_time)               
            
        print(f"Stopping {self.settings.raw_data_name} sampling")
        return msg_count
