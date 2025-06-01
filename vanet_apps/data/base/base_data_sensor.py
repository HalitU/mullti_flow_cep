from datetime import datetime, timezone
from typing import List, Tuple
from fastapi import FastAPI, Response
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.stats_helper.mean_stat_base import MeanStatBase
from cep_library.stats_helper.stats_file_helper import StatsFileHelper
from vanet_apps.data.dataconfigs.sensor_data_pattern import SensorDataParameters

class RawProductionStats:
    def __init__(self) -> None:
        self.production_counter_stat:MeanStatBase = MeanStatBase()
    
    def generate_row(self, record_date:str, host_name:str, raw_name:str, produced_count:str) -> List[str]:
        return [record_date, host_name, raw_name, produced_count]
    
    def get_headers(self):
        return [
            "row_id",
            "record_date",
            "host_name",
            "raw_name",
            "produced_count"
        ]

class BaseDataSensor:
    def __init__(self, 
                 app:FastAPI, 
                 settings:RawSettings,
                 sdp: SensorDataParameters) -> None:
        print(f"[BaseDataSensor] Initializing with params: {sdp.msg_per_delay}")
        self.killed = False
        self.sdp: SensorDataParameters = sdp
        self.settings: RawSettings = settings
        
        self.rps = RawProductionStats()
        self.raw_production_stats = StatsFileHelper(
            app=app, 
            header_arr=self.rps.get_headers(),
            endpoint_prefix="production_stats",
            endpoint_name=settings.raw_data_name)

        @app.get("/stop_sampling/"+settings.raw_data_name+"/")
        def _() -> Response:
            self.stop()
            return Response(status_code=200)
        
        @app.get("/start_sampling/"+settings.raw_data_name+"/")
        def _():
            self.killed = False
            self.run()
            return Response(status_code=200)        
                
        @app.get("/test_sampling/"+settings.raw_data_name+"/")
        def _(required_len:int):
            self.killed = False
            self.run()
            return Response(status_code=200)                

    def run(self):
        raise Exception("Not implemented inherit and implement the method.")

    def stop(self) -> None:
        self.killed = True
    
    def get_next_step_production(self, time:float) -> Tuple[int, float, bool, bool]:
        # Stop after last time passes
        if self.sdp.default_duration <= time:
            return 0, 0, False, True
        
        for _, window in self.sdp.alarming_data_time_intervals.items():
            count_per_delay:int = window["count_per_delay"]
            delayS:float = window["delayS"]
            alarming:bool = window["alarming"]
            start:float = window["interval"][0]
            end:float = window["interval"][1]
            
            # if count_per_delay == 0 or delayS == 0 or alarming == False or end == 0:
            if count_per_delay == 0 or delayS == 0 or end == 0:
                raise Exception("Invalid raw data production arguments detected!")
            
            # A window matched
            if start <= time and time <= end:
                return count_per_delay, delayS, alarming, False
        
        # No special window matched, use default production settings
        return self.sdp.msg_per_delay, self.sdp.default_delay_s, False, False

    def print_raw_output_stats(self):
        self.rps.production_counter_stat.add(1)
        crr_rps = RawProductionStats()
        crr_production_stat_row:List[List[str]] = [crr_rps.generate_row(
            str(datetime.now(tz = timezone.utc)),
            str(self.settings.producer_name), 
            str(self.settings.raw_data_name), 
            str(self.rps.production_counter_stat.count))]
        
        self.raw_production_stats.print_device_stats(crr_production_stat_row)
