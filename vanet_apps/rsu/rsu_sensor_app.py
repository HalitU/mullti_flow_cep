from pathlib import Path
from threading import Thread
from time import sleep
from typing import List
from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager
import uvicorn
from cep_library import configs
from cep_library.cep.client_registration import RegisterCEPClient
from cep_library.data.database_management_service import DatabaseManagementService
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer

from vanet_apps.cep.alarm_consumers.ambulance_alarm_consumer import AmbulanceAlarmConsumer
from vanet_apps.cep.alarm_consumers.congestion_consumer import CongestionConsumer
from vanet_apps.cep.alarm_consumers.health_alarm_consumer import HealthAlarmConsumer
from vanet_apps.cep.alarm_consumers.traffic_alarm_consumer import TrafficAlarmConsumer
from vanet_apps.cep.alarm_consumers.civilian_density_consumer import CivilianDensityConsumer
from vanet_apps.cep.alarm_consumers.fastcar_alarm_consumer import FastCarAlarmConsumer
from vanet_apps.cep.alarm_consumers.weatherapp_consumer import WeatherAppConsumer

from vanet_apps.data.base.data_sensor_factory import DataSensorFactory
from vanet_apps.data.dataconfigs.sensor_data_pattern import SensorDataParameters, SensorDataPattern

class RsuClientApp:
    def __init__(self) -> None:
        pass
    
    def run(self):
        # Statistics, required since CEP manager will try to write the statistics to these locations
        Path("stats").mkdir(parents=True, exist_ok=True)

        @asynccontextmanager
        async def lifespan(app:FastAPI):
            print ("starting up the web server...")
            yield
            print ("Shutting down")
            
        # https://github.com/tiangolo/fastapi
        app = FastAPI(lifespan=lifespan)
    
        print("Running the client with parameters: ")
        print("env host name: ", configs.env_host_name)
        print("env host ip: ", configs.env_host_ip)
        print("env host port: ", configs.env_host_port)
        print("env mongo host: ", configs.env_mongo_host)
        print("env mongo port: ", configs.env_mongo_port)
        print("env self mongo name: ", configs.env_mongo_self)

        ##############################################################
        # Need a global class for managing CEP flow
        # as well as consumer registration, topic creation etc.
        # CEP management Example
        # tasks = RegisterTasks(app)
        db = DatabaseManagementService(configs.env_host_name, groupid="shared_db_instance")
        rcc = RegisterCEPClient(app, db)
        sensors = []
        event_consumers = []
        
        Thread(daemon=True, target=self.initalize_sensors, args=(sensors, event_consumers, db, app, rcc)).start()

        print(f"Initialized sensors with {len(sensors)} sensors and {len(event_consumers)} consumers.")

        # Starting the server
        uvicorn.run(app, host=configs.env_host_ip, port=configs.env_host_port, log_level="warning")

    def initalize_sensors(self, sensors:List, event_consumers:List, db:DatabaseManagementService, app:FastAPI,
                          rcc:RegisterCEPClient) -> None:
        # Get the sensor data pattern configs
        sdp = SensorDataPattern()
        data_patterns = sdp.read_json(str(configs.data_pattern_json_file))
        
        # Register raw data producing sensors
        if configs.raw_producer_types and configs.raw_producer_types is not None:
            rpf = DataSensorFactory(db, app)
            for _, config_type in enumerate(configs.raw_producer_types):
                print(f"Preparing sensor of type: {config_type}")

                # Skip if invalid raw data producer type
                if config_type not in data_patterns:
                    continue

                data_pattern = data_patterns[config_type]
                sdp = SensorDataParameters(data_pattern, config_type)
                    
                print(f"[Client] Creating a sensor with params: raw_output_production_duration: {sdp.get_default_duration()}, raw_per_moment: {sdp.get_default_count_per_delay()}, raw_output_delay_ms: {sdp.get_default_delay_s()}")

                cep_sensor_manager:RawDataProducer = rpf.get_sensor(sdp)
                cep_raw_settings: RawSettings = cep_sensor_manager.get_settings()
                
                sensor_producer = rpf.get_sensor_producer(
                    sdp,
                    cep_sensor_manager, 
                    app, 
                    cep_raw_settings)

                sensors.append(sensor_producer)
                
                print("[Client] Sensor created.")
            
        if configs.event_consumer_types and configs.event_consumer_types is not None:
            for _, config_type in enumerate(configs.event_consumer_types):
                print(f"[{configs.env_host_name}] Registering consumer for {config_type} alarm.")
                if config_type == "AmbulanceAlarmConsumer":
                    aac = AmbulanceAlarmConsumer(app=app, db=db)
                    aac.run()
                elif config_type == "HealthAlarmConsumer":
                    aac = HealthAlarmConsumer(app=app, db=db)
                    aac.run()
                elif config_type == "TrafficAlarmConsumer":
                    aac = TrafficAlarmConsumer(app=app, db=db)
                    aac.run()
                elif config_type == "CivilianDensityConsumer":
                    aac = CivilianDensityConsumer(app=app, db=db)
                    aac.run()
                elif config_type == "FastCarAlarmConsumer":
                    aac = FastCarAlarmConsumer(app=app, db=db)
                    aac.run()
                elif config_type == "WeatherAppConsumer":
                    aac = WeatherAppConsumer(app=app, db=db)
                    aac.run()
                elif config_type == "CongestionConsumer":
                    aac = CongestionConsumer(app=app, db=db)
                    aac.run()
                else:
                    raise Exception("Invalid configuration type")
                
                event_consumers.append(aac)

        while True:
            all_initialized = True
            for ec in event_consumers:
                all_initialized = all_initialized and ec.cec.initialized
            if all_initialized:
                break
            sleep(1.0)
            
        print(f"Initialized sensors with {len(sensors)} sensors and {len(event_consumers)} consumers.")
        rcc.client_management.cepsm.initialized = True
