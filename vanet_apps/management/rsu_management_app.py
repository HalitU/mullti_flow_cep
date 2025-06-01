# Required from library
from pathlib import Path
from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager
import uvicorn
from cep_library.management.server.management_server import ManagementServer
from cep_library import configs

# custom apps
from vanet_apps.cep.smart_traffic_scenario.smart_traffic_flow import SmartTrafficFlow
from vanet_apps.cep.weather_quality_scenario.weather_quality_flow import WeatherQualityFlow
from vanet_apps.management.trigger_alarm_experiment import TriggerCEPExperiment

class RsuManagementApp:
    def __init__(self) -> None:
        pass
    
    def run(self):
        # Statistics
        Path("stats").mkdir(parents=True, exist_ok=True)

        @asynccontextmanager
        async def lifespan(app:FastAPI):
            print ("starting up the web server...")
            yield
            print ("Shutting down")
            
        # https://github.com/tiangolo/fastapi
        app = FastAPI(lifespan=lifespan)

        ##############################################################
        # Need a global class for managing CEP flow
        # as well as consumer registration, topic creation etc.
        # CEP management Example
        server_management = ManagementServer(app)

        # VANET scenario
        if configs.server_query_type == 7:
            print("Registering vehicle & pollution flows...")
            first_flow_tasks = SmartTrafficFlow()
            first_flow_tasks.register_tasks(server_management)
            second_flow_tasks = WeatherQualityFlow()
            second_flow_tasks.register_tasks(server_management)
            print("Registered vehicle & pollution flows...")
        else:
            raise Exception("Invalid scenario selected!")

        # Register the test helper endpoint
        cep_exp = TriggerCEPExperiment()
        cep_exp.register_test_helper(app, server_management)

        # After the device is awake and connects to raw data producer nodes, 
        # experiment endpoint can be called to trigger the simulations.
        uvicorn.run(app, host=configs.env_server_name, port=configs.env_server_port, log_level="warning")        
