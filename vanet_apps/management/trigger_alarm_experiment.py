import sys
from fastapi import FastAPI
from fastapi.responses import FileResponse
import requests
import os
from pathlib import Path
from typing import Callable
import cep_library.configs as configs
from threading import Lock, Thread
import shutil
import time

# From cep lib
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.server.management_server import ManagementServer
from cep_library.raw.model.raw_settings import RawSettings

class TriggerCEPExperiment:
    def __init__(self) -> None:
        pass
    
    # A helper for registering test experiment trigger
    def register_test_helper(self, app:FastAPI, server_management:ManagementServer) -> None:
        @app.post("/start_simulation/")
        def _(algorithm_number:int, hist_data_after:int=0) -> FileResponse:
            print(f"[SERVER] setting algorithm as: {algorithm_number}")
            print(f"[SERVER] Setting history config as: {hist_data_after}")

            # configs.USE_HISTORIC_DIST_AFTER_N = hist_data_after

            self.run(
                    algorithm_number,
                    server_management.server_statistics_manager.reset_stat_files,
                    server_management.server_optimization_manager.periodic_evaluation_detail,
                    producers=server_management.server_endpoint_manager.producers,
                    workers=server_management.server_endpoint_manager.workers,
                    consumers=server_management.server_endpoint_manager.event_consumers,
                    cep_lock=server_management.server_endpoint_manager.worker_lock
                    )

            print("Experiment is over,  downloading results from the management device...")
            
            return FileResponse(path='current_run_results.zip', filename="current_run_results.zip")        

        @app.get("/download_simulation_results")
        def _() -> FileResponse:
            return FileResponse(path='current_run_results.zip', filename="current_run_results.zip")

    def run(self, 
            algorithm_number:int, 
            file_reset_func:Callable[[],None], 
            periodic_evaluation:Callable[[], None],
            producers:dict[str, dict[str, RawSettings]],
            workers:dict[str, dict[str, int | float]],
            consumers:dict[str, dict[str, CEPConsumerSettings]],
            task_weight:int=0,
            repeat:int=0,
            cep_lock:Lock=Lock(),
            ):
        
        Path("/current_run_results").mkdir(parents=True, exist_ok=True)
                    
        os.environ['DISTRIBUTION_TYPE']=str(algorithm_number)
        configs.env_distribution_type = algorithm_number
        configs.EVAL_ACTIVE = 1
        
        print("Unlocking devices for simulations...")

        # time.sleep(configs.evaluation_period * 2)
        time.sleep(configs.evaluation_period - 10.0)
        
        # crr_experiment_folder = "/current_run_results/"+str(algorithm_number)+"_"+str(task_weight)+"_"+str(repeat)
        crr_experiment_folder = "/current_run_results/server_stats"
        Path(crr_experiment_folder).mkdir(parents=True, exist_ok=True)
        
        # Call each client async and wait until all of them finishes
        thread_list = []
        for _, p_settings  in producers.items():
            for _, raw_settings in p_settings.items():
                thread_list.append(Thread(daemon=True, target=self.run_device, 
                                        kwargs={
                                            'host': raw_settings.producer_name, 
                                            'port': raw_settings.producer_port, 
                                            'data_name': raw_settings.raw_data_name}))
        t:Thread
        for t in thread_list:
            t.start()

        print("Sensors triggered...")
            
        def timed_join_all(threads:list[Thread], timeout:float):
            start = cur_time = time.time()
            while cur_time <= (start + timeout):
                for thread in threads:
                    if not thread.is_alive():
                        thread.join()
                time.sleep(1)
                cur_time: float = time.time()        
                
        # timed_join_all(thread_list, configs.simulation_duration + 60)
        timed_join_all(thread_list, configs.simulation_duration + 60)

        # Trigger statistic collection one last time to make sure current results are collected
        # periodic_evaluation()

        print("Stop the evaulation...")
        configs.EVAL_ACTIVE = 0

        print("Triggering stop for all producers...")
        
        thread_list: list[Thread] = []
        for _, p_settings  in producers.items():
            for _, raw_settings in p_settings.items():
                thread_list.append(Thread(daemon=True, target=self.stop_device, 
                                        kwargs={
                                            'host': raw_settings.producer_name, 
                                            'port': raw_settings.producer_port, 
                                            'data_name': raw_settings.raw_data_name}))
        
        t:Thread
        for t in thread_list:
            t.start()
            
        t:Thread
        for t in thread_list:
            t.join()                

        wait_after_finishing:int = 30
        print(f"Wait {wait_after_finishing} seconds to stop files from being written...")
        
        time.sleep(wait_after_finishing)
        
        # Carry the current statistic files to another directory
        print("Experiment is over, accumulating the results...")
        
        # Download the additional stats from raw data producers/workers etc.
        # Download the consumer stats
        print("Downloading alarm stats....")
        alarm_stat_path = "/current_run_results/alarm_stats"
        Path(alarm_stat_path).mkdir(parents=True, exist_ok=True)
        processed_alarm_file_names: list[str] = []
        for _, consumer_settings in consumers.items():
            for _, consumer_setting in consumer_settings.items():
                print("Downloading file...")
                alarm_file: requests.Response = self.download_consumer_stats(
                    consumer_setting.host_name, 
                    consumer_setting.host_port, 
                    consumer_setting.topic_name
                    )
                # print(alarm_file.headers)
                # print(alarm_file.content)
                # print(alarm_file.text)
                alarm_file_name: str = alarm_file.headers['content-disposition'].split(';')[1].removeprefix(' filename="').removesuffix('"')
                # print(alarm_file_name)
                if alarm_file_name not in processed_alarm_file_names:
                    processed_alarm_file_names.append(alarm_file_name)
                    with open(f"{alarm_stat_path}/{alarm_file_name}", "w") as outfile:
                        outfile.write(alarm_file.content.decode())
                    outfile.close()
                else:
                    print("File name was already processed, POSSIBLE DUPLICATE FILE NAMING!!!! This is a fatal error!")
                    sys.exit(0)

        # Download the raw data production stats
        print("Downloading raw production stats....")
        raw_production_stat_path = "/current_run_results/production_stats"
        Path(raw_production_stat_path).mkdir(parents=True, exist_ok=True)
        
        processed_raw_production_stats:list[str] = []
        for _, p_settings  in producers.items():
            for _, raw_settings in p_settings.items():
                print("Downloading file...")
                # Raw data production stats
                raw_production_stat_file: requests.Response = self.download_raw_data_production_stats(
                    raw_settings.producer_name, 
                    raw_settings.producer_port,
                    raw_settings.raw_data_name)
                
                raw_production_stat_file_name: str = raw_production_stat_file.headers['content-disposition'].split(';')[1].removeprefix(' filename="').removesuffix('"')
        
                if raw_production_stat_file_name not in processed_raw_production_stats:
                    processed_raw_production_stats.append(raw_production_stat_file_name)
                    with open(f"{raw_production_stat_path}/{raw_production_stat_file_name}", "w") as outfile:
                        outfile.write(raw_production_stat_file.content.decode())
                    outfile.close()
                else:
                    print("File name was already processed, POSSIBLE DUPLICATE FILE NAMING!!!! This is a fatal error!")
                    sys.exit(0)        
                                        
        # Copy the results from current management server to another file for zipping
        cep_lock.acquire()
        shutil.copytree("stats", crr_experiment_folder, dirs_exist_ok=True)     
        cep_lock.release()
        
        # print("Waiting 60 seconds until the next evaluation cycle to get latest results...")
        # time.sleep(60)
        # # Rest the stat files
        # file_reset_func()

        # Zip the results file
        print("Zipping results...")
        
        shutil.make_archive('current_run_results', 'zip', "/current_run_results")
        configs.EVAL_ACTIVE = 0
        
        print(f"Waiting for zip download testing...")
        # time.sleep(15) # TODO: why wait here?
                
    def stop_device(self, host:str, port:int, data_name:str) -> None:
        url: str = f'http://{host}:{port}/stop_sampling/{data_name}'
        response: requests.Response = requests.get(url=url)  
        if response.status_code != 200:
            raise Exception("Status is NOT 200!")              
                
    def run_device(self, host:str, port:int, data_name:str) -> None:
        url: str = f'http://{host}:{port}/start_sampling/{data_name}'
        response: requests.Response = requests.get(url=url)
        if response.status_code != 200:
            err_response = response.content.decode()
            print(f"Status is NOT 200: {err_response}, {host}:{port} data: {data_name}!")
            raise Exception(f"Status is NOT 200 for device: {host}:{port} data: {data_name}!")

    def download_consumer_stats(self, host:str, port:str, endpoint_name:str) -> requests.Response:
        url: str = f"http://{host}:{port}/fetch_stats/alarm_stats/{endpoint_name}"
        response: requests.Response = requests.get(url=url)
        if response.status_code not in [200, 404]:
            raise Exception("Status is NOT 200 or 404!")
        return response

    def download_raw_data_production_stats(self, host:str, port:int, endpoint_name:str):
        url: str = f"http://{host}:{port}/fetch_stats/production_stats/{endpoint_name}"
        print(f"Downloading production stat from: {url}")
        response: requests.Response = requests.get(url=url)
        if response.status_code not in [200, 404]:
            raise Exception("Status is NOT 200 or 404!")
        return response
