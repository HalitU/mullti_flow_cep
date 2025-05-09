from typing import List
from cep_library import configs
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.cp_v5.common.cp_sat_event_info import CPSATV3EventInfo
from cep_library.management.cp_v5.common.cp_sat_raw_source import CPSATV3RawSource
from ortools.sat.python import cp_model


class CPSATV4AssignmentVariables:
    def __init__(self,
                 worker_device_ids:List[str],
                 events:dict[str, CPSATV3EventInfo],
                 raws:dict[str, CPSATV3RawSource],
                 model:cp_model.CpModel,
                 consumers: dict[str, dict[str, CEPConsumerSettings]],
                 executable_device_limit:int) -> None:
        self.model:cp_model.CpModel = model
        self.executable_device_limit = executable_device_limit

        
        self.raw_source_targets:dict[str, dict[str, dict[str, cp_model.IntVar]]] = {}
        self.raw_source_target_costs:dict[str, dict[str, dict[str, cp_model.IntVar]]] = {}

        
        self.event_reading_locations:dict[str, dict[str, dict[str, cp_model.IntVar]]] = {}
        self.event_reading_location_costs:dict[str, dict[str, List[cp_model.IntVar]]] = {}

        
        self.event_execution_location_vars:dict[str, dict[str, cp_model.IntVar]] = {}

        
        self.event_output_location_vars:dict[str, dict[str, dict[str, cp_model.IntVar]]] = {}
        self.event_output_location_costs:dict[str, dict[str, List[cp_model.IntVar]]] = {}

        
        self.event_consumer_location_vars:dict[str, dict[str, dict[str, cp_model.IntVar]]] = {}
        self.event_consumer_reading_costs:dict[str, dict[str, dict[str, cp_model.IntVar]]] = {}

        
        print("generating variables")

        n_vars = self.raw_source_output_targets(worker_device_ids, raws)
        print(f"raw_source_output_targets completed: {n_vars}")

        n_vars = self.event_consumer_locations(worker_device_ids, consumers)
        print(f"event_consumer_locations completed: {n_vars}")

        n_vars = self.prepare_event_reading_locations(worker_device_ids, events)
        print(f"prepare_event_reading_locations completed: {n_vars}")

        n_vars = self.event_execution_locations(worker_device_ids, events)
        print(f"event_execution_locations completed: {n_vars}")

        n_vars = self.event_output_locations(worker_device_ids, events)
        print(f"event_output_locations completed: {n_vars}")

    
    def raw_source_output_targets(self, worker_device_ids:list[str], raws:dict[str, CPSATV3RawSource]):
        n_vars = 0
        for raw_source_id, raw_setting in raws.items():            
            self.raw_source_targets[raw_source_id] = {}
            self.raw_source_target_costs[raw_source_id] = {}

            
            topics:List[str] = [raw_setting.settings.output_topic.output_topic]
            for raw_output_topic in topics:
                self.raw_source_targets[raw_source_id][raw_output_topic] = {}
                self.raw_source_target_costs[raw_source_id][raw_output_topic] = {}

                
                for worker_device_id in worker_device_ids:
                    new_raw_source_target = self.model.NewBoolVar("v")

                    
                    if configs.KEEP_RAWDATA_AT_SOURCE:
                        if worker_device_id == raw_setting.settings.producer_name:
                            self.model.Add(new_raw_source_target == 1)
                        else:
                            self.model.Add(new_raw_source_target == 0)

                    self.raw_source_targets[raw_source_id][raw_output_topic][worker_device_id] = new_raw_source_target

        return n_vars

    def prepare_event_reading_locations(self, worker_device_ids:List[str], events:dict[str, CPSATV3EventInfo]):
        n_vars = 0
        for event_id, event_info in events.items():
            self.event_reading_locations[event_id] = {}
            self.event_reading_location_costs[event_id] = {}

            for rti in event_info.cep_task.settings.required_sub_tasks:
                self.event_reading_locations[event_id][rti.input_topic] = {}
                self.event_reading_location_costs[event_id][rti.input_topic] = []

        return n_vars

    
    def event_execution_locations(self, worker_device_ids:list[str], events:dict[str, CPSATV3EventInfo]):
        n_vars = 0
        
        for event_id in events:
            current_event_execution_locations:dict[str, cp_model.IntVar] = {}
            for worker_device_id in worker_device_ids:
                new_event_execution_location: cp_model.IntVar = self.model.NewBoolVar("v")
                n_vars += 1
                current_event_execution_locations[worker_device_id] = new_event_execution_location
                                
            self.event_execution_location_vars[event_id] = current_event_execution_locations

        return n_vars

    
    def event_output_locations(self, worker_device_ids:List[str], events:dict[str, CPSATV3EventInfo]):
        n_vars = 0
        for e_id, e_task in events.items():
            self.event_output_location_vars[e_id] = {}
            self.event_output_location_costs[e_id] = {}
            
            topics:List[str] = [e_task.cep_task.settings.output_topic.output_topic]
            for output_topic in topics:
                self.event_output_location_vars[e_id][output_topic] = {}
                self.event_output_location_costs[e_id][output_topic] = []
                
                for d_id in worker_device_ids:
                    new_topic_output_location = self.model.NewBoolVar("v")
                    n_vars += 1

                    if configs.CP_SAT_WRITE_AT_EXECUTION_LOC:
                        exec_at_loc:cp_model.IntVar = self.event_execution_location_vars[e_id][d_id]
                        self.model.Add(exec_at_loc == new_topic_output_location)

                    self.event_output_location_vars[e_id][output_topic][d_id] = new_topic_output_location

        return n_vars

    
    def event_consumer_locations(self, worker_device_ids:List[str], consumers:dict[str, dict[str, CEPConsumerSettings]]):
        
        for consumer_id, consumer_topics in consumers.items():
            self.event_consumer_location_vars[consumer_id] = {}
            self.event_consumer_reading_costs[consumer_id] = {}

            
            for topic_id, _ in consumer_topics.items():                
                self.event_consumer_location_vars[consumer_id][topic_id] = {}
                self.event_consumer_reading_costs[consumer_id][topic_id] = {}
