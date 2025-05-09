from typing import List
from ortools.sat.python import cp_model

from cep_library import configs
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.cp_v5.common.cp_sat_event_info import CPSATV3EventInfo
from cep_library.management.cp_v5.common.cp_sat_raw_source import CPSATV3RawSource
from cep_library.management.cp_v5.v4.v4_assignment_vars import CPSATV4AssignmentVariables
from cep_library.management.model.topology import NodeType, Topology


class V4ConstraintManager:
    def __init__(self, 
                 model:cp_model.CpModel, 
                 variables: CPSATV4AssignmentVariables,
                 events:dict[str, CPSATV3EventInfo],
                 raws:dict[str, CPSATV3RawSource],
                 all_device_ids:List[str],
                 consumers: dict[str, dict[str, CEPConsumerSettings]],
                 topology:Topology) -> None:
        
        self.model = model
        self.variables = variables
        self.events = events
        self.raws = raws
        self.all_device_ids = all_device_ids
        self.consumers = consumers
        self.topology = topology

        self.raw_output_constraints()
        print(f"raw_output_constraints finished.")
        
        self.event_output_constraints()
        print("event_output_constraints finished")
        
        self.event_execution_constraints()
        print("event_execution_constraints finished")

        print("Finished [CPSATV3ConstraintManager]")

    
    
    
    def raw_output_constraints(self):
        
        for raw_name, raw_source_targets in self.variables.raw_source_targets.items():
            if configs.CP_SAT_DEBUG_VALIDATIONS:
                successors = self.topology.get_successors_from_name(raw_name)
                writable_limit:int = len(successors)
                if writable_limit == 0:
                    raise Exception("Every raw node should have successors!")
            
            for raw_topic_id, raw_output_targets in raw_source_targets.items():
                
                rel_nodes = self.topology.get_nodes_from_out_topic(raw_topic_id)
                raw_output_written_devices: List[cp_model.IntVar] = []
                
                for written_device_id, written_to_device in raw_output_targets.items():
                    raw_output_written_devices.append(written_to_device)
                    
                    
                    for rn in rel_nodes:
                        if rn.node_type == NodeType.EVENTEXECUTION:
                            self.variables.event_reading_locations[rn.name][raw_topic_id][written_device_id] = written_to_device
                        elif rn.node_type == NodeType.CONSUMER:
                            self.variables.event_consumer_location_vars[rn.name][raw_topic_id][written_device_id] = written_to_device
               
                
                self.model.AddExactlyOne(raw_output_written_devices)

    def event_execution_constraints(self):
        print("Processing [event_execution_constraints]")
        
        worker_loads:dict[str, List[cp_model.IntVar]] = {}
        
        
        
        for _, event_locations in self.variables.event_execution_location_vars.items():
            event_execution_locs:List[cp_model.IntVar] = []

            
            for worker_id, worker_var in event_locations.items():
                event_execution_locs.append(worker_var)

                if worker_id not in worker_loads:
                    worker_loads[worker_id] = []
                worker_loads[worker_id].append(worker_var)

            
            self.model.AddExactlyOne(event_execution_locs)

    def event_output_constraints(self):
        print("Processing [event_output_constraints]")
        
        
        for event_id, e_topics in self.variables.event_output_location_vars.items():
            if configs.CP_SAT_DEBUG_VALIDATIONS:
                successors = self.topology.get_successors_from_name(event_id)
                writable_limit:int = len(successors)
                if writable_limit == 0:
                    raise Exception("Every raw node should have successors!")

            
            for topic_id, t_devices in e_topics.items():
                
                rel_nodes = self.topology.get_nodes_from_out_topic(topic_id)
                
                
                output_device_vars:List[cp_model.IntVar] = []
                for d_id, d_var in t_devices.items():
                    output_device_vars.append(d_var)

                    
                    for rn in rel_nodes:
                        if rn.node_type == NodeType.EVENTEXECUTION:
                            
                            self.variables.event_reading_locations[rn.name][topic_id][d_id] = d_var
                        elif rn.node_type == NodeType.CONSUMER:
                            
                            cn_data:CEPConsumerSettings = rn.node_data 
                            self.variables.event_consumer_location_vars[cn_data.host_name][topic_id][d_id] = d_var
                        else:
                            raise Exception("Invalid node type detected!")

                
                self.model.AddExactlyOne(output_device_vars)
