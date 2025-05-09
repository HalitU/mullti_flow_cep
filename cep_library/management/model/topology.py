from enum import Enum
import math
from pathlib import Path
from typing import List
import networkx as nx
from networkx import DiGraph

from cep_library import configs
from cep_library.cep.model.cep_task import CEPTask
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.management.statistics.consumer_server_statics import ConsumerServerStatics
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import ManagementServerStatisticsAnalyzer
from cep_library.raw.model.raw_settings import RawSettings

class NodeType(Enum):
    HEAD=0
    RAWOUTPUT=1
    EVENTEXECUTION=2
    SINK=3
    CONSUMER=4

class TopologyNode:
    def __init__(self, node_id:int, node_type:NodeType, node_data:RawSettings|CEPTask|CEPConsumerSettings, name:str) -> None:
        self.node_id: int = node_id
        self.node_type: NodeType = node_type
        self.node_data: RawSettings | CEPTask | CEPConsumerSettings = node_data
        self.name: str = name
        self.processed = False
        self.raw_sources:List[str] = []
        self.flow_id:int=0
        self.expected_execution_count:float=1.0
        
    def set_expected_execution_count(self, count:float) -> None:
        self.expected_execution_count = count
        
    def increment_expected_execution_count(self, count:float) -> None:
        self.expected_execution_count += count

    def get_inputs_topics(self) -> List[str]:
        match self.node_type:
            case NodeType.RAWOUTPUT:
                raise Exception("Invalid node type for getting input topics!")
            case NodeType.EVENTEXECUTION:
                nd:CEPTask = self.node_data
                return [st.input_topic for st in nd.settings.required_sub_tasks]
            case NodeType.CONSUMER:
                cd:CEPConsumerSettings = self.node_data
                return [cd.topic_name]
            case NodeType.HEAD:
                pass
            case NodeType.SINK:
                pass
        raise Exception("Invalid node type for getting input topics!")
    
    def get_output_topic(self) -> str:
        match self.node_type:
            case NodeType.RAWOUTPUT:
                rd:RawSettings = self.node_data
                return rd.output_topic.output_topic
            case NodeType.EVENTEXECUTION:
                nd:CEPTask = self.node_data
                return nd.settings.output_topic.output_topic
            case NodeType.CONSUMER:
                cd:CEPConsumerSettings = self.node_data
                return cd.topic_name
            case NodeType.SINK:
                raise Exception("Invalid node to call output topics!")
            case NodeType.HEAD:
                raise Exception("Invalid node to call output topics!")
        raise Exception("Invalid node to call output topics!")

class Topology:
    def __init__(self) -> None:
        self.G = nx.DiGraph()
        self.Head = None
        self.graph_file = "visuals/graph_output.txt"
        self.current_host_code_dist: dict
        self.crr_node_id = 1
        self.is_weak = False
        self.n_of_nodes = 0

        
        self.input_topic_nodes:dict[str, List[TopologyNode]] = {}
        
        
        self.output_topic_nodes:dict[str, List[TopologyNode]] = {}
        
        self.producer_nodes:List[TopologyNode] = []
        self.executor_nodes:List[TopologyNode] = []
    
        Path("visuals/").mkdir(parents=True, exist_ok=True)
        with open(self.graph_file, "w") as outfile:
            outfile.write("\n")
        outfile.close()    
    
    def update_expected_flow_counts(self,
            mssa: ManagementServerStatisticsAnalyzer,
            rawstats: RawServerStatAnalyzer,
            consumerstats: ConsumerServerStatics
        ) -> None:
        
        
        
        
        mssa.expected_event_input_counts = {}
        mssa.expected_event_output_counts = {}
        consumerstats.expected_event_counts = {}
        
        
        for pn in self.producer_nodes:
            
            n_d:RawSettings = pn.node_data
            output_topic_list:List[str] = [n_d.output_topic.output_topic]
            raw_expected_event_count = math.ceil(rawstats.get_expected_event_count(n_d.producer_name, n_d.output_topic.output_topic))
            print(f"Current raw expected count: {n_d.output_topic.output_topic}, {raw_expected_event_count}")
            
            
            successors:List[TopologyNode] = []
            raw_successors: List[TopologyNode] = self.get_successors(pn)
            
            
            successors = successors + raw_successors
            
            while len(successors) > 0:
                
                
                succ:TopologyNode = successors[0]

                if succ.node_type == NodeType.RAWOUTPUT or succ.node_type == NodeType.SINK:
                    pass
                
                if succ.node_type == NodeType.EVENTEXECUTION:
                    e_d:CEPTask = succ.node_data
                    
                    
                    
                    for rti in e_d.settings.required_sub_tasks:
                        for otl in output_topic_list:
                            
                            
                            if rti.input_topic == otl:
                                
                                mssa.increment_expected_event_input_counts(e_d.settings.action_name, rti.input_topic, raw_expected_event_count)
                                

                    
                    
                    mssa.increment_expected_event_output_counts(e_d.settings.action_name, e_d.settings.output_topic.output_topic, raw_expected_event_count)
                    
                    
                    
                    if e_d.settings.output_topic.output_topic not in output_topic_list:
                        output_topic_list.append(e_d.settings.output_topic.output_topic)

                
                if succ.node_type == NodeType.CONSUMER:
                    s_d:CEPConsumerSettings = succ.node_data
                    
                    for otl in output_topic_list:
                        if s_d.topic_name == otl:
                            if configs.LOGS_ACTIVE:
                                print(f"Processing consumer input topic: {s_d.topic_name} with expected: {raw_expected_event_count}")
                            consumerstats.increment_expected_event_counts(s_d.host_name, s_d.topic_name, raw_expected_event_count)
                            if configs.LOGS_ACTIVE:
                                print(f"consumer input topic: {s_d.topic_name} current expected: {consumerstats.get_expected_event_counts(s_d.host_name, s_d.topic_name)}")
    
                
                next_successors = self.get_successors(succ)
                for ns in next_successors:
                    successors.append(ns)
                    
                
                del successors[0]

        

    def get_next_node_id(self)->int:
        available_id = self.crr_node_id
        self.crr_node_id += 1
        return available_id
    
    def get_node_count(self):
        return self.n_of_nodes
    
    
    
    def validate_all_paths_start_from_sink_end_at_head(self):
        print("Validating the graph...")
        bfs_count = len(self.get_bfs())
        print(f"Number of nodes in the bfs structure is: {bfs_count}")
        print(f"Number of nodes added: {self.n_of_nodes}")
        
        if bfs_count != self.n_of_nodes:
            print("Not all nodes are between the source and the sink!!!")
            return False
        print("Head-Sink connection validation completed...")
        return True

    def get_topological_ordered_nodes(self):
        return list(nx.topological_sort(self.G))
    
    def get_topologically_ordered_executor_nodes(self) -> List[TopologyNode]:
        ordered_tasks = []
        t:TopologyNode
        for t in list(nx.topological_sort(self.G)):
            if t.node_type == NodeType.EVENTEXECUTION:
                ordered_tasks.append(t)
        return ordered_tasks
    
    def get_executor_nodes(self) -> List[TopologyNode]:
        return self.executor_nodes
        
    def get_producer_node(self, prod_name:str, raw_name:str) -> TopologyNode:
        return [p for p in self.producer_nodes if p.node_data.producer_name == prod_name and p.node_data.output_topic.output_topic == raw_name][0]
        
    def get_producer_nodes(self) -> List[TopologyNode]:
        return self.producer_nodes
    
    def get_host_code_relationship(self) -> dict:
        return self.current_host_code_dist
    
    def set_source(self, node:TopologyNode):
        self.Head = node
    
    def add_node(self, node:TopologyNode):
        if node.node_type == NodeType.RAWOUTPUT:
            self.producer_nodes.append(node)
            r_data:RawSettings = node.node_data
            
            
            output_topic:str = r_data.output_topic.output_topic
            if output_topic not in self.input_topic_nodes:
                self.input_topic_nodes[output_topic] = []
            self.input_topic_nodes[output_topic].append(node)
            
        if node.node_type == NodeType.EVENTEXECUTION:
            self.executor_nodes.append(node)
            e_data:CEPTask = node.node_data

            
            for ri in e_data.settings.required_sub_tasks:
                input_topic:str = ri.input_topic
                if input_topic not in self.output_topic_nodes:
                    self.output_topic_nodes[input_topic] = []
                self.output_topic_nodes[input_topic].append(node)
            
            
            output_topic:str = e_data.settings.output_topic.output_topic
            if output_topic not in self.input_topic_nodes:
                self.input_topic_nodes[output_topic] = []
            self.input_topic_nodes[output_topic].append(node)
                
        if node.node_type == NodeType.CONSUMER:
            c_data:CEPConsumerSettings= node.node_data
            
            
            input_topic:str = c_data.topic_name
            if input_topic not in self.output_topic_nodes:
                self.output_topic_nodes[input_topic] = []
            self.output_topic_nodes[input_topic].append(node)

        self.G.add_node(node)
        self.n_of_nodes += 1

    def add_edge(self, from_node:TopologyNode, to_node:TopologyNode, topic:str) -> bool:
        if self.G.has_edge(from_node, to_node):
            return False
        self.G.add_edge(from_node, to_node, topic=topic)
        return True
        
    def get_predecessors_from_name(self, name:str):
        crr_node = [n for n in self.G.nodes if n.name == name][0]
        return list(self.G.predecessors(crr_node))
        
    def get_predecessors(self, node:TopologyNode):
        return list(self.G.predecessors(node))
    
    def get_successors(self, node:TopologyNode) -> list[TopologyNode]:
        return list(self.G.successors(node))

    def get_successors_from_name(self, name:str):
        crr_node = [n for n in self.G.nodes if n.name == name][0]
        return list(self.G.successors(crr_node))

    def get_nodes_from_input_topic(self, input_topic:str) -> List[TopologyNode]:
        return self.input_topic_nodes[input_topic]

    def get_nodes_from_out_topic(self, output_topic:str) -> List[TopologyNode]:
        return self.output_topic_nodes[output_topic]

    def print_nodes(self) -> None:
        for u, n in self.G.nodes(data=True):
            print(u, " ", n)
            
    def get_network(self) -> DiGraph:
        return self.G

    def reset_visited_status(self):
        for u, _ in self.G.nodes(data=True):
            u.processed = False

    def get_bfs(self) -> nx.DiGraph:
        return nx.bfs_tree(self.G, self.Head)
    
    def print_graph_test(self):
        bfs: DiGraph = self.get_bfs()
        with open(self.graph_file, "a") as outfile:
            for node, _ in bfs.nodes(data=True):
                outfile.write(node.to_text())
        outfile.close()
