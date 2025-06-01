from copy import deepcopy
from datetime import datetime, timezone
from typing import List, Literal, Tuple
from cep_library.cep.model.cep_task import CEPTask

from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
from cep_library.management.model.task_alteration import (
    DataMigrationModel,
    TaskAlterationModel,
)
from cep_library.management.model.topology import NodeType, Topology, TopologyNode
from cep_library.management.statistics.consumer_server_statics import (
    ConsumerServerStatics,
)
from cep_library.management.statistics.load_helper import get_max_percentage
from cep_library.management.statistics.raw_server_statics import RawServerStatAnalyzer
from cep_library.management.statistics.statistics_analyzer import (
    ManagementServerStatisticsAnalyzer,
)
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.model.raw_update_event import RawUpdateEvent
import numpy as np
import cep_library.configs as configs


class PathCostTracker:
    def __init__(self) -> None:
        self.current_cost: float = 0.0

    def add_cost(self, cost: float) -> None:
        self.current_cost += cost

    def subtract_cost(self, cost: float) -> None:
        self.current_cost -= cost


class EncodingModel:
    def __init__(self, step_name: str, node_type: NodeType) -> None:
        self.step_name: str = step_name
        self.node_type: NodeType = node_type

        self.output_targets: List[str] = []
        self.execution_targets: List[str] = []

        self.step_cost: float = 0.0
        self.to_be_updated: bool = False

    def update_step_cost(self, step_cost: float) -> None:
        self.step_cost = step_cost
        self.to_be_updated = False

    def get_step_cost(self) -> float:
        return self.step_cost

    def get_output_topic(self) -> str:
        return self.output_topic

    def register_output_topic(self, output_topic: str):
        self.output_topic: str = output_topic

    def add_output_target(self, output_target: str) -> Literal[True]:
        if output_target not in self.output_targets:
            self.output_targets.append(output_target)
        return True

    def add_execution_target(self, execution_target: str) -> Literal[True]:
        if execution_target not in self.execution_targets:
            self.execution_targets.append(execution_target)
        return True

    def mutate_output_target(self, ix: int, new_target: str) -> None:
        self.output_targets[ix] = new_target

    def get_output_targets(self) -> List[str]:
        return self.output_targets

    def mutate_execution_target(self, ix: int, new_target: str) -> None:
        self.execution_targets[ix] = new_target

    def get_execution_targets(self) -> List[str]:
        return self.execution_targets


class CEPGeneticAlgorithm:
    def __init__(
        self,
        topology: Topology,
        producer_records: dict[str, dict[str, RawSettings]],
        workers: dict[str, dict[str, int | float]],
        event_consumers: dict[str, dict[str, CEPConsumerSettings]],
        cep_tasks: List[CEPTask],
        rawstats: RawServerStatAnalyzer,
        mssa: ManagementServerStatisticsAnalyzer,
        consumerstats: ConsumerServerStatics,
        last_best_encoding: dict,
    ) -> None:
        self.workers: dict[str, dict[str, int | float]] = workers
        self.topology: Topology = topology
        self.producer_records: dict[str, dict[str, RawSettings]] = producer_records
        self.event_consumers: dict[str, dict[str, CEPConsumerSettings]] = (
            event_consumers
        )
        self.mssa: ManagementServerStatisticsAnalyzer = mssa
        self.max_generation = configs.GA_PARAMS_MAX_GENERATION
        self.population_size = configs.GA_PARAMS_MAX_POPULATION
        self.parent_selection = configs.GA_PARAMS_PARENT_SELECTION
        self.mutation_size = configs.GA_PARAMS_MUTATION_SIZE
        self.mutation_probability = configs.GA_PARAMS_MUTATION_PROB
        self.device_limit: int = configs.DEVICE_ACTION_LIMIT
        self.population = []
        self.last_best_encoding: dict[str, EncodingModel] = last_best_encoding
        self.rawstats: RawServerStatAnalyzer = rawstats
        self.consumerstats: ConsumerServerStatics = consumerstats

        self.migration_penalty: float = configs.DEVICE_CHANGE_PENALTY

    def solve(
        self,
    ) -> tuple[
        List[TaskAlterationModel],
        List[RawUpdateEvent],
        List[CEPConsumerUpdateEvent],
        dict[str, EncodingModel],
        float,
    ]:

        if not self.workers:
            return [], [], [], {}, 0.0

        start_time = datetime.now(tz=timezone.utc)
        best_citizen_in_population = [], -1, -1
        self.population: List[
            Tuple[dict[str, EncodingModel], float, float, dict[str, float], float]
        ] = self.initialize_population()

        best_citizen_in_population, _ = self.update_best_solution(
            best_citizen_in_population, self.population
        )
        initial_population_elapsed = (
            datetime.now(tz=timezone.utc) - start_time
        ).seconds
        print(
            f"Initial population took {initial_population_elapsed} seconds to generate."
        )

        gen_ix = 0
        for _ in range(self.max_generation):

            parents_to_keep = self.ga_select_ranking()

            children = self.ga_cross_single_point()

            self.ga_mutate(parents_to_keep, children)

            if len(self.population) != self.population_size:
                raise Exception(
                    "Newly generated population does not match the population size!"
                )

            prev_best = best_citizen_in_population
            best_citizen_in_population, sol_changed = self.update_best_solution(
                best_citizen_in_population, self.population
            )

            if sol_changed and best_citizen_in_population[1] == prev_best[1]:
                raise Exception("Solution changed but model did not!")

            gen_ix += 1

        print(
            f"Number of generations finished: {gen_ix}, best solution value: {best_citizen_in_population[1]}, bw load: {best_citizen_in_population[2]}"
        )

        if gen_ix != self.max_generation:
            raise Exception(
                "Number of generations does not match with what is required!"
            )

        return self.get_final_distribution(best_citizen_in_population)

    def ga_select_ranking(self):

        self.population.sort(key=lambda t: t[1], reverse=True)

        return self.population[: self.parent_selection]

    def update_best_solution(
        self,
        current_best_solution,
        population: List[
            Tuple[dict[str, EncodingModel], float, float, dict[str, float], float]
        ],
    ):
        sol_changed = False
        for p in population:
            if p[1] > current_best_solution[1]:
                current_best_solution = p
                sol_changed = True
        if sol_changed:
            print("Best solution is changed.")
        return current_best_solution, sol_changed

    def ga_cross_single_point(
        self,
    ) -> List[Tuple[dict[str, EncodingModel], float, float, dict[str, float], float]]:

        encodings: List[int] = []
        weights: List[float] = []
        for enc_ix, (_, w, _, _, _) in enumerate(self.population):
            if w != 0:
                encodings.append(enc_ix)
                weights.append(w)

        weights = np.divide(weights, sum(weights))

        new_generation = []
        for _ in range(0, self.population_size - self.parent_selection):
            current_parents = np.random.choice(
                encodings, size=2, p=weights, replace=False
            )

            first_encoding: dict[str, EncodingModel] = self.population[
                current_parents[0]
            ][0]
            second_encoding: dict[str, EncodingModel] = self.population[
                current_parents[1]
            ][0]

            worker_loads: dict[str, int] = {}
            for w_id in self.workers.keys():
                worker_loads[w_id] = 0

            new_child_encoding: dict[str, EncodingModel] = {}
            for step_name, em in first_encoding.items():
                new_child_encoding[step_name] = EncodingModel(
                    em.step_name, em.node_type
                )
                new_child_encoding[step_name].to_be_updated = True

                if em.node_type == NodeType.CONSUMER:
                    continue
                else:

                    new_child_encoding[step_name].register_output_topic(
                        em.get_output_topic()
                    )

                if em.node_type == NodeType.RAWOUTPUT:

                    fop: List[str] = em.get_output_targets()
                    sop: List[str] = second_encoding[step_name].get_output_targets()

                    combined = fop + sop
                    unique_set = list(set(combined))

                    fop_l = len(fop)
                    sop_l = len(sop)

                    probable_len = min(fop_l, sop_l)

                    if len(unique_set) > 1:
                        size_choice: int = np.random.randint(1, probable_len + 1)
                        new_output_targets = np.random.choice(
                            a=unique_set, size=size_choice, replace=False
                        )
                    else:
                        new_output_targets: List[str] = unique_set

                    if len(new_output_targets) == 0:
                        raise Exception("Number of output targets cannot be 0!")

                    for no in new_output_targets:
                        new_child_encoding[step_name].add_output_target(no)
                elif em.node_type == NodeType.EVENTEXECUTION:

                    fop: List[str] = em.get_execution_targets()
                    sop: List[str] = second_encoding[step_name].get_execution_targets()

                    fop_l: int = len(fop)
                    sop_l: int = len(sop)

                    probable_len: int = min(fop_l, sop_l)

                    combined: List[str] = fop + sop
                    unique_set = list(set(combined))

                    valid_set = self.get_valid_workers_from_zero(worker_loads)
                    unique_valid_set: List[str] = []
                    for us in unique_set:
                        if us in valid_set:
                            unique_valid_set.append(us)

                    if len(unique_valid_set) > 0:
                        unique_set = unique_valid_set
                    else:
                        unique_set = valid_set

                    if len(unique_set) > 1:
                        size_choice = np.random.randint(1, probable_len + 1)
                        new_output_targets = np.random.choice(
                            a=unique_set, size=size_choice, replace=False
                        )
                    else:
                        new_output_targets = unique_set

                    if len(new_output_targets) == 0:
                        raise Exception(
                            f"Number of output targets cannot be 0, {unique_set} - {valid_set}, {unique_valid_set}"
                        )

                    for no in new_output_targets:
                        new_child_encoding[step_name].add_execution_target(no)
                        worker_loads[no] += 1
                        if worker_loads[no] > configs.DEVICE_ACTION_LIMIT:
                            raise Exception("Device limit is passed!")
                        if configs.GENETIC_ALGO_WRITE_AT_DEVICE:
                            new_child_encoding[step_name].add_output_target(no)

                    if not configs.GENETIC_ALGO_WRITE_AT_DEVICE:

                        fop: List[str] = em.get_output_targets()
                        sop: List[str] = second_encoding[step_name].get_output_targets()

                        combined = fop + sop
                        unique_set = list(set(combined))

                        fop_l = len(fop)
                        sop_l = len(sop)

                        probable_len = min(fop_l, sop_l)

                        if len(unique_set) > 1:
                            size_choice = np.random.randint(1, probable_len + 1)
                            new_output_targets = np.random.choice(
                                a=unique_set, size=size_choice, replace=False
                            )
                        else:
                            new_output_targets = unique_set

                        if len(new_output_targets) == 0:
                            raise Exception("Number of output targets cannot be 0!")

                        for no in new_output_targets:
                            new_child_encoding[step_name].add_output_target(no)

            child_fitness, bw_load, worker_external_loads, max_perc_load = (
                self.calculate_fitness(new_child_encoding)
            )

            if child_fitness == 0:
                raise Exception("Child fitness cannot be zero!")

            new_candidate = (
                new_child_encoding,
                child_fitness,
                bw_load,
                worker_external_loads,
                max_perc_load,
            )

            new_generation.append(new_candidate)

        return new_generation

    def ga_mutate(
        self,
        parents_to_keep: List[
            Tuple[dict[str, EncodingModel], float, float, dict[str, float], float]
        ],
        children: List[
            Tuple[dict[str, EncodingModel], float, float, dict[str, float], float]
        ],
    ):
        self.population = parents_to_keep + children

        self.population.sort(key=lambda t: t[1], reverse=True)

        worker_list: List[str] = list(self.workers.keys())

        mutated_count = 0
        for p_ix in range(
            (self.population_size - self.mutation_size), self.population_size
        ):

            mutation_target: dict[str, EncodingModel]
            mutation_fitness: float
            (
                mutation_target,
                mutation_fitness,
                mutation_bw,
                worker_external_loads,
                max_perc_load,
            ) = self.population[p_ix]

            worker_loads: dict[str, int] = {}
            for w_id in self.workers.keys():
                worker_loads[w_id] = 0

            for _, step_encoding in mutation_target.items():
                if step_encoding.node_type == NodeType.EVENTEXECUTION:
                    output_targets = step_encoding.get_execution_targets()
                    for ot in output_targets:
                        worker_loads[ot] += 1

            for _, step_encoding in mutation_target.items():
                if step_encoding.node_type == NodeType.RAWOUTPUT:
                    output_targets = step_encoding.get_output_targets()
                    for ot_ix, ot_d in enumerate(output_targets):
                        prob_weights = [
                            (
                                self.mutation_probability
                                if w_l == ot_d
                                else (1.0 - self.mutation_probability)
                                / (len(worker_list) - 1)
                            )
                            for w_l in worker_list
                        ]
                        if len(worker_list) == 1:
                            prob_weights = [1]
                        change = np.random.choice(a=worker_list, size=1, p=prob_weights)

                        if not configs.KEEP_RAWDATA_AT_SOURCE:
                            step_encoding.mutate_output_target(ot_ix, change[0])
                        step_encoding.to_be_updated = True
                elif step_encoding.node_type == NodeType.EVENTEXECUTION:
                    crr_valid_worker_list = self.get_valid_workers_from_zero(
                        worker_loads
                    )
                    output_targets = step_encoding.get_execution_targets()
                    for ot_ix, ot_d in enumerate(output_targets):
                        coin_flip = np.random.choice(
                            a=[1, 2],
                            size=1,
                            replace=False,
                            p=[
                                self.mutation_probability,
                                (1.0 - self.mutation_probability),
                            ],
                        )
                        if coin_flip == 1 and len(crr_valid_worker_list) > 0:
                            change = np.random.choice(a=crr_valid_worker_list, size=1)
                        else:
                            change = [ot_d]
                        step_encoding.mutate_execution_target(ot_ix, change[0])
                        step_encoding.to_be_updated = True

                        worker_loads[ot_d] -= 1
                        worker_loads[change[0]] += 1

                        if worker_loads[change[0]] > configs.DEVICE_ACTION_LIMIT:
                            raise Exception("Device limit is passed!")

                        if configs.GENETIC_ALGO_WRITE_AT_DEVICE:
                            step_encoding.mutate_output_target(ot_ix, change[0])

                    if not configs.GENETIC_ALGO_WRITE_AT_DEVICE:
                        output_targets = step_encoding.get_output_targets()
                        for ot_ix, ot_d in enumerate(output_targets):
                            prob_weights = [
                                (
                                    self.mutation_probability
                                    if w_l == ot_d
                                    else (1.0 - self.mutation_probability)
                                    / (len(crr_valid_worker_list) - 1)
                                )
                                for w_l in crr_valid_worker_list
                            ]
                            if len(crr_valid_worker_list) == 1:
                                prob_weights = [1]
                            change = np.random.choice(
                                a=crr_valid_worker_list, size=1, p=prob_weights
                            )
                            step_encoding.mutate_output_target(ot_ix, change[0])
                            step_encoding.to_be_updated = True

            mutation_fitness, mutation_bw, worker_external_loads, max_perc_load = (
                self.calculate_fitness(mutation_target)
            )

            self.population[p_ix] = (
                mutation_target,
                mutation_fitness,
                mutation_bw,
                worker_external_loads,
                max_perc_load,
            )

            mutated_count += 1

        if mutated_count != self.mutation_size:
            raise Exception(
                f"Number of mutations does not match what is required: {mutated_count}!"
            )

    def initialize_population(self):
        generation = []
        fixed_pop_size = self.population_size

        if self.last_best_encoding:
            w_val, bw_val, worker_external_loads, max_load_perc = (
                self.calculate_fitness(self.last_best_encoding)
            )
            last_enc = (
                self.last_best_encoding,
                w_val,
                bw_val,
                worker_external_loads,
                max_load_perc,
            )
            generation.append(last_enc)
            fixed_pop_size -= 1

        for _ in range(fixed_pop_size):
            generation.append(self.generate_citizen())
        return generation

    def calculate_fitness(
        self, encoding: dict[str, EncodingModel]
    ) -> Tuple[float, float, dict[str, float], float]:
        maximum_path_cost = PathCostTracker()
        raw_nodes = self.topology.get_producer_nodes()
        ct = PathCostTracker()
        worker_external_loads: dict[str, float] = {}
        worker_external_loads_keeper: List[str] = []
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]] = {}

        device_keys = list(self.workers.keys())
        worker_cpu_load: dict[str, float] = {}

        for w in device_keys:
            worker_cpu_load[w] = 0.0

        for raw_n in raw_nodes:

            self.traverse_raw_node(
                raw_n,
                ct,
                encoding,
                {},
                {},
                maximum_path_cost,
                worker_external_loads,
                worker_external_loads_keeper,
                worker_link_loads,
                worker_cpu_load,
            )

        link_ct = PathCostTracker()
        max_link_ct = PathCostTracker()
        if configs.GA_USE_END_TO_END_LINK_MAX:
            max_load = 0.0
            tr_device_uplink_loads, tr_device_downlink_loads = (
                self.get_device_load_penalties(worker_link_loads)
            )
            for raw_n in raw_nodes:
                self.traverse_raw_load(
                    raw_n,
                    link_ct,
                    encoding,
                    {},
                    {},
                    max_link_ct,
                    tr_device_uplink_loads,
                    tr_device_downlink_loads,
                    worker_cpu_load,
                )
        else:

            sum_load, max_load, link_load_sum, link_load_max = (
                self.get_external_load_penalty(
                    worker_external_loads, worker_link_loads, worker_cpu_load
                )
            )

        if configs.GA_USE_BANDWIDTH_MULTIPLIER:
            maximum_path_cost.current_cost = maximum_path_cost.current_cost * max_load
        elif configs.GA_USE_MAX_BW_USAGE:
            maximum_path_cost.current_cost = max_load
        elif configs.GA_USE_SUM_BW_USAGE:
            maximum_path_cost.current_cost = sum_load
        elif configs.GA_USE_MAX_LINK_USAGE:
            maximum_path_cost.current_cost = link_load_max
        elif configs.GA_USE_SUM_LINK_USAGE:
            maximum_path_cost.current_cost = link_load_sum
        elif configs.GA_USE_END_TO_END_LINK_MAX:
            maximum_path_cost.current_cost = max_link_ct.current_cost

        if maximum_path_cost.current_cost == 0:
            raise Exception("maximum_path_cost cannot be zero at any point.")

        device_uplink_load, device_downlink_load = self.get_device_load_penalties(
            worker_link_loads
        )
        max_load_perc = get_max_percentage(device_uplink_load, device_downlink_load)

        return (
            (1.0 / maximum_path_cost.current_cost),
            max_load,
            worker_external_loads,
            max_load_perc,
        )

    def get_worker_probabilities(self, worker_usage: dict[str, int]) -> List[float]:
        probs: List[float] = []
        for _, wu in worker_usage.items():

            probs.append(1.0)
        probs_sum = sum(probs)

        if probs_sum == 0:
            raise Exception("Sum of probabilities are equal to zero!")

        for p_ix, p in enumerate(probs):
            probs[p_ix] = p / probs_sum
        return probs

    def get_valid_workers(
        self,
        worker_cpu_usage: dict[str, int],
        worker_external_loads: dict[str, float],
        worker_cpu_load: dict[str, float],
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]],
    ):
        device_uplink_loads, device_downlink_loads = self.get_device_load_penalties(
            worker_link_loads
        )

        valid_workers: List[str] = []
        for k, v in worker_cpu_usage.items():
            limit_reached: bool = (
                v > configs.DEVICE_ACTION_LIMIT + 1
                or (
                    k in device_uplink_loads
                    and configs.GA_USE_HARD_BW_LIMIT
                    and device_uplink_loads[k] > (configs.CUMULATIVE_LOAD_LIMIT / 2.0)
                )
                or (
                    k in device_downlink_loads
                    and configs.GA_USE_HARD_BW_LIMIT
                    and device_downlink_loads[k] > (configs.CUMULATIVE_LOAD_LIMIT / 2.0)
                )
                or worker_cpu_load[k] > configs.DEVICE_EXECUTION_LIMIT
            )

            if not limit_reached:
                valid_workers.append(k)

        if len(valid_workers) == 0:
            sorted_placements: dict[str, float] = {}
            for w_key, w_load in worker_external_loads.items():
                sorted_placements[w_key] = 0.0
            for w_key, w_load in device_downlink_loads.items():
                sorted_placements[w_key] = max(w_load, sorted_placements[w_key])
            for w_key, w_load in device_uplink_loads.items():
                sorted_placements[w_key] = max(w_load, sorted_placements[w_key])
            sorted_placements: dict[str, float] = dict(
                sorted(sorted_placements.items(), key=lambda item: item[1])
            )
            chosen: str = next(iter(sorted_placements))
            valid_workers.append(chosen)

        return valid_workers

    def get_valid_workers_from_zero(self, worker_cpu_usage: dict[str, int]):
        valid_workers: List[str] = []
        for k, v in worker_cpu_usage.items():
            if v < configs.DEVICE_ACTION_LIMIT:
                valid_workers.append(k)
        return valid_workers

    def traverse_event_node(
        self,
        node: TopologyNode,
        ct: PathCostTracker,
        encodings: dict[str, EncodingModel],
        worker_data_usage: dict[str, int],
        worker_cpu_usage: dict[str, int],
        maximum_path_cost: PathCostTracker,
        prev_step_name: str,
        worker_external_loads: dict[str, float],
        worker_external_loads_keeper: List[str],
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]],
        worker_cpu_load: dict[str, float],
    ):

        cep_task: CEPTask = node.node_data
        device_keys = list(self.workers.keys())

        encoding_exists = node.name in encodings
        new_step_encoding = EncodingModel(node.name, node.node_type)

        if not encoding_exists:
            prev_output_hosts = encodings[prev_step_name].get_output_targets()
            device_count = np.random.choice(range(1, len(prev_output_hosts) + 1))

            valid_device_ids: List[str] = self.get_valid_workers(
                worker_cpu_usage,
                worker_external_loads,
                worker_cpu_load,
                worker_link_loads,
            )
            valid_cpu_usages: dict[str, int] = {
                k: v for k, v in worker_cpu_usage.items() if k in valid_device_ids
            }

            probs = self.get_worker_probabilities(valid_cpu_usages)
            device_choices: List[str] = np.random.choice(
                valid_device_ids, size=device_count, p=probs, replace=False
            )
            execution_hosts: List[str] = []
            for dc in device_choices:
                added = new_step_encoding.add_execution_target(dc)
                if added:
                    execution_hosts.append(dc)
                    worker_cpu_usage[dc] += 1

            device_count = 1
            probs = self.get_worker_probabilities(worker_data_usage)
            device_choices: List[str] = np.random.choice(
                device_keys, size=device_count, p=probs, replace=False
            )

            if configs.GENETIC_ALGO_WRITE_AT_DEVICE:
                device_choices = execution_hosts

            output_hosts: List[str] = []
            for dc in device_choices:
                added = new_step_encoding.add_output_target(dc)
                if added:
                    output_hosts.append(dc)
                    worker_data_usage[dc] += 1
        else:
            execution_hosts: List[str] = encodings[node.name].get_execution_targets()
            output_hosts: List[str] = encodings[node.name].get_output_targets()

            new_step_encoding.execution_targets = execution_hosts
            new_step_encoding.output_targets = output_hosts

        new_step_encoding.register_output_topic(
            cep_task.settings.output_topic.output_topic
        )

        if (
            self.last_best_encoding
            and node.name in self.last_best_encoding[node.name].get_output_targets()
        ):
            prev_output_target_hosts = self.last_best_encoding[
                node.name
            ].get_output_targets()
        else:
            prev_output_target_hosts = output_hosts

        execution_cost = self.mssa.event_execution_get_avg_ns_over_byte(node.name, True)

        if configs.GA_COST_TYPE == 0:
            local_output_cost = self.mssa.event_output_get_avg_ns_over_byte(
                node.name, False
            )
            remote_output_cost = self.mssa.event_output_get_avg_ns_over_byte(
                node.name, True
            )
        elif configs.GA_COST_TYPE == 1:
            local_output_cost = self.mssa.event_output_get_avg_byte_mul_ns_square(
                node.name, False
            )
            remote_output_cost = self.mssa.event_output_get_avg_byte_mul_ns_square(
                node.name, True
            )
        else:
            raise Exception("Invalid GA_COST_TYPE")

        local_reading_cost = 0.0
        remote_reading_cost = 0.0

        total_step_cost: float = 0.0
        prev_output_hosts: List[str] = encodings[prev_step_name].get_output_targets()
        prev_output_topic: str = encodings[prev_step_name].get_output_topic()
        prev_output_host: str = prev_output_hosts[0]
        execution_host: str = execution_hosts[0]

        if not configs.GA_EXLCUDE_EXECUTION_COST:
            total_step_cost += execution_cost

        for rti in cep_task.settings.required_sub_tasks:
            if rti.input_topic == prev_output_topic:
                if configs.GA_COST_TYPE == 0:
                    remote_reading_cost += self.mssa.topic_get_avg_ns_over_byte(
                        node.name, rti.input_topic, True
                    )
                    local_reading_cost += self.mssa.topic_get_avg_ns_over_byte(
                        node.name, rti.input_topic, False
                    )
                elif configs.GA_COST_TYPE == 1:
                    remote_reading_cost += self.mssa.topic_get_avg_byte_mul_ns_square(
                        node.name, rti.input_topic, True
                    )
                    local_reading_cost += self.mssa.topic_get_avg_byte_mul_ns_square(
                        node.name, rti.input_topic, False
                    )
                else:
                    raise Exception("Invalid GA_COST_TYPE")

                if execution_host != prev_output_host:
                    total_step_cost += remote_reading_cost

                    load_id: str = cep_task.settings.action_name + ":" + rti.input_topic
                    if load_id not in worker_external_loads_keeper:
                        worker_external_loads_keeper.append(load_id)
                        if execution_host not in worker_external_loads:
                            worker_external_loads[execution_host] = 0.0
                        if prev_output_host not in worker_external_loads:
                            worker_external_loads[prev_output_host] = 0.0

                        if configs.GA_BANDWIDTH_COST_TYPE == 0:
                            worker_external_loads[execution_host] += remote_reading_cost
                            worker_external_loads[
                                prev_output_host
                            ] += remote_reading_cost
                        elif configs.GA_BANDWIDTH_COST_TYPE == 1:

                            estimated_input_load: float = (
                                self.mssa.get_expected_topic_load_per_second(
                                    cep_task.settings.action_name,
                                    rti.input_topic,
                                    False,
                                )
                            )

                            if self.last_best_encoding:
                                previous_execution_locations: List[str] = (
                                    self.last_best_encoding[
                                        node.name
                                    ].get_execution_targets()
                                )
                                if (
                                    configs.GA_USE_OSCILLATION_PENALTY
                                    and execution_host
                                    not in previous_execution_locations
                                ):
                                    estimated_input_load = (
                                        estimated_input_load
                                        * configs.GA_OSCILLATION_PENALTY
                                    )

                            worker_external_loads[
                                execution_host
                            ] += estimated_input_load
                            worker_external_loads[
                                prev_output_host
                            ] += estimated_input_load

                            cpu_load: float = self.mssa.get_expected_event_input_counts(
                                cep_task.settings.action_name, rti.input_topic
                            )
                            worker_cpu_load[execution_host] += cpu_load

                            self.add_to_worker_link_loads(
                                worker_link_loads,
                                cep_task.settings.action_name,
                                rti.input_topic,
                                execution_host,
                                prev_output_host,
                                estimated_input_load,
                            )
                        else:
                            raise Exception("Invalid GA Cost type!!!!")
                else:
                    total_step_cost += local_reading_cost

                if (
                    (not configs.GA_EXLCUDE_EXECUTION_COST and execution_cost == 0)
                    or local_reading_cost == 0
                    or remote_reading_cost == 0
                    or local_output_cost == 0
                    or remote_output_cost == 0
                ):
                    raise Exception("Cost cannot be zero at any point.")

        for dc in output_hosts:
            if dc not in execution_hosts:
                if dc not in prev_output_target_hosts:
                    total_step_cost += remote_output_cost * self.migration_penalty
                else:
                    total_step_cost += remote_output_cost
            else:
                if dc not in prev_output_target_hosts:
                    total_step_cost += local_output_cost * self.migration_penalty
                else:
                    total_step_cost += local_output_cost

        new_step_encoding.update_step_cost(total_step_cost)
        encodings[node.name] = new_step_encoding

        if total_step_cost == 0:
            raise Exception("Cost cannot be 0!")

        ct.add_cost(total_step_cost)

        successors: List[TopologyNode] = self.topology.get_nodes_from_out_topic(
            cep_task.settings.output_topic.output_topic
        )

        for succ in successors:
            if succ.node_type == NodeType.EVENTEXECUTION:
                self.traverse_event_node(
                    succ,
                    ct,
                    encodings,
                    worker_data_usage,
                    worker_cpu_usage,
                    maximum_path_cost,
                    node.name,
                    worker_external_loads,
                    worker_external_loads_keeper,
                    worker_link_loads,
                    worker_cpu_load,
                )
            elif succ.node_type == NodeType.CONSUMER:
                self.traverse_consumer_node(
                    succ,
                    ct,
                    encodings,
                    maximum_path_cost,
                    node.name,
                    worker_external_loads,
                    worker_external_loads_keeper,
                    worker_link_loads,
                    worker_cpu_load,
                )

        ct.subtract_cost(total_step_cost)

    def traverse_raw_load(
        self,
        node: TopologyNode,
        ct: PathCostTracker,
        encodings: dict[str, EncodingModel],
        worker_data_usage: dict[str, int],
        worker_cpu_usage: dict[str, int],
        maximum_path_cost: PathCostTracker,
        device_uplink_load: dict[str, float],
        device_downlink_load: dict[str, float],
        worker_cpu_load: dict[str, float],
    ) -> None:

        data: RawSettings = node.node_data

        successors: List[TopologyNode] = self.topology.get_nodes_from_out_topic(
            data.output_topic.output_topic
        )

        for succ in successors:
            self.traverse_event_load(
                succ,
                ct,
                encodings,
                worker_data_usage,
                worker_cpu_usage,
                maximum_path_cost,
                node.name,
                device_uplink_load,
                device_downlink_load,
                worker_cpu_load,
            )

    def traverse_event_load(
        self,
        node: TopologyNode,
        ct: PathCostTracker,
        encodings: dict[str, EncodingModel],
        worker_data_usage: dict[str, int],
        worker_cpu_usage: dict[str, int],
        maximum_path_cost: PathCostTracker,
        prev_step_name: str,
        device_uplink_load: dict[str, float],
        device_downlink_load: dict[str, float],
        worker_cpu_load: dict[str, float],
    ) -> None:

        cep_task: CEPTask = node.node_data
        step_cost: float = 0.0

        prev_output_hosts: List[str] = encodings[prev_step_name].get_output_targets()
        prev_output_topic: str = encodings[prev_step_name].get_output_topic()
        prev_output_host: str = prev_output_hosts[0]
        execution_hosts: List[str] = encodings[node.name].get_execution_targets()
        execution_host: str = execution_hosts[0]

        for rti in cep_task.settings.required_sub_tasks:
            if rti.input_topic == prev_output_topic:
                if execution_host != prev_output_host:
                    u_device_load: float = device_uplink_load[prev_output_host] / (
                        configs.CUMULATIVE_LOAD_LIMIT / 2.0
                    )
                    v_device_load: float = device_downlink_load[execution_host] / (
                        configs.CUMULATIVE_LOAD_LIMIT / 2.0
                    )

                    if device_uplink_load[prev_output_host] > (
                        configs.CUMULATIVE_LOAD_LIMIT / 2.0
                    ):
                        u_device_load = u_device_load * 1000000.0
                    elif device_uplink_load[prev_output_host] > (
                        configs.CUMULATIVE_LOAD_LIMIT * configs.CP_SAT_BW_FIRST_TH / 2.0
                    ):
                        u_device_load = u_device_load * configs.CP_SAT_ABOVE_BW_MULTP
                    if device_downlink_load[execution_host] > (
                        configs.CUMULATIVE_LOAD_LIMIT / 2.0
                    ):
                        v_device_load = v_device_load * 1000000.0
                    elif device_downlink_load[execution_host] > (
                        configs.CUMULATIVE_LOAD_LIMIT * configs.CP_SAT_BW_FIRST_TH / 2.0
                    ):
                        u_device_load = u_device_load * configs.CP_SAT_ABOVE_BW_MULTP

                    if (
                        worker_cpu_load[prev_output_host]
                        > configs.DEVICE_EXECUTION_LIMIT
                    ):
                        u_device_load = u_device_load * 1000000.0

                    if worker_cpu_load[execution_host] > configs.DEVICE_EXECUTION_LIMIT:
                        v_device_load = v_device_load * 1000000.0

                    max_load = max(u_device_load, v_device_load)

                    step_cost += max_load

        ct.add_cost(step_cost)

        successors: List[TopologyNode] = self.topology.get_nodes_from_out_topic(
            cep_task.settings.output_topic.output_topic
        )

        for succ in successors:
            if succ.node_type == NodeType.EVENTEXECUTION:
                self.traverse_event_load(
                    succ,
                    ct,
                    encodings,
                    worker_data_usage,
                    worker_cpu_usage,
                    maximum_path_cost,
                    node.name,
                    device_uplink_load,
                    device_downlink_load,
                    worker_cpu_load,
                )
            elif succ.node_type == NodeType.CONSUMER:
                self.traverse_consumer_load(
                    succ,
                    ct,
                    encodings,
                    maximum_path_cost,
                    node.name,
                    device_uplink_load,
                    device_downlink_load,
                    worker_cpu_load,
                )

        ct.subtract_cost(step_cost)

    def traverse_consumer_load(
        self,
        node: TopologyNode,
        ct: PathCostTracker,
        encodings: dict[str, EncodingModel],
        maximum_path_cost: PathCostTracker,
        prev_step_name: str,
        device_uplink_load: dict[str, float],
        device_downlink_load: dict[str, float],
        worker_cpu_load: dict[str, float],
    ) -> None:
        cons_data: CEPConsumerSettings = node.node_data
        prev_output_hosts: List[str] = encodings[prev_step_name].get_output_targets()

        current_cost: float = 0.0

        for poh in prev_output_hosts:
            if poh != cons_data.host_name:
                u_device_load: float = device_uplink_load[poh] / (
                    configs.CUMULATIVE_LOAD_LIMIT / 2.0
                )
                v_device_load: float = device_downlink_load[cons_data.host_name] / (
                    configs.CUMULATIVE_LOAD_LIMIT / 2.0
                )

                if device_uplink_load[poh] > (configs.CUMULATIVE_LOAD_LIMIT / 2.0):
                    u_device_load = u_device_load * 1000000.0
                elif device_uplink_load[poh] > (
                    configs.CUMULATIVE_LOAD_LIMIT * configs.CP_SAT_BW_FIRST_TH / 2.0
                ):
                    u_device_load = u_device_load * configs.CP_SAT_ABOVE_BW_MULTP
                if device_downlink_load[cons_data.host_name] > (
                    configs.CUMULATIVE_LOAD_LIMIT / 2.0
                ):
                    v_device_load = v_device_load * 1000000.0
                elif device_downlink_load[cons_data.host_name] > (
                    configs.CUMULATIVE_LOAD_LIMIT * configs.CP_SAT_BW_FIRST_TH / 2.0
                ):
                    v_device_load = v_device_load * configs.CP_SAT_ABOVE_BW_MULTP

                if worker_cpu_load[poh] > configs.DEVICE_EXECUTION_LIMIT:
                    u_device_load = u_device_load * 1000000.0

                if (
                    worker_cpu_load[cons_data.host_name]
                    > configs.DEVICE_EXECUTION_LIMIT
                ):
                    v_device_load = v_device_load * 1000000.0

                max_load = max(u_device_load, v_device_load)

                current_cost += max_load

        ct.add_cost(current_cost)

        maximum_path_cost.current_cost = (
            ct.current_cost
            if ct.current_cost > maximum_path_cost.current_cost
            else maximum_path_cost.current_cost
        )

        ct.subtract_cost(current_cost)

    def traverse_raw_node(
        self,
        node: TopologyNode,
        ct: PathCostTracker,
        encodings: dict[str, EncodingModel],
        worker_data_usage: dict[str, int],
        worker_cpu_usage: dict[str, int],
        maximum_path_cost: PathCostTracker,
        worker_external_loads: dict[str, float],
        worker_external_loads_keeper: List[str],
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]],
        worker_cpu_load: dict[str, float],
    ):

        data: RawSettings = node.node_data
        device_keys = list(self.workers.keys())

        encoding_exists = node.name in encodings

        new_step_encoding = EncodingModel(node.name, node.node_type)

        if not encoding_exists:

            device_count = 1
            probs = self.get_worker_probabilities(worker_data_usage)
            device_choices: List[str] = np.random.choice(
                device_keys, size=device_count, p=probs, replace=False
            )

            if configs.KEEP_RAWDATA_AT_SOURCE:
                device_choices = [data.producer_name]

            raw_storage_hosts: List[str] = []
            for dc in device_choices:
                added = new_step_encoding.add_output_target(dc)
                if added:
                    raw_storage_hosts.append(dc)
                    worker_data_usage[dc] += 1
            new_step_encoding.register_output_topic(data.output_topic.output_topic)
        else:

            raw_storage_hosts: List[str] = encodings[node.name].get_output_targets()
            new_step_encoding.output_targets = raw_storage_hosts
            new_step_encoding.register_output_topic(data.output_topic.output_topic)

        if (
            self.last_best_encoding
            and node.name in self.last_best_encoding[node.name].get_output_targets()
        ):
            prev_output_target_hosts = self.last_best_encoding[
                node.name
            ].get_output_targets()
        else:
            prev_output_target_hosts = raw_storage_hosts

        if configs.GA_COST_TYPE == 0:
            raw_local_cost = self.rawstats.get_avg_ns_over_byte(
                data.producer_name, data.output_topic.output_topic, False
            )
            raw_remote_cost = self.rawstats.get_avg_ns_over_byte(
                data.producer_name, data.output_topic.output_topic, True
            )
        elif configs.GA_COST_TYPE == 1:
            raw_local_cost = self.rawstats.get_avg_byte_mul_ns_square(
                data.producer_name, data.output_topic.output_topic, False
            )
            raw_remote_cost = self.rawstats.get_avg_byte_mul_ns_square(
                data.producer_name, data.output_topic.output_topic, True
            )

        elif configs.GA_COST_TYPE == 2:
            raw_local_cost = 0.0
            raw_remote_cost = 0.0
        else:
            raise Exception("Invalid configs.GA_COST_TYPE")

        if raw_local_cost == 0 or raw_remote_cost == 0:
            raise Exception("Cost cannot be zero at any point.")

        total_step_cost: float = 0.0
        for dc in raw_storage_hosts:
            if data.producer_name != dc:
                if dc not in prev_output_target_hosts:
                    total_step_cost += raw_remote_cost * self.migration_penalty
                else:
                    total_step_cost += raw_remote_cost
            else:
                if dc not in prev_output_target_hosts:
                    total_step_cost += raw_local_cost * self.migration_penalty
                else:
                    total_step_cost += raw_local_cost

        new_step_encoding.update_step_cost(total_step_cost)
        encodings[node.name] = new_step_encoding

        step_cost = total_step_cost

        if step_cost == 0:
            raise Exception("Cost cannot be 0!")

        ct.add_cost(step_cost)

        cpu_load = self.rawstats.get_expected_event_count(
            data.producer_name, data.output_topic.output_topic
        )
        worker_cpu_load[data.producer_name] += cpu_load

        successors: List[TopologyNode] = self.topology.get_nodes_from_out_topic(
            data.output_topic.output_topic
        )

        for succ in successors:
            self.traverse_event_node(
                succ,
                ct,
                encodings,
                worker_data_usage,
                worker_cpu_usage,
                maximum_path_cost,
                node.name,
                worker_external_loads,
                worker_external_loads_keeper,
                worker_link_loads,
                worker_cpu_load,
            )

        ct.subtract_cost(step_cost)

    def add_to_worker_link_loads(
        self,
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]],
        event_name: str,
        topic_name: str,
        source: str,
        dest: str,
        load: float,
    ) -> None:
        if event_name not in worker_link_loads:
            worker_link_loads[event_name] = {}

        if topic_name not in worker_link_loads[event_name]:
            worker_link_loads[event_name][topic_name] = {}

        if source not in worker_link_loads[event_name][topic_name]:
            worker_link_loads[event_name][topic_name][source] = {}

        if dest not in worker_link_loads[event_name][topic_name][source]:
            worker_link_loads[event_name][topic_name][source][dest] = load

    def traverse_consumer_node(
        self,
        node: TopologyNode,
        ct: PathCostTracker,
        encodings: dict[str, EncodingModel],
        maximum_path_cost: PathCostTracker,
        prev_step_name: str,
        worker_external_loads: dict[str, float],
        worker_external_loads_keeper: List[str],
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]],
        worker_cpu_load: dict[str, float],
    ) -> None:
        total_step_cost: float = 0.0
        new_step_encoding = EncodingModel(node.name, node.node_type)
        cons_data: CEPConsumerSettings = node.node_data

        prev_output_hosts = encodings[prev_step_name].get_output_targets()

        if len(prev_output_hosts) == 0:
            raise Exception("prev_output_hosts cannot be empty")

        if configs.GA_COST_TYPE == 0:
            low_reading_cost = self.consumerstats.get_reading_ns_over_byte(
                cons_data.host_name, cons_data.topic_name, False
            )
            high_reading_cost = self.consumerstats.get_reading_ns_over_byte(
                cons_data.host_name, cons_data.topic_name, True
            )
        elif configs.GA_COST_TYPE == 1:
            low_reading_cost = self.consumerstats.get_reading_byte_mul_ns_square(
                cons_data.host_name, cons_data.topic_name, False
            )
            high_reading_cost = self.consumerstats.get_reading_byte_mul_ns_square(
                cons_data.host_name, cons_data.topic_name, True
            )
        else:
            raise Exception("Invalid GA_COST_TYPE")

        for poh in prev_output_hosts:
            if poh != cons_data.host_name:
                total_step_cost += high_reading_cost

                load_id: str = cons_data.host_name + ":" + cons_data.topic_name
                if load_id not in worker_external_loads_keeper:
                    worker_external_loads_keeper.append(load_id)
                    if cons_data.host_name not in worker_external_loads:
                        worker_external_loads[cons_data.host_name] = 0.0
                    if poh not in worker_external_loads:
                        worker_external_loads[poh] = 0.0

                    if configs.GA_BANDWIDTH_COST_TYPE == 0:
                        worker_external_loads[cons_data.host_name] += high_reading_cost
                        worker_external_loads[poh] += high_reading_cost
                    elif configs.GA_BANDWIDTH_COST_TYPE == 1:

                        estimated_load: float = (
                            self.consumerstats.get_expected_load_per_second(
                                cons_data.host_name, cons_data.topic_name, False
                            )
                        )
                        worker_external_loads[cons_data.host_name] += estimated_load
                        worker_external_loads[poh] += estimated_load

                        cpu_load: float = self.consumerstats.get_expected_event_counts(
                            cons_data.host_name, cons_data.topic_name
                        )
                        worker_cpu_load[cons_data.host_name] += cpu_load

                        self.add_to_worker_link_loads(
                            worker_link_loads,
                            cons_data.host_name,
                            cons_data.topic_name,
                            cons_data.host_name,
                            poh,
                            estimated_load,
                        )
                    else:
                        raise Exception("Invalid GA Cost type!!!!")
            else:
                total_step_cost += low_reading_cost

        new_step_encoding.update_step_cost(total_step_cost)
        encodings[node.name] = new_step_encoding

        if total_step_cost == 0:
            raise Exception("Cost cannot be 0!")

        ct.add_cost(total_step_cost)

        maximum_path_cost.current_cost = (
            ct.current_cost
            if ct.current_cost > maximum_path_cost.current_cost
            else maximum_path_cost.current_cost
        )

        ct.subtract_cost(total_step_cost)

    def get_device_load_penalties(
        self, worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]]
    ) -> Tuple[dict[str, float], dict[str, float]]:
        device_uplink_load: dict[str, float] = {}
        device_downlink_load: dict[str, float] = {}

        device_keys = list(self.workers.keys())
        for w_id in device_keys:
            device_uplink_load[w_id] = 0.0
            device_downlink_load[w_id] = 0.0

        for _, event_topics in worker_link_loads.items():
            for _, topic_source_devices in event_topics.items():
                for dst_device, dst_devices in topic_source_devices.items():
                    for src_device, link_load_value in dst_devices.items():
                        if src_device != dst_device:
                            device_uplink_load[src_device] += link_load_value
                            device_downlink_load[dst_device] += link_load_value

        return device_uplink_load, device_downlink_load

    def get_external_load_penalty(
        self,
        worker_external_loads: dict[str, float],
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]],
        worker_cpu_load: dict[str, float],
    ) -> Tuple[float, float, float, float]:
        device_uplink_load: dict[str, float] = {}
        device_downlink_load: dict[str, float] = {}

        device_keys = list(self.workers.keys())
        for w_id in device_keys:
            device_uplink_load[w_id] = 0.0
            device_downlink_load[w_id] = 0.0

        for _, event_topics in worker_link_loads.items():
            for _, topic_source_devices in event_topics.items():
                for dst_device, dst_devices in topic_source_devices.items():
                    for src_device, link_load_value in dst_devices.items():
                        if src_device != dst_device:
                            device_uplink_load[src_device] += link_load_value
                            device_downlink_load[dst_device] += link_load_value

        max_val: float = 1.0
        sum_val: float = 0.0
        for d_id, d_load in device_uplink_load.items():
            crr_d_load: float = d_load
            if d_load > (configs.CUMULATIVE_LOAD_LIMIT / 2.0):
                crr_d_load = crr_d_load * 1000000000.0
            max_val = max(max_val, crr_d_load)
            sum_val += crr_d_load

        for d_id, d_load in device_downlink_load.items():
            crr_d_load: float = d_load
            if d_load > (configs.CUMULATIVE_LOAD_LIMIT / 2.0):
                crr_d_load = crr_d_load * 1000000000.0
            max_val = max(max_val, crr_d_load)
            sum_val += crr_d_load

        link_load_sum: float = 0.0
        link_load_max: float = 0.0
        for _, event_topics in worker_link_loads.items():
            for _, topic_source_devices in event_topics.items():
                for dst_device, dst_devices in topic_source_devices.items():
                    for src_device, link_load_value in dst_devices.items():
                        u_load_rate: float = link_load_value / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )
                        v_load_rate: float = link_load_value / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )

                        u_device_load: float = device_uplink_load[src_device] / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )
                        v_device_load: float = device_downlink_load[dst_device] / (
                            configs.CUMULATIVE_LOAD_LIMIT / 2.0
                        )

                        if u_device_load > 0.5:
                            u_load_rate = u_load_rate * 1.25
                        elif u_device_load > 0.75:
                            u_load_rate = u_load_rate * 2.0
                        elif u_device_load > 1.0:
                            u_load_rate = u_load_rate * 10000.0

                        if v_device_load > 0.5:
                            v_load_rate = v_load_rate * 1.25
                        elif v_device_load > 0.75:
                            v_load_rate = v_load_rate * 2.0
                        elif v_device_load > 1.0:
                            v_load_rate = v_load_rate * 10000.0

                        if configs.GA_USE_UP_DOWN_LINK_COST_ADVANCED:
                            u_load_rate = u_device_load
                            v_load_rate = v_device_load

                            if device_uplink_load[src_device] > (
                                configs.CUMULATIVE_LOAD_LIMIT / 2.0
                            ):
                                u_load_rate = u_load_rate * 1000000.0
                            elif device_uplink_load[src_device] > (
                                configs.CUMULATIVE_LOAD_LIMIT
                                * configs.CP_SAT_BW_FIRST_TH
                                / 2.0
                            ):
                                u_load_rate = (
                                    u_load_rate * configs.CP_SAT_ABOVE_BW_MULTP
                                )
                            if device_downlink_load[dst_device] > (
                                configs.CUMULATIVE_LOAD_LIMIT / 2.0
                            ):
                                v_load_rate = v_load_rate * 1000000.0
                            elif device_downlink_load[dst_device] > (
                                configs.CUMULATIVE_LOAD_LIMIT
                                * configs.CP_SAT_BW_FIRST_TH
                                / 2.0
                            ):
                                v_load_rate = (
                                    v_load_rate * configs.CP_SAT_ABOVE_BW_MULTP
                                )

                        if worker_cpu_load[src_device] > configs.DEVICE_EXECUTION_LIMIT:
                            u_load_rate = u_load_rate * 1000000.0

                        if worker_cpu_load[dst_device] > configs.DEVICE_EXECUTION_LIMIT:
                            v_load_rate = v_load_rate * 1000000.0

                        max_rate: float = max(u_load_rate, v_load_rate)

                        link_load_max = max(link_load_max, max_rate)
                        link_load_sum += max_rate

        return sum_val, max_val, link_load_sum, link_load_max

    def generate_citizen(
        self,
    ) -> Tuple[dict[str, EncodingModel], float, float, dict[str, float], float]:

        device_keys = list(self.workers.keys())
        worker_data_usage: dict[str, int] = {}
        worker_cpu_usage: dict[str, int] = {}
        worker_external_loads: dict[str, float] = {}
        worker_external_loads_keeper: List[str] = []
        worker_link_loads: dict[str, dict[str, dict[str, dict[str, float]]]] = {}
        worker_cpu_load: dict[str, float] = {}

        for w in device_keys:
            worker_data_usage[w] = 1
            worker_cpu_usage[w] = 1
            worker_cpu_load[w] = 0.0

        step_encodings: dict[str, EncodingModel] = {}
        maximum_path_cost = PathCostTracker()
        raw_nodes = self.topology.get_producer_nodes()
        ct = PathCostTracker()

        for raw_n in raw_nodes:

            self.traverse_raw_node(
                raw_n,
                ct,
                step_encodings,
                worker_data_usage,
                worker_cpu_usage,
                maximum_path_cost,
                worker_external_loads,
                worker_external_loads_keeper,
                worker_link_loads,
                worker_cpu_load,
            )

        link_ct = PathCostTracker()
        max_link_ct = PathCostTracker()
        if configs.GA_USE_END_TO_END_LINK_MAX:
            max_load = 0.0
            tr_device_uplink_loads, tr_device_downlink_loads = (
                self.get_device_load_penalties(worker_link_loads)
            )
            for raw_n in raw_nodes:
                self.traverse_raw_load(
                    raw_n,
                    link_ct,
                    step_encodings,
                    {},
                    {},
                    max_link_ct,
                    tr_device_uplink_loads,
                    tr_device_downlink_loads,
                    worker_cpu_load,
                )
        else:

            sum_load, max_load, link_load_sum, link_load_max = (
                self.get_external_load_penalty(
                    worker_external_loads, worker_link_loads, worker_cpu_load
                )
            )

        if configs.GA_USE_BANDWIDTH_MULTIPLIER:
            maximum_path_cost.current_cost = maximum_path_cost.current_cost * max_load
        elif configs.GA_USE_MAX_BW_USAGE:
            maximum_path_cost.current_cost = max_load
        elif configs.GA_USE_SUM_BW_USAGE:
            maximum_path_cost.current_cost = sum_load
        elif configs.GA_USE_MAX_LINK_USAGE:
            maximum_path_cost.current_cost = link_load_max
        elif configs.GA_USE_SUM_LINK_USAGE:
            maximum_path_cost.current_cost = link_load_sum
        elif configs.GA_USE_END_TO_END_LINK_MAX:
            maximum_path_cost.current_cost = max_link_ct.current_cost

        if maximum_path_cost.current_cost == 0:
            raise Exception("maximum_path_cost cannot be zero at any point.")

        device_uplink_load, device_downlink_load = self.get_device_load_penalties(
            worker_link_loads
        )
        max_load_perc = get_max_percentage(device_uplink_load, device_downlink_load)

        return (
            step_encodings,
            (1.0 / maximum_path_cost.current_cost),
            max_load,
            worker_external_loads,
            max_load_perc,
        )

    def get_final_distribution(self, encoding) -> tuple[
        List[TaskAlterationModel],
        List[RawUpdateEvent],
        List[CEPConsumerUpdateEvent],
        dict[str, EncodingModel],
        float,
    ]:

        step_targets: dict[str, EncodingModel]
        total_cost: float
        step_targets, total_cost, bw_cost, worker_external_loads, max_load_perc = (
            encoding
        )

        print("================================")
        print(f"bw limit: {configs.CUMULATIVE_LOAD_LIMIT}")
        print("Final device loads:")
        for f_id, f_load in worker_external_loads.items():
            print(f"device id: {f_id}, device load: {f_load}")

        print("================================")

        print("Arranging the GA assignments...")

        producer_updates: List[RawUpdateEvent] = []
        alterations: List[TaskAlterationModel] = []
        consumer_alterations: List[CEPConsumerUpdateEvent] = []

        workers = deepcopy(self.workers)

        n_tasks_activated = 0
        n_tasks_deactivated = 0
        n_consumer_requests = 0

        topic_targets: dict[str, List[str]] = {}

        step: TopologyNode
        for step in self.topology.get_topological_ordered_nodes():
            if step.node_type in (NodeType.HEAD, NodeType.SINK):
                continue
            if step.node_type == NodeType.RAWOUTPUT:
                print("----------------")
                print(f"Processing node: {step.name}")
                current_step_targets = step_targets[step.name].get_output_targets()

                producer: RawSettings = step.node_data
                producer_target_db_update = RawUpdateEvent()
                producer_target_db_update.producer_name = producer.producer_name
                producer_target_db_update.output_topic = producer.output_topic
                producer_target_db_update.raw_name = producer.raw_data_name
                producer_updates.append(producer_target_db_update)

                topic_list = []
                producer.output_topic.target_databases = current_step_targets
                topic_list.append(producer.output_topic.output_topic)
                print(
                    f"Raw source {step.name} will write: {producer.output_topic.output_topic} its output to: {current_step_targets}"
                )

                if producer.output_topic.output_topic not in topic_targets:
                    topic_targets[producer.output_topic.output_topic] = []
                for c in current_step_targets:
                    if c not in topic_targets[producer.output_topic.output_topic]:
                        topic_targets[producer.output_topic.output_topic].append(c)

                successors = self.topology.get_successors(step)

                if len(successors) == 0:
                    raise Exception(
                        "All raw data sources are required to have successors as events!"
                    )

                print(f"Topics: {topic_list} are written to: {current_step_targets}")

            if step.node_type == NodeType.EVENTEXECUTION:
                print("----------------")
                print(f"Processing node: {step.name}")

                execution_targets = step_targets[step.name].get_execution_targets()
                event_output_targets = step_targets[step.name].get_output_targets()

                print(
                    f"Event {step.name} will be executed at : {execution_targets}, will write its outputs to: {event_output_targets}"
                )

                if len(execution_targets) != 1:
                    raise Exception("Invalid number of execution devices detected!")

                task: CEPTask = deepcopy(step.node_data)

                for exec in execution_targets:
                    task: CEPTask = deepcopy(step.node_data)
                    task.settings.host_name = exec
                    task.settings.output_topic.target_databases = event_output_targets

                    if configs.GENETIC_ALGO_WRITE_AT_DEVICE:
                        if exec != event_output_targets[0]:
                            raise Exception("GENETIC_ALGO_WRITE_AT_DEVICE is invalid!")

                    number_of_reads: int = 0
                    for rit in task.settings.required_sub_tasks:
                        rit.subscription_topics = topic_targets[rit.input_topic]
                        number_of_reads += len(rit.subscription_topics)

                    if number_of_reads == 0:
                        raise Exception(
                            "Execution cannot have 0 input sources assigned to it!"
                        )

                    if task.settings.output_topic.output_topic not in topic_targets:
                        topic_targets[task.settings.output_topic.output_topic] = []
                    for c in event_output_targets:
                        if (
                            c
                            not in topic_targets[
                                task.settings.output_topic.output_topic
                            ]
                        ):
                            topic_targets[
                                task.settings.output_topic.output_topic
                            ].append(c)

                    alteration = TaskAlterationModel()
                    alteration.host = exec
                    alteration.activate = True
                    alteration.job_name = task.settings.action_name
                    alteration.cep_task = task
                    alteration.migration_requests = []
                    alteration.only_migration = False
                    alterations.append(alteration)
                    n_tasks_activated += 1

                for host in [*workers]:
                    if host not in execution_targets:

                        migration_request = DataMigrationModel()
                        migration_request.topic = (
                            task.settings.output_topic.output_topic
                        )
                        migration_request.current_host = host
                        migration_request.target_host = task.settings.host_name

                        deactivation = TaskAlterationModel()
                        deactivation.host = host
                        deactivation.activate = False
                        deactivation.job_name = task.settings.action_name
                        deactivation.migration_requests = []
                        deactivation.only_migration = False
                        alterations.append(deactivation)
                        n_tasks_deactivated += 1

            if step.node_type == NodeType.CONSUMER:
                cons_et: CEPConsumerSettings = step.node_data
                alteration = CEPConsumerUpdateEvent()
                alteration.host = cons_et.host_name
                alteration.topic_name = cons_et.topic_name
                alteration.topics = topic_targets[cons_et.topic_name]

                if len(alteration.topics) == 0:
                    raise Exception("Invalid consumer assignment detected!")

                consumer_alterations.append(alteration)
                n_consumer_requests += 1

                print(
                    f"Assigning consumer: {cons_et.host_name}:{cons_et.topic_name}, to: {alteration.topics}"
                )

        print(
            "Number of tasks activated is: ",
            n_tasks_activated,
            " deactivated: ",
            n_tasks_deactivated,
            " consumer: ",
            n_consumer_requests,
        )

        print(f"Distribution finished with final cost: {total_cost}")
        return (
            alterations,
            producer_updates,
            consumer_alterations,
            step_targets,
            max_load_perc,
        )


def genetic_algorithm(
    topology: Topology,
    producer_records: dict[str, dict[str, RawSettings]],
    workers: dict[str, dict[str, int | float]],
    event_consumers: dict[str, dict[str, CEPConsumerSettings]],
    cep_tasks: List[CEPTask],
    rawstats: RawServerStatAnalyzer,
    mssa: ManagementServerStatisticsAnalyzer,
    consumerstats: ConsumerServerStatics,
    last_best_encoding: dict,
) -> tuple[
    List[TaskAlterationModel],
    List[RawUpdateEvent],
    List[CEPConsumerUpdateEvent],
    dict[str, EncodingModel],
]:
    print("Running Genetic algorithm distribution...")
    ga = CEPGeneticAlgorithm(
        topology,
        producer_records,
        workers,
        event_consumers,
        cep_tasks,
        rawstats,
        mssa,
        consumerstats,
        last_best_encoding,
    )

    return ga.solve()
