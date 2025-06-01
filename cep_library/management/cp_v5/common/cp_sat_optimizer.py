from ortools.sat.python import cp_model

from cep_library import configs


class CPSATV3Optimizer:
    def __init__(self, model: cp_model.CpModel) -> None:
        self.model = model

    def optimize(self):

        print("Running the optimization...")

        def cp_logger(log):
            if configs.CP_SAT_LOG:
                print(log)

        solution_found = False
        solver = cp_model.CpSolver()
        solver.parameters.num_workers = configs.CP_SAT_WORKER_COUNT
        solver.parameters.max_time_in_seconds = configs.CP_RUNTIME_SECONDS
        solver.parameters.relative_gap_limit = configs.CP_SAT_ABSOLUTE_GAP
        solver.parameters.cp_model_presolve = configs.CP_SAT_PRESOLVE
        solver.parameters.debug_crash_on_bad_hint = True
        solver.parameters.fix_variables_to_their_hinted_value = True

        if configs.CP_SAT_PRESOLVE and configs.CP_SAT_PRESOLVE_ITERATIONS > 0:
            solver.parameters.max_presolve_iterations = (
                configs.CP_SAT_PRESOLVE_ITERATIONS
            )

        solver.parameters.log_search_progress = configs.CP_SAT_LOG

        solver.log_callback = cp_logger
        status = solver.Solve(self.model)
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            print(f"Status OPTIMAL: {status == cp_model.OPTIMAL}")
            print(f"Status FEASIBLE: {status == cp_model.FEASIBLE}")
            print(f"Solution found: {solver.ObjectiveValue()}")
            solution_found = True
        else:
            print(f"Status INFEASIBLE: {status == cp_model.INFEASIBLE}")
            print(f"Status MODEL_INVALID: {status == cp_model.MODEL_INVALID}")
            print(f"Status UNKNOWN: {status == cp_model.UNKNOWN}")
            print("No solution found.")

        if status == cp_model.INFEASIBLE:
            print(f"{solver.SufficientAssumptionsForInfeasibility()}")

        if solution_found == False:
            raise Exception("No solution found.")
        else:
            print(f"Optimized solution is: {solver.ObjectiveValue()}")

        return solver, solution_found
