from cep_library import configs


def get_estimated_data_delay(load: float) -> float:
    return load / configs.CUMULATIVE_LOAD_LIMIT


def get_max_percentage(
    uplink_loads: dict[str, float], downlink_loads: dict[str, float]
) -> float:
    max_load_percentage: float = 0.0
    for _, load in uplink_loads.items():
        perc: float = load / (configs.CUMULATIVE_LOAD_LIMIT / 2.0)
        max_load_percentage = max(max_load_percentage, perc)

    for _, load in downlink_loads.items():
        perc: float = load / (configs.CUMULATIVE_LOAD_LIMIT / 2.0)
        max_load_percentage = max(max_load_percentage, perc)

    return max_load_percentage * 100.0
