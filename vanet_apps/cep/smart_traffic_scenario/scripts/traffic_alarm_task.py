# does a redundant check if any of the incoming values are true
import time

from cep_library import configs


def traffic_alarm_task(data):        
    res = True
    if "vehicle_density_task" in data:
        for d in data["vehicle_density_task"]:
            res = res or d

    if "traffic_congestion_task" in data:
        for d in data["traffic_congestion_task"]:
            res = res or d

    # sleep
    if configs.ENABLE_ACTION_ARTIFICAL_DELAY:
        time.sleep(configs.ACTION_ARTIFICAL_DELAY)

    if not configs.ENABLE_ARTIFICAL_TASK_LOAD:
        artifical_load= {}
    else:
        if configs.ARTIFICAL_TASK_LOAD_TYPE == 1:
            artifical_load = data
        elif configs.ARTIFICAL_TASK_LOAD_TYPE == 2:
            artifical_load = bytearray(1 * configs.ARTIFICAL_TASK_LOAD_MULTIPLIER)
        else:
            artifical_load = bytearray(configs.ARTIFICAL_TASK_LOAD)

    return {"traffic_alarm_task": {"alarm_exists": res, "load":artifical_load}}, res, False, 0
