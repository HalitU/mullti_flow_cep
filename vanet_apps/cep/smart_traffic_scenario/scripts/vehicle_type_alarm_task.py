# does a redundant check if any of the incoming values are true
import time

from cep_library import configs


def vehicle_type_alarm_task(data):        
    res = True
    if "vehicle_type_detection_task" in data:
        for d in data["vehicle_type_detection_task"]:
            if "ambulance" in d["vehicles"]:
                res = True

    # sleep
    if configs.ENABLE_ACTION_ARTIFICAL_DELAY:
        time.sleep(configs.ACTION_ARTIFICAL_DELAY)

    if not configs.ENABLE_ARTIFICAL_TASK_LOAD:
        artifical_load= {}
    else:
        if configs.ARTIFICAL_TASK_LOAD_TYPE == 1:
            artifical_load = data
        elif configs.ARTIFICAL_TASK_LOAD_TYPE == 2:
            artifical_load = bytearray(2 * configs.ARTIFICAL_TASK_LOAD_MULTIPLIER)
        else:
            artifical_load = bytearray(configs.ARTIFICAL_TASK_LOAD)

    return {"vehicle_type_alarm_task": {"alarm_exists": res, "load":artifical_load}}, res, False, 0
