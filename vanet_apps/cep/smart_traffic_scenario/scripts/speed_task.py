# does a redundant check if any of the incoming values are true
import time

from cep_library import configs


def speed_task(data):        
    res = True
    if "speed_sensor" in data:
        for d in data["speed_sensor"]:
            res = res or (d < 50.0)
    
    # sleep
    if configs.ENABLE_ACTION_ARTIFICAL_DELAY:
        time.sleep(configs.ACTION_ARTIFICAL_DELAY)
    
    if not configs.ENABLE_ARTIFICAL_TASK_LOAD:
        artifical_load= {}
    else:
        if configs.ARTIFICAL_TASK_LOAD_TYPE == 1:
            artifical_load = data
        elif configs.ARTIFICAL_TASK_LOAD_TYPE == 2:
            artifical_load = bytearray(3 * configs.ARTIFICAL_TASK_LOAD_MULTIPLIER)
        else:
            artifical_load = bytearray(configs.ARTIFICAL_TASK_LOAD)
    
    return {"speed_task": [res], "load":artifical_load}, res, False, 0
