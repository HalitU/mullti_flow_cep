# does a redundant check if any of the incoming values are true
import time

from cep_library import configs


def vehicle_density_task(data):        
    res = True
    if "laser_counter_sensor" in data:
        for d in data["laser_counter_sensor"]:
            res = res or (d < 50.0)      
    
    if "speed_task" in data:
        for d in data["speed_task"]:
            res = res or d
    
    if "vehicle_type_detection_task" in data:
        for d in data["vehicle_type_detection_task"]:
            res = res or "ambulance" in d["vehicles"]
    
    if "common_object_detection_task" in data:
        for d in data["common_object_detection_task"]:
            for ot in d["object.types"]:
                res = res or ot == "human"
    
    if "audio_detection_task" in data:
        for d in data["audio_detection_task"]:
            for ot in d["object.types"]:
                res = res or ot == "human"
    
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
    
    return {"vehicle_density_task": [res], "load":artifical_load}, res, False, 0
