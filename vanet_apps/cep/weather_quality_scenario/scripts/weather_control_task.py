# does a redundant check if any of the incoming values are true
import time
from cep_library import configs


def weather_control_task(data):        
    res = True
    if "humidity_sensor" in data:
        for d in data["humidity_sensor"]:
            res = res or (d < 50.0)

    if "wind_sensor" in data:
        for d in data["wind_sensor"]:
            res = res or (d < 50.0)
            
    if "temperature_data_sensor" in data:
        for d in data["temperature_data_sensor"]:
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
            artifical_load = bytearray(2 * configs.ARTIFICAL_TASK_LOAD_MULTIPLIER)
        else:
            artifical_load = bytearray(configs.ARTIFICAL_TASK_LOAD)

    return {"weather_control_task": [res], "load":artifical_load}, res, False, 0
