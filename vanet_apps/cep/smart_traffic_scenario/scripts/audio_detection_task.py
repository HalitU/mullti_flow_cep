# import librosa

import time

from cep_library import configs


def audio_detection_task(data):
    res = True
    if "audio_sensor" in data:
        for d in data["audio_sensor"]:
            audio_sample = d["audio"]

            # amplitude and corresponding decibels
            # X = librosa.stft(audio_sample, n_fft=220)
            # Xdb = librosa.amplitude_to_db(abs(X))
    
            Xdb = [1, 2, 3, 4, 5]

            for _ in Xdb:
                # if db[0] >= 30 or db[1] >= 30:
                pass
            
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
            artifical_load = bytearray(3 * configs.ARTIFICAL_TASK_LOAD_MULTIPLIER)
        else:
            artifical_load = bytearray(configs.ARTIFICAL_TASK_LOAD)

    return {"audio_detection_task": {"object.types": ["cat", "dog", "human", "truck"], "load":artifical_load}}, res, False, 0
