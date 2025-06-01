import time
from typing import List
import cv2
import numpy as np

from cep_library import configs

# initialize the HOG descriptor
# hog = cv2.HOGDescriptor()
# hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
face_cascade = cv2.CascadeClassifier('vanet_apps/data/sample/haarcascade_smile.xml') # type: ignore

def coinFlip(p):
    return np.random.binomial(1, p)

# does redundant bool checks and applies a redundant cascade object detection classifier
def common_object_detection_task(data):
    res:List[str] = []
    object_exists = True
    if "camera_feed_sensor" in data:
        n_img=len(data["camera_feed_sensor"])
        
        for im in data["camera_feed_sensor"]:
            # (humans, _) = hog.detectMultiScale(im, winStride=(1, 1), padding=(5, 5), scale=1.1)
            face_cascade.detectMultiScale(im["image"])
            if im["importance"] == True:
                res.append("ambulance")
                object_exists = True
            else:
                res.append("car")

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

    data_priority = 1 if object_exists else 0
    return {"common_object_detection_task": {"object.types": ["cat", "dog"], "load":artifical_load}}, object_exists, False, data_priority
