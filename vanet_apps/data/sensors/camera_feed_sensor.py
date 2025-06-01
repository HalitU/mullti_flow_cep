import os
import time

from fastapi import FastAPI
from cep_library.data import data_helper
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer
import cv2
from cep_library import configs
from vanet_apps.data.base.base_data_sensor import BaseDataSensor
from vanet_apps.data.dataconfigs.sensor_data_pattern import SensorDataParameters


class CameraFeedSensor(BaseDataSensor):
    def __init__(self, 
                 sdp:SensorDataParameters,
                 producer:RawDataProducer, 
                 app:FastAPI, 
                 settings:RawSettings
                 ) -> None:

        print(f"[CameraDataSensor] Initializing with params: {sdp.msg_per_delay}, {sdp.default_delay_s}")
        super().__init__(app, settings, sdp)
        self.producer = producer
                
        #region Vid
        filename = os.environ['VIDEO_PATH']
        # clip = VideoFileClip(filename) 
        # frame = clip.get_frame(0)
        
        # read single frame
        clip = cv2.VideoCapture(filename) # type: ignore
        ret, frame = clip.read()

        # grayscaled_frame = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
        
        scale_percent = configs.RAW_IMAGE_SCALE_PERCENT # percent of original size
        width = int(frame.shape[1] * scale_percent / 100)
        height = int(frame.shape[0] * scale_percent / 100)
        dim = (width, height)
        
        # resize image
        self.resized = cv2.resize(frame, dim, interpolation = cv2.INTER_AREA)     # type: ignore
        self.resized = cv2.cvtColor(self.resized, cv2.COLOR_RGB2GRAY) # type: ignore
        
        #endregion Vid        
        print(f"[CameraDataSensor] Intialized")

    def run(self):    
        print(f"Activating the {self.settings.raw_data_name} data")
        # return
        
        staticvid_size = data_helper.estimate_size_kbytes(self.resized)
        staticvid_noise = [1] * int(staticvid_size/(8*4))
        
        print("Resized image shapes...")
        print(self.resized.shape)
        print(staticvid_size)        
        
        print(f"Starting to send {self.settings.raw_data_name} sensor data...")
        
        msg_count=0
        sim_time = 0.0
        
        crr_time:float = 0.0
        count_per_delay:int
        delayS:float
        alarming:bool
        stop:bool
        count_per_delay, delayS, alarming, stop = self.get_next_step_production(crr_time)
        
        while not stop:
            count_per_delay = count_per_delay * configs.MULTIPLY_VIDEO_DATA_PRODUCTION_COUNT
            
            # Do this M times for this step
            for _ in range(count_per_delay):
                if self.killed: 
                    print(f"Stopping to send {self.settings.raw_data_name} sensor data...")
                    return True        
                
                # print("Sending frame number: ", msg_count)
                data = dict()

                # Determine if sim time is between an alarming window
                is_important_vehicle = True
                
                # Arrange the alarming data if necessary
                if configs.scenario_case == 2 and configs.server_query_type == 8:
                    data[self.settings.raw_data_name] = {"image": bytearray(1000), "importance": is_important_vehicle}
                elif configs.scenario_case == 2 and configs.server_query_type == 9:
                    data[self.settings.raw_data_name] = {"image": bytearray(250), "importance": is_important_vehicle}
                elif alarming:
                    data[self.settings.raw_data_name] = {"image": self.resized, "importance": is_important_vehicle}
                else:
                    data[self.settings.raw_data_name] = {"image": self.resized, "importance": is_important_vehicle}
                
                self.producer.send(data, raw_data_tracker="camera_feed_data")
                msg_count += 1
                crr_sleep_duration = (delayS * 1.0) / count_per_delay
                                
                self.print_raw_output_stats()
                
                sim_time += crr_sleep_duration 
                time.sleep(crr_sleep_duration)

            # Next step
            crr_time += delayS
            count_per_delay, delayS, alarming, stop = self.get_next_step_production(crr_time)   

        print(f"Stopping to send {self.settings.raw_data_name} sensor data...")
        return msg_count
