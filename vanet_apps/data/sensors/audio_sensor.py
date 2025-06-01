import os
import time
import librosa

from fastapi import FastAPI
from cep_library import configs
from cep_library.raw.model.raw_settings import RawSettings
from cep_library.raw.raw_data_producer import RawDataProducer

from vanet_apps.data.base.base_data_sensor import BaseDataSensor
from vanet_apps.data.dataconfigs.sensor_data_pattern import SensorDataParameters

class AudioSensor(BaseDataSensor):
    def __init__(self, 
                 sdp:SensorDataParameters,
                 producer:RawDataProducer, 
                 app:FastAPI, 
                 settings:RawSettings
                 ) -> None:
        print(f"[AudioSensor] Initializing with params: {sdp.msg_per_delay}, {sdp.default_delay_s}")
        super().__init__(app, settings, sdp)
        self.producer = producer
        
        #region Audio
        sound_file=os.environ['AUDIO_PATH']
        self.audio_sample, self.sampling_rate = self.process_sample_audio_file(sound_file, 0.01, 0.01)
        #endregion Audio
        
        print(f"[AudioSensor] Intialized")
        
    def run(self):    
        print(f"Starting {self.settings.raw_data_name} sampling")
        msg_count = 0
        
        crr_time:float = 0.0
        count_per_delay:int
        delayS:float
        alarming:bool
        stop:bool
        count_per_delay, delayS, alarming, stop = self.get_next_step_production(crr_time)
        
        while not stop:
            count_per_delay = count_per_delay * configs.MULTIPLY_AUDIO_DATA_PRODUCTION_COUNT
            # Do this M times for this step
            for _ in range(count_per_delay):
                if self.killed: 
                    print(f"Stopping {self.settings.raw_data_name} sampling") 
                    return True        
                
                data = dict()
                # Determine if sim time is between an alarming window
                important_event = True
                
                # Arrange the alarming data if necessary
                if configs.scenario_case == 2 and configs.server_query_type == 8:
                    data[self.settings.raw_data_name] = {
                        "audio": bytearray(1000),
                        "sampling_rate": None,
                        "importance": important_event
                        }
                elif configs.scenario_case == 2 and configs.server_query_type == 9:
                    data[self.settings.raw_data_name] = {
                        "audio": bytearray(250),
                        "sampling_rate": None,
                        "importance": important_event
                        }                    
                elif alarming:
                    data[self.settings.raw_data_name] = {
                        "audio": self.audio_sample,
                        "sampling_rate": self.sampling_rate,
                        "importance": important_event
                        }
                else:
                    data[self.settings.raw_data_name] = {
                        "audio": self.audio_sample,
                        "sampling_rate": self.sampling_rate,
                        "importance": important_event
                        }

                self.producer.send(data, raw_data_tracker="audio_data")
                
                msg_count += 1
                crr_sleep_duration = (delayS * 1.0) / count_per_delay
                
                self.print_raw_output_stats()                
                
                time.sleep(crr_sleep_duration)
            
            # Next step
            crr_time += delayS
            count_per_delay, delayS, alarming, stop = self.get_next_step_production(crr_time)            
            
        print(f"Stopping {self.settings.raw_data_name} sampling")
        return msg_count

    def process_sample_audio_file(self, filename:str, offset:float, duration:float):
        # filename = librosa.example('nutcracker')
            
        # 2. Load the audio as a waveform `y`
        #    Store the sampling rate as `sr`
        #   sr means that there will be sr amount of samples for one second!
        y, sr = librosa.load(filename, offset=offset, duration=duration, mono=True, sr=None)
        return y, sr
            
       
        # getting a single sample
                
        # plt.figure(figsize=(14, 5))
        # librosa.display.waveshow(y, sr=sr)
        # plt.show()
        
        # Checking the loudness of the signal, amplitude
        # X = librosa.stft(y)
        # Xdb = librosa.amplitude_to_db(abs(X))
        # plt.figure(figsize=(14, 5))
        # librosa.display.specshow(Xdb, sr=sr, x_axis='time', y_axis='hz')
        # plt.colorbar()
        # plt.show()
        
        # print(sr)
        # print(y)
        # print(len(y))
        # print(len(Xdb))

        # 3. Run the default beat tracker
        # tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr)

        # print('Estimated tempo: {:.2f} beats per minute'.format(tempo))

        # 4. Convert the frame indices of beat events into timestamps
        # beat_times = librosa.frames_to_time(beat_frames, sr=sr)

    def process_sample_audio_file_test(self):
        # 2. Load the audio as a waveform `y`
        #    Store the sampling rate as `sr`
        #   sr means that there will be sr amount of samples for one second!
        filename = "cep_library/example/data_processing/audio/data/BabyElephantWalk60.wav"
        offset = 0.0
        duration = 60.0
        y, sr = librosa.load(filename, offset=offset, duration=duration)

                  
        # getting a single sample
                
        # plt.figure(figsize=(14, 5))
        # librosa.display.waveshow(y, sr=sr)
        # plt.show()
        
        # Checking the loudness of the signal, amplitude
        X = librosa.stft(y)
        Xdb = librosa.amplitude_to_db(abs(X))
        print(Xdb)
        # plt.figure(figsize=(14, 5))
        # librosa.display.specshow(Xdb, sr=sr, x_axis='time', y_axis='hz')
        # plt.colorbar()
        # plt.show()
