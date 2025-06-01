from fastapi import FastAPI
from cep_library.cep.model.cep_task import RequiredOutputTopics
from cep_library.data.database_management_service import DatabaseManagementService
from cep_library.raw.model.raw_settings import RawSettings
from cep_library import configs
from cep_library.raw.raw_data_producer import RawDataProducer
from vanet_apps.data.base.base_data_sensor import BaseDataSensor
from vanet_apps.data.dataconfigs.sensor_data_pattern import SensorDataParameters

from vanet_apps.data.sensors.audio_sensor import AudioSensor
from vanet_apps.data.sensors.camera_feed_sensor import CameraFeedSensor
from vanet_apps.data.sensors.humidity_sensor import HumiditySensor
from vanet_apps.data.sensors.laser_counter_sensor import LaserCounterSensor
from vanet_apps.data.sensors.noise_sensor import NoiseSensor
from vanet_apps.data.sensors.pollutant_sensor import PollutantSensor
from vanet_apps.data.sensors.speed_sensor import SpeedSensor
from vanet_apps.data.sensors.temperature_data_sensor import TemperatureDataSensor
from vanet_apps.data.sensors.wind_sensor import WindSensor


class DataSensorFactory:
    def __init__(self, db: DatabaseManagementService, app: FastAPI) -> None:
        self.db: DatabaseManagementService = db
        self.app: FastAPI = app
        print("[RawProducerFactory] Initialized")

    def get_sensor(
        self,
        stp: SensorDataParameters
    ) -> RawDataProducer:
        out_topic = RequiredOutputTopics()
        out_topic.output_topic = stp.get_topic()
        out_topic.target_databases = [str(configs.env_host_name)]

        temperature_raw_settings = RawSettings(
            raw_data_name=stp.get_data_name(),
            producer_name=configs.env_host_name,
            producer_port=configs.env_host_port,
            host_x=configs.env_host_x,
            host_y=configs.env_host_y,
            consumer_group=f"producer_{configs.env_host_name}_{out_topic.output_topic}",
            output_topic=out_topic
        )

        return RawDataProducer(temperature_raw_settings, db=self.db, app=self.app)

    def get_sensor_producer(self, 
                            sdp:SensorDataParameters, 
                            producer:RawDataProducer, 
                            app:FastAPI, 
                            settings:RawSettings) -> BaseDataSensor:

        print(f"[RawProducerFactory] Getting a sensor with params: length: {sdp.get_default_duration()}, per_moment: {sdp.get_default_count_per_delay()}, sleep_duration:{sdp.get_default_duration()}")

        if sdp.raw_type == "audio_sensor":
            return AudioSensor(sdp, producer, app, settings)

        if sdp.raw_type == "camera_feed_sensor":
            return CameraFeedSensor(sdp, producer, app, settings)

        if sdp.raw_type == "humidity_sensor":
            return HumiditySensor(sdp, producer, app, settings)

        if sdp.raw_type == "laser_counter_sensor":
            return LaserCounterSensor(sdp, producer, app, settings)

        if sdp.raw_type == "noise_sensor":
            return NoiseSensor(sdp, producer, app, settings)

        if sdp.raw_type == "pollutant_sensor":
            return PollutantSensor(sdp, producer, app, settings)
        
        if sdp.raw_type == "speed_sensor":
            return SpeedSensor(sdp, producer, app, settings)
        
        if sdp.raw_type == "temperature_data_sensor":
            return TemperatureDataSensor(sdp, producer, app, settings)
        
        if sdp.raw_type == "wind_sensor":
            return WindSensor(sdp, producer, app, settings)

        raise Exception("Invalid sensor type detected!")
