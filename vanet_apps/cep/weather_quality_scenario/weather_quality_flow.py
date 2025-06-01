from datetime import timedelta
import pymongo
from cep_library.cep.model.cep_task import (
    CEPTask,
    CEPTaskSetting,
    MongoQuery,
    RequiredInputTopics,
    RequiredOutputTopics,
)
from cep_library import configs
from cep_library.management.server.management_server import ManagementServer

class WeatherQualityFlow:
    def __init__(self) -> None:
        pass

    def get_mongo_query(self, read_timedelta:int = configs.query_with_timedelta, limit:int = configs.MIN_REQUIRED_ACTIVE_VAR):
        mongo_query = MongoQuery()
        mongo_query.columns = {"_id": 1, "data": 1, "initDateMin": 1, "initDateMax": 1}
        mongo_query.sort = [("cep_priority", pymongo.DESCENDING), ("createdAt", pymongo.DESCENDING)]
        # library uses createdAt gte for given timedelta
        mongo_query.query = {}
        mongo_query.within_timedelta = timedelta(
            milliseconds=read_timedelta
        )
        mongo_query.limit = limit
        return mongo_query

    def register_tasks(self, manager: ManagementServer):
        manager.register_task_for_management(self.weather_control_task())
        manager.register_task_for_management(self.pollution_detection_task())
        manager.register_task_for_management(self.health_alarm_task())
        manager.register_task_for_management(self.air_quality_task())
       

    def weather_control_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.humidity_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "raw.wind_sensor"
        rit_two.from_database = True
        rit_two.is_image_read = False
        
        rit_three = RequiredInputTopics()
        rit_three.input_topic = "raw.temperature_data_sensor"
        rit_three.from_database = True
        rit_three.is_image_read = False                
        
        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.weather_control_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=1,
            action_name="weather_control_task",
            action_path="vanet_apps/cep/weather_quality_scenario/scripts/weather_control_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one,rit_two,rit_three],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)

    def health_alarm_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.pollution_detection_task"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "event.weather_control_task"
        rit_two.from_database = True
        rit_two.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.health_alarm_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=1,
            action_name="health_alarm_task",
            action_path="vanet_apps/cep/weather_quality_scenario/scripts/health_alarm_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one,rit_two],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def pollution_detection_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.pollutant_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.pollution_detection_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=1,
            action_name="pollution_detection_task",
            action_path="vanet_apps/cep/weather_quality_scenario/scripts/pollution_detection_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)          

    def air_quality_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.pollution_detection_task"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "event.weather_control_task"
        rit_two.from_database = True
        rit_two.is_image_read = False

        rit_three = RequiredInputTopics()
        rit_three.input_topic = "raw.noise_sensor"
        rit_three.from_database = True
        rit_three.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.air_quality_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=1,
            action_name="air_quality_task",
            action_path="vanet_apps/cep/weather_quality_scenario/scripts/air_quality_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one,rit_two,rit_three],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)         

