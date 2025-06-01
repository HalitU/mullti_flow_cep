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

class SmartTrafficFlow:
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
        manager.register_task_for_management(self.audio_detection_task())
        manager.register_task_for_management(self.civilian_density_task())
        manager.register_task_for_management(self.common_object_detection_task())
        manager.register_task_for_management(self.rule_violation_task())
        manager.register_task_for_management(self.speed_task())
        manager.register_task_for_management(self.traffic_alarm_task())
        manager.register_task_for_management(self.traffic_congestion_task())
        manager.register_task_for_management(self.vehicle_density_task())
        manager.register_task_for_management(self.vehicle_type_alarm_task())
        manager.register_task_for_management(self.vehicle_type_detection_task())

    def audio_detection_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.audio_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.audio_detection_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=2,
            action_name="audio_detection_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/audio_detection_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)
        
    def civilian_density_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.audio_detection_task"
        rit_one.from_database = True
        rit_one.is_image_read = False

        rit_two = RequiredInputTopics()
        rit_two.input_topic = "event.common_object_detection_task"
        rit_two.from_database = True
        rit_two.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.civilian_density_task"
        out_one.to_database = True
        
        cep_task_setting = CEPTaskSetting(
            flow_id=2,
            action_name="civilian_density_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/civilian_density_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one, rit_two],
            query=self.get_mongo_query()
        )        
        return CEPTask(cep_task_setting=cep_task_setting)
        
    def common_object_detection_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.camera_feed_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.common_object_detection_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=2,
            action_name="common_object_detection_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/common_object_detection_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)

    def rule_violation_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.vehicle_type_detection_task"
        rit_one.from_database = True
        rit_one.is_image_read = False

        rit_two = RequiredInputTopics()
        rit_two.input_topic = "event.speed_task"
        rit_two.from_database = True
        rit_two.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.rule_violation_task"
        out_one.to_database = True
        
        cep_task_setting = CEPTaskSetting(
            flow_id=2,
            action_name="rule_violation_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/rule_violation_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one, rit_two],
            query=self.get_mongo_query()
        )        
        return CEPTask(cep_task_setting=cep_task_setting)
        
    def speed_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.speed_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.speed_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=2,
            action_name="speed_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/speed_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)

    def traffic_alarm_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.vehicle_density_task"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.traffic_alarm_task"
        out_one.to_database = True
        
        cep_task_setting = CEPTaskSetting(
            flow_id=2,
            action_name="traffic_alarm_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/traffic_alarm_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )        
        return CEPTask(cep_task_setting=cep_task_setting)
        
    def traffic_congestion_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.vehicle_density_task"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.traffic_congestion_task"
        out_one.to_database = True
        
        cep_task_setting = CEPTaskSetting(
            flow_id=2,
            action_name="traffic_congestion_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/traffic_congestion_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )        
        return CEPTask(cep_task_setting=cep_task_setting)
        
    def vehicle_density_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.laser_counter_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False

        rit_two = RequiredInputTopics()
        rit_two.input_topic = "event.speed_task"
        rit_two.from_database = True
        rit_two.is_image_read = False
                
        rit_five = RequiredInputTopics()
        rit_five.input_topic = "event.audio_detection_task"
        rit_five.from_database = True
        rit_five.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.vehicle_density_task"
        out_one.to_database = True
        
        cep_task_setting = CEPTaskSetting(
            flow_id=2,
            action_name="vehicle_density_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/vehicle_density_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one, rit_two, rit_five],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_task_setting)
        
    def vehicle_type_alarm_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "event.vehicle_type_detection_task"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.vehicle_type_alarm_task"
        out_one.to_database = True
        
        cep_task_setting = CEPTaskSetting(
            flow_id=2,
            action_name="vehicle_type_alarm_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/vehicle_type_alarm_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )        
        return CEPTask(cep_task_setting=cep_task_setting)
        
    def vehicle_type_detection_task(self) -> CEPTask:
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "raw.camera_feed_sensor"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        out_one = RequiredOutputTopics()
        out_one.output_topic = "event.vehicle_type_detection_task"
        out_one.to_database = True
        
        cep_action_settings = CEPTaskSetting(
            flow_id=2,
            action_name="vehicle_type_detection_task",
            action_path="vanet_apps/cep/smart_traffic_scenario/scripts/vehicle_type_detection_task",
            output_enabled=True,
            output_topic=out_one,
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query()
        )
        return CEPTask(cep_task_setting=cep_action_settings)

