from datetime import datetime, timedelta, timezone
from typing import Callable, List

from cep_library import configs


class EventModel:
    def __init__(self):
        self.event_date: datetime = datetime.now(tz=timezone.utc)
        self.raw_date: datetime = None
        self.data_id: str = ""
        self.vsm: str = ""
        self.data = None
        self.raw_data_tracker: str = ""


class RequiredInputTopics:
    def __init__(self):
        self.order: int
        self.input_topic: str
        self.from_database: bool
        self.type: type
        self.subscription_topics: List[str]
        self.is_image_read: bool


class RequiredOutputTopics:
    def __init__(self):
        self.order: int
        self.output_topic: str
        self.to_database: bool = True
        self.type: type
        self.target_databases: List[str]
        self.target_hosts: List[str] = []


class MongoQuery:
    def __init__(self):
        self.query: dict = {}
        self.columns: dict = {}
        self.sort = []
        self.within_timedelta: timedelta
        self.limit: int = configs.MIN_REQUIRED_ACTIVE_VAR
        self.aggregate = []


class CEPTaskSetting:
    def __init__(
        self,
        flow_id: int,
        query: MongoQuery,
        output_topic: RequiredOutputTopics,
        required_sub_tasks: List[RequiredInputTopics],
        action_name: str,
        action_path: str,
        arguments=[],
        created_date=datetime.now(tz=timezone.utc),
        updated_date=None,
        output_enabled: bool = True,
        delete_after_consume: bool = False,
        host_name: str = "",
    ) -> None:
        self.flow_id: int = flow_id
        self.action_name: str = action_name
        self.output_topic: RequiredOutputTopics = output_topic
        self.arguments = arguments
        self.created_date: datetime = created_date
        self.updated_date: datetime | None = updated_date
        self.required_sub_tasks: List[RequiredInputTopics] = required_sub_tasks
        self.output_enabled: bool = output_enabled
        self.delete_after_consume: bool = delete_after_consume
        self.query: MongoQuery = query
        self.action_path: str = action_path
        self.host_name: str = host_name


class CEPTask:
    def __init__(self, cep_task_setting: CEPTaskSetting) -> None:
        self.task: Callable
        self.settings: CEPTaskSetting = cep_task_setting
