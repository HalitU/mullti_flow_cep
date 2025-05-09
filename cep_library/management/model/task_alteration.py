from typing import List
from cep_library.cep.model.cep_task import CEPTask

class DataMigrationModel:
    def __init__(self):
        self.topic: str
        self.target_host: str
        self.current_host: str
        self.data_type: int
        self.writer_host: str
        self.event_name: str

class TaskAlterationModel:
    def __init__(self):
        self.host: str
        self.job_name: str
        self.activate: bool
        self.cep_task: CEPTask
        self.migration_requests: List[DataMigrationModel]
        self.only_migration = False

class TaskAlterationBulk:
    def __init__(self) -> None:
        self.host: str
        self.activations: List[TaskAlterationModel]
        self.deactivations: List[TaskAlterationModel]
