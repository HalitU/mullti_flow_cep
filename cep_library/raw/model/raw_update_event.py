from cep_library.cep.model.cep_task import RequiredOutputTopics


class RawUpdateEvent:
    def __init__(self) -> None:
        self.db_host: str
        self.db_port: int
        self.producer_name: str
        self.output_topic: RequiredOutputTopics
        self.raw_name: str
