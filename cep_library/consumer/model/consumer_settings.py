from typing import List
from cep_library.cep.model.cep_task import MongoQuery


class CEPConsumerSettings:
    def __init__(
        self,
        host_name: str,
        host_port: str,
        topic_name: str,
        query: MongoQuery,
        host_x: int,
        host_y: int,
        is_image_read: bool = False,
    ) -> None:
        self.host_name: str = host_name
        self.host_port: str = host_port
        self.topic_name: str = topic_name
        self.host_x: int = host_x
        self.host_y: int = host_y
        self.query: MongoQuery = query
        self.is_image_read: bool = is_image_read
        self.source_topics: List[str] = []
