from typing import List

class CEPConsumerUpdateEvent:
    def __init__(self) -> None:
        self.topic_name:str
        self.host:str
        self.topics:List[str]
