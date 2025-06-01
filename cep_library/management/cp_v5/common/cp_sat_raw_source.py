from typing import List
from cep_library.raw.model.raw_settings import RawSettings, RawStatisticsBook


class CPSATV3RawSource:
    def __init__(
        self,
        device_id: str,
        output_topic: str,
        currently_stored_devices: List[str],
        settings: RawSettings,
        flow_id: int,
    ) -> None:
        self.device_id = device_id
        self.output_topic = output_topic
        self.currently_stored_devices = currently_stored_devices
        self.settings = settings
        self.flow_id = flow_id

    def get_key(self):
        return self.device_id + "_" + self.output_topic

    def topic_related(self, topic: str):
        return self.settings.output_topic.output_topic == topic
