from typing import List
from cep_library.cep.model.cep_task import CEPTask


class CPSATV3EventInfo:
    def __init__(self, event_id: str, historical_data, cep_task: CEPTask) -> None:
        self.event_id: str = event_id
        self.historical_data = historical_data
        self.cep_task: CEPTask = cep_task

    def get_key(self):
        return self.event_id

    def topic_exists(self, topic) -> bool:
        return topic in [
            rs.input_topic for rs in self.cep_task.settings.required_sub_tasks
        ]

    def output_topic_exists(self, topic) -> bool:
        return topic == self.cep_task.settings.output_topic.output_topic

    def get_output_topic_currently_written_devices(self):
        if not self.historical_data:
            return []
        current_written_devices = []
        for _, event_details in self.historical_data.items():
            worker_written_device_topics = event_details["output_topics"]
            for _, written_devices in worker_written_device_topics.items():
                for wwd in written_devices:
                    if wwd not in current_written_devices:
                        current_written_devices.append(wwd)
        return current_written_devices

    def get_input_topic_locations(self, input_topic_name: str):
        if not self.historical_data:
            return []
        current_read_devices = []
        for _, event_details in self.historical_data.items():
            worker_topic_read_devices: List[str] = event_details["input_topics"][
                input_topic_name
            ]
            for wwd in worker_topic_read_devices:
                if wwd not in current_read_devices:
                    current_read_devices.append(wwd)
        return current_read_devices

    def get_output_location_input_keys(self):
        if not self.historical_data:
            return {}

        output_input_list: dict[str, List[str]] = {}
        for _, event_details in self.historical_data.items():

            worker_written_device_topics = event_details["output_topics"]
            for _, worker_written_devices in worker_written_device_topics.items():
                for wwd in worker_written_devices:
                    if wwd not in output_input_list:
                        output_input_list[wwd] = []

                    worker_read_device_topics = event_details["input_topics"]
                    for _, worker_read_devices in worker_read_device_topics.items():
                        for wrd in worker_read_devices:
                            if wrd not in output_input_list[wwd]:
                                output_input_list[wwd].append(wrd)

        output_input_keys: dict[str, List[str]] = {}
        for oik, oik_list in output_input_list.items():
            sorted_list = sorted(oik_list)
            sorted_list_key = "-".join(sorted_list)
            if sorted_list_key not in output_input_keys:
                output_input_keys[sorted_list_key] = []
            output_input_keys[sorted_list_key].append(oik)

        if len(list(output_input_keys.keys())) == 0:
            raise Exception(
                "Number of output input keys cannot be zero if historical data exists for this event!!!"
            )

        return output_input_keys

    def get_currently_executing_devices(self) -> list[str]:
        if not self.historical_data:
            return []
        currently_executing_devices: list[str] = []
        for worker_id, _ in self.historical_data.items():
            if worker_id not in currently_executing_devices:
                currently_executing_devices.append(worker_id)
        return currently_executing_devices

    def get_devices_that_write_to_specific_location(self, output_target):
        if not self.historical_data:
            return []
        currently_executing_devices = []
        for worker_id, event_details in self.historical_data.items():
            worker_written_device_topics = event_details["output_topics"]
            for _, worker_written_devices in worker_written_device_topics.items():
                if (
                    output_target in worker_written_devices
                    and worker_id not in currently_executing_devices
                ):
                    currently_executing_devices.append(worker_id)
        return currently_executing_devices
