import datetime
import pickle
from threading import Lock, Thread
import time
from typing import Any
from fastapi import FastAPI, Response
import requests

from cep_library import configs
from cep_library.cep.model.cep_task import EventModel
from cep_library.consumer.model.consumer_settings import CEPConsumerSettings
from cep_library.consumer.model.consumer_topic_stats import ConsumerTopicStats
from cep_library.consumer.model.consumer_update_event import CEPConsumerUpdateEvent
from cep_library.data import data_helper
from cep_library.data.database_management_service import DatabaseManagementService
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.stats_helper.mean_stat_base import MeanStatBase


class CEPEventConsumer:
    def __init__(
        self,
        app: FastAPI,
        topic_name: str,
        action,
        settings: CEPConsumerSettings,
        db: DatabaseManagementService,
    ) -> None:
        self.app: FastAPI = app
        self.topic_name: str = topic_name
        self.action = action
        self.killed = False
        self.settings: CEPConsumerSettings = settings
        self.db: DatabaseManagementService = db
        self.initialized = False

        self.stat_lock: Lock = Lock()
        self.consumer_stats = ConsumerTopicStats(
            self.settings.host_name, self.settings.topic_name
        )

        self.register_action_file_service(app)
        self.send_heartbeat()

        if configs.MQTT_USE_PREFIX_FOR_EVENTS:
            fixed_topic_name: str = f"{topic_name}_{self.settings.host_name}"
        else:
            fixed_topic_name: str = topic_name

        self.message_consumer = MQTTConsumer(
            client_id=f"{configs.env_host_name}_{self.topic_name}_alarm_consumer",
            core_topic=self.topic_name,
            topic_names=[fixed_topic_name],
            target=self.trigger_action,
            target_args=None,
            shared_subscription=False,
        )

        self.alteration_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_consumer_topics[0]}_{settings.host_name}_{settings.topic_name}_alarm_consumer",
            core_topic=configs.kafka_consumer_topics[0],
            topic_names=configs.kafka_consumer_topics,
            target=self.trigger_alteration,
            target_args=None,
            shared_subscription=False,
        )

    def run(self):
        Thread(daemon=True, target=self.message_consumer.run).start()
        Thread(daemon=True, target=self.alteration_consumer.run).start()
        if configs.HEARTBEATS_ACTIVE:
            Thread(daemon=True, target=self.heartbeat).start()
        self.initialized = True

    def stop(self):
        print("STOPPING THE ALARM CONSUMER!!!")
        self.message_consumer.close()
        self.alteration_consumer.close()
        self.killed = True

    def register_action_file_service(self, app: FastAPI):
        @app.get("/event_consumer/" + self.topic_name + "/")
        def event_consumerraw_statistics_report():

            self.stat_lock.acquire()

            usage = self.consumer_stats

            collected_data_count = (
                usage.local_read_data_count.count + usage.remote_read_data_count.count
            )

            print(
                f"=========================================================================================="
            )
            print(
                f"[CONSUMER] {configs.env_host_name}:{self.settings.topic_name} miss count: {usage.miss_count.count}, avg read per miss: {usage.miss_count.mean} "
            )
            print(
                f"[CONSUMER] {configs.env_host_name}:{self.settings.topic_name} hit count: {usage.hit_count.count}, avg read per miss: {usage.hit_count.mean} "
            )
            for raw_hc_name, raw_hc_count in usage.source_hit_count.items():
                print(
                    f"[CONSUMER] {configs.env_host_name}:{self.settings.topic_name}:{raw_hc_name} hit count: {raw_hc_count.count}, avg read per miss: {usage.source_miss_count[raw_hc_name].count} "
                )

            encoded = pickle.dumps(usage)
            self.consumer_stats.reset_stats()

            self.stat_lock.release()

            return Response(content=encoded, media_type="application/json")

    def heartbeat(self):
        while not self.killed:
            self.send_heartbeat()
            time.sleep(1.0)

    def send_heartbeat(self):

        ready = False or configs.env_server_name == "localhost"
        server_url = (
            "http://"
            + configs.env_server_name
            + ":"
            + str(configs.env_server_port)
            + "/event_consumer/"
        )

        while not ready:
            try:
                setting_bytes = pickle.dumps(self.settings)
                ready = requests.post(server_url, data=setting_bytes)
            except Exception as e:
                print("[*] [event consumer] error during get request to server: ", e)

            time.sleep(1)

    def trigger_action(self, msg: bytes, _):
        em_request: EventModel = pickle.loads(msg)

        data, raw_data_tracker = self.get_data_from_vsm(em_request)

        if not data:
            return

        self.action(data, raw_data_tracker)

    def trigger_alteration(self, msg: bytes, _):

        target_update_request: CEPConsumerUpdateEvent = pickle.loads(msg)

        if (
            target_update_request.host != self.settings.host_name
            or target_update_request.topic_name != self.settings.topic_name
        ):

            return

        diff_detected = False
        for td in target_update_request.topics:
            if td not in self.settings.source_topics:
                diff_detected = True
        if diff_detected:
            print("[CONSUMER] Processing alteration request.")

        self.settings.source_topics = target_update_request.topics

    def get_data_from_vsm(self, em_request: EventModel):

        datasets: dict[str, Any] = {}
        raw_dates: list[datetime.datetime] = []

        in_local_data_byte = 0.0
        in_local_read_duration = 0.0
        in_local_data_count = 0.0
        in_local_count = 0

        in_remote_data_byte = 0.0
        in_remote_read_duration = 0.0
        in_remote_data_count = 0
        in_remote_count = 0

        event_received_duration: float = (
            datetime.datetime.now(datetime.timezone.utc) - em_request.event_date
        ).microseconds / 1000.0
        event_received_duration = (
            10.0 if event_received_duration < 0 else event_received_duration
        )
        if not configs.INCLUDE_EVENT_DELAY_TO_COST:
            event_received_duration = 0.0

        crr_read_time_start = data_helper.get_start_time()

        self.settings.query.query["_id"] = em_request.data_id

        if not configs.USE_MQTT_FOR_DATA_TRANSFER:
            current_queried_data: list[Any] = self.db.query(
                self.settings.query,
                self.settings.topic_name,
                em_request.vsm,
            )
        else:
            current_queried_data = [em_request.data]

        crr_data_size: float = data_helper.estimate_size_kbytes(current_queried_data)
        if em_request.vsm == self.settings.host_name:
            in_local_data_byte += crr_data_size
            in_local_read_duration += (
                data_helper.get_elapsed_ms(crr_read_time_start)
                + event_received_duration
            )
            in_local_data_count += len(current_queried_data)
            in_local_count += 1
        else:
            in_remote_data_byte += crr_data_size
            in_remote_read_duration += (
                data_helper.get_elapsed_ms(crr_read_time_start)
                + event_received_duration
            )
            in_remote_data_count += len(current_queried_data)
            in_remote_count += 1

        self.stat_lock.acquire()
        if in_remote_data_count > 0:
            self.consumer_stats.remote_reading_byte.add(in_remote_data_byte)
            self.consumer_stats.remote_reading_ns.add(in_remote_read_duration)
            self.consumer_stats.remote_read_data_count.add(in_remote_data_count)
        if in_local_data_count > 0:
            self.consumer_stats.local_reading_byte.add(in_local_data_byte)
            self.consumer_stats.local_reading_ns.add(in_local_read_duration)
            self.consumer_stats.local_read_data_count.add(in_local_data_count)
        self.stat_lock.release()

        if len(current_queried_data) > 0:
            value_matrix = {}

            for sub_data in current_queried_data:
                decoded_data = pickle.loads(sub_data["data"])

                raw_dates.append(sub_data["initDateMin"])
                raw_dates.append(sub_data["initDateMax"])

                for key, value in decoded_data.items():
                    if key in value_matrix:
                        if isinstance(value, list):
                            for v in value:
                                value_matrix[key].append(v)
                        else:
                            value_matrix[key].append(value)
                    else:
                        if isinstance(value, list):
                            value_matrix[key] = value
                        else:
                            value_matrix[key] = [value]

            for key, val in value_matrix.items():
                datasets[key] = val

            if in_remote_data_byte == 0 and in_remote_count > 0:
                raise Exception("Invalid statistics detected")

            if in_local_data_byte == 0 and in_local_count > 0:
                raise Exception("Invalid statistics detected")

            if em_request.raw_data_tracker == "":
                raise Exception("Invalid raw tracker name!")

            if em_request.raw_data_tracker not in self.consumer_stats.source_hit_count:
                self.consumer_stats.source_hit_count[em_request.raw_data_tracker] = (
                    MeanStatBase()
                )
            if em_request.raw_data_tracker not in self.consumer_stats.source_miss_count:
                self.consumer_stats.source_miss_count[em_request.raw_data_tracker] = (
                    MeanStatBase()
                )

            self.consumer_stats.source_hit_count[em_request.raw_data_tracker].add(1)
            self.consumer_stats.hit_count.add(1)
        else:
            if em_request.raw_data_tracker not in self.consumer_stats.source_hit_count:
                self.consumer_stats.source_hit_count[em_request.raw_data_tracker] = (
                    MeanStatBase()
                )
            if em_request.raw_data_tracker not in self.consumer_stats.source_miss_count:
                self.consumer_stats.source_miss_count[em_request.raw_data_tracker] = (
                    MeanStatBase()
                )

            self.consumer_stats.source_miss_count[em_request.raw_data_tracker].add(1)
            self.consumer_stats.miss_count.add(1)
            self.consumer_stats.miss_read_count.add(len(current_queried_data))
            return {}, ""

        if len(datasets.keys()) == 0:
            raise Exception("Dataset cannot be empty here!")

        datasets["minRawDate"] = em_request.raw_date
        datasets["maxRawDate"] = em_request.raw_date

        return datasets, em_request.raw_data_tracker
