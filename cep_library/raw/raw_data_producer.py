from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock, Thread
import time
from typing import List
import uuid
from fastapi import FastAPI, Response
import requests
from cep_library.cep.model.cep_task import EventModel

from cep_library.data import data_helper
from cep_library.data.database_management_service import DatabaseManagementService
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer
from cep_library.raw.model.raw_settings import RawSettings, RawStatisticsBook
from cep_library.raw.model.raw_update_event import RawUpdateEvent

import cep_library.configs as configs
import pickle


class RawDataProducer:
    def __init__(
        self, settings: RawSettings, db: DatabaseManagementService, app: FastAPI
    ) -> None:

        self.database_management_one = db
        self.database_management_one.create_collection(
            settings.producer_name,
            settings.output_topic.output_topic,
            configs.collection_lifetime,
        )

        self.settings: RawSettings = settings
        self.killed = False
        self.db_lock = Lock()
        self.produced_id_ctr = 0

        self.lock: Lock = Lock()

        self.send_heartbeat()

        self.message_producer = MQTTProducer(
            client_id=f"{settings.producer_name}_{settings.producer_port}_producer_{configs.kafka_producer_topics[0]}_{settings.raw_data_name}"
        )

        self.message_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_producer_topics[0]}_{settings.producer_name}_{settings.raw_data_name}",
            core_topic=configs.kafka_producer_topics[0],
            topic_names=configs.kafka_producer_topics,
            target=self.update_data_target,
            target_args=None,
            shared_subscription=False,
        )

        self.raw_statistics = RawStatisticsBook(
            producer_name=settings.producer_name,
            raw_name=settings.raw_data_name,
            topic_name=settings.output_topic.output_topic,
        )
        self.register_action_file_service(app)

        Thread(daemon=True, target=self.message_producer.run).start()
        Thread(daemon=True, target=self.message_consumer.run).start()

        if configs.HEARTBEATS_ACTIVE:
            Thread(daemon=True, target=self.heartbeat).start()

    def get_settings(self):
        return self.settings

    def register_action_file_service(self, app: FastAPI):
        @app.get(
            "/raw_statistics_report/" + self.settings.output_topic.output_topic + "/"
        )
        def raw_statistics_report():
            self.lock.acquire()

            usage = self.raw_statistics
            encoded = pickle.dumps(usage)

            written_count = (
                usage.local_write_data_count.count + usage.remote_write_data_count.count
            )
            print(
                f"[PRODUCER] {self.settings.raw_data_name} wrote count: {written_count}, total produced count: {self.produced_id_ctr}"
            )

            self.raw_statistics.reset_stats()

            self.lock.release()

            return Response(content=encoded, media_type="application/json")

    def send(self, data, raw_data_tracker: str, data_priority: int = 0):
        self.db_lock.acquire()

        raw_data_date = datetime.now(tz=timezone.utc)
        data_id = str(uuid.uuid4())
        converted_data = {
            "_id": data_id,
            "data": pickle.dumps(data),
            "createdAt": raw_data_date,
            "initDateMin": raw_data_date,
            "initDateMax": raw_data_date,
            "source": configs.env_host_name,
            "cep_priority": data_priority,
        }

        payload_size = data_helper.estimate_size_kbytes(converted_data)

        local_write_time = 0
        remote_write_time = 0

        local_data_size = 0
        remote_data_size = 0

        local_data_count = 0
        remote_data_count = 0

        target_hosts: List[str] = deepcopy(self.settings.output_topic.target_hosts)
        target_db_list: List[str] = deepcopy(
            self.settings.output_topic.target_databases
        )
        for target_db in target_db_list:
            start_time = data_helper.get_start_time()

            if not configs.USE_MQTT_FOR_DATA_TRANSFER:
                self.database_management_one.insert_one(
                    converted_data,
                    self.settings.output_topic.output_topic,
                    target_db,
                )

            em = EventModel()
            em.event_date = raw_data_date
            em.data_id = data_id
            em.vsm = target_db
            em.raw_data_tracker = raw_data_tracker
            em.raw_date = raw_data_date

            if configs.USE_MQTT_FOR_DATA_TRANSFER:
                em.data = converted_data

            payload = pickle.dumps(em)

            if configs.MQTT_USE_PREFIX_FOR_EVENTS:
                for th in target_hosts:
                    fixed_output_topic = (
                        f"{self.settings.output_topic.output_topic}_{th}"
                    )
                    self.message_producer.produce(fixed_output_topic, payload)
            else:
                fixed_output_topic = self.settings.output_topic.output_topic
                self.message_producer.produce(fixed_output_topic, payload)

            event_received_duration: float = (
                datetime.now(timezone.utc) - raw_data_date
            ).microseconds / 1000.0
            event_received_duration = (
                10.0 if event_received_duration < 0 else event_received_duration
            )
            if not configs.INCLUDE_EVENT_DELAY_TO_COST:
                event_received_duration = 0.0

            if target_db == self.settings.producer_name:
                local_write_time += (
                    data_helper.get_elapsed_ms(start_time) + event_received_duration
                )
                local_data_size += payload_size
                local_data_count += 1
            else:
                remote_write_time += (
                    data_helper.get_elapsed_ms(start_time) + event_received_duration
                )
                remote_data_size += payload_size
                remote_data_count += 1

        self.produced_id_ctr += 1

        self.lock.acquire()
        self.raw_statistics.add_raw_statistic(
            local_write_time,
            local_data_size,
            local_data_count,
            remote_write_time,
            remote_data_size,
            remote_data_count,
        )
        self.lock.release()

        self.db_lock.release()

    def ack(self, err, msg):
        pass

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
            + "/raw_producer/"
        )

        while not ready:
            try:
                setting_bytes = pickle.dumps(self.settings)
                ready = requests.post(server_url, data=setting_bytes)
            except Exception as e:
                print("[*] error during get request to server: ", e)

            time.sleep(1)

    def stop(self):
        print(f"STOPPING THE RAW DATA PRODUCER!!!!")
        self.message_producer.close()
        self.message_consumer.close()
        self.database_management_one.close()
        self.killed = True

    def update_data_target(self, msg: bytes, args):
        target_update_request: RawUpdateEvent = pickle.loads(msg)

        if self.settings.producer_name != target_update_request.producer_name:
            return

        if not target_update_request.output_topic:
            return

        print("[RAW PRODUCER] A valid alteration request acquired")

        self.db_lock.acquire()

        for new_target_db in [
            n_t_db
            for n_t_db in target_update_request.output_topic.target_databases
            if n_t_db not in self.settings.output_topic.target_databases
        ]:

            if configs.DEBUG_MODE:
                print(
                    "[P] Target database update request is valid, starting the alteration..."
                )

            self.database_management_one.hold_till_connection_exists(new_target_db)

            self.database_management_one.create_collection(
                new_target_db,
                target_update_request.output_topic.output_topic,
                configs.collection_lifetime,
            )

        self.settings.output_topic.target_databases = (
            target_update_request.output_topic.target_databases
        )
        self.settings.output_topic.target_hosts = (
            target_update_request.output_topic.target_hosts
        )

        self.db_lock.release()

        if configs.DEBUG_MODE:
            print(
                f"[P] Target database update completed for {target_update_request.output_topic.output_topic}"
            )
