import pickle
from threading import Lock, Thread
import time
from cep_library.cep.model.cep_task import MongoQuery

from cep_library.data.database_info_model import DataCarryModel, DatabaseInfoModel
import cep_library.configs as configs
import pymongo
from pymongo.errors import PyMongoError
from datetime import datetime, timezone

from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer


class DatabaseManagementService:
    def __init__(self, host_name: str, groupid: str) -> None:
        if configs.DATABASE_TYPE == 1:
            raise Exception("INVALID DATABASE TYPE")

        self.connections = {}
        self._host_name = host_name
        self._database_publisher = MQTTProducer(
            client_id=f"producer_{configs.kafka_database_topics[0]}_{self._host_name}_{groupid}"
        )

        self._database_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_database_topics[0]}_{self._host_name}_{groupid}",
            core_topic=configs.kafka_database_topics[0],
            topic_names=configs.kafka_database_topics,
            target=self.process_database_info,
            target_args=None,
            shared_subscription=False,
        )

        self.db_conn_lock = Lock()

        dim = DatabaseInfoModel(
            name=self._host_name,
            host=configs.env_mongo_host,
            port=configs.env_mongo_port,
        )

        self._db_info_mdoel = pickle.dumps(dim)

        db_client = self.get_mongo_client(
            configs.env_mongo_self, configs.env_mongo_port
        )

        db = db_client.cep

        self.connections[self._host_name] = db_client

        if configs.DEBUG_MODE:
            print("[*] connected database name: ", db.name)

        self.killed = False

    def create_collection(
        self, host_name: str, collection_name: str, ttl_duration: int
    ):

        self.hold_till_connection_exists(host_name)

        conn: pymongo.MongoClient = self.connections[host_name]
        db = conn.get_database("cep")

        try:
            if collection_name not in db.list_collection_names():
                coll = db.create_collection(collection_name)

                coll.create_index("source", name="ix_source", sparse=True)
                coll.create_index("cep_priority", name="ix_cep_priority", sparse=True)

            else:

                pass
        except:
            pass
        return True

    def hold_till_connection_exists(self, host_name: str):
        while True:
            if host_name in self.connections:

                break
            if configs.DEBUG_MODE:
                print("Target database not available yet: ", host_name)
                print("Current connections: ", self.connections)
            self.publish_database_info()
            time.sleep(1.0)

    def process_database_info(self, msg: bytes, _):
        self.db_conn_lock.acquire()
        db_info: DatabaseInfoModel = pickle.loads(msg)

        if db_info.name not in self.connections and db_info.host != self._host_name:

            db_client = self.get_mongo_client(db_info.host, db_info.port)

            self.connections[db_info.name] = db_client

            self._database_publisher.produce(
                configs.kafka_database_topics[0], self._db_info_mdoel
            )

            if configs.DEBUG_MODE:
                print(
                    self._host_name,
                    " connected to another database with connection info: ",
                    db_info.__dict__,
                )
        self.db_conn_lock.release()

    def get_mongo_client(self, host: str, port: str):
        return pymongo.MongoClient(
            "mongodb://%s:%s@%s:%s" % ("root", "password", host, port),
            tz_aware=True,
            connect=False,
            directConnection=True,
            maxConnecting=configs.data_max_connection,
        )

    def run(self):

        Thread(daemon=True, target=self._database_publisher.run).start()
        Thread(daemon=True, target=self._database_consumer.run).start()

        Thread(daemon=True, target=self.heartbeat).start()

        if configs.DEBUG_MODE:
            print("[*] client database management initialized")

    def heartbeat(self):
        while not self.killed:

            self.publish_database_info()
            time.sleep(1.0)

    def publish_database_info(self):

        self._database_publisher.produce(
            configs.kafka_database_topics[0], self._db_info_mdoel
        )

    def close(self):
        self._database_publisher.close()
        self._database_consumer.close()

        try:
            db_conn: pymongo.MongoClient
            for db_conn in self.connections:
                self.connections[db_conn].close()
        except Exception as e:
            print(f"[DB Close Exception]: {e}")

        self.killed = True

    def ack(self, err, msg):
        pass

    def insert_one(self, data: dict, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):

                db: pymongo.MongoClient = self.connections[preferred]
                id = db.cep[collection].insert_one(data).inserted_id
                return id
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                if (
                    "Cannot use MongoClient after close" in exc._message
                    or "BulkWriteError" in exc._message
                ):
                    pass
                else:
                    print(f"failed with non-timeout error: {exc!r}")

    def insert_many(self, data, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):

                db: pymongo.MongoClient = self.connections[preferred]
                ids = db.cep[collection].insert_many(data, ordered=False).inserted_ids
                return ids
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                if (
                    "Cannot use MongoClient after close" in exc._message
                    or "BulkWriteError" in exc._message
                ):
                    pass
                else:
                    if exc.__class__.__name__ == "BulkWriteError":
                        print(
                            f"[CLIENT] [ERROR] [MONGO] Bulk write error has occured..."
                        )
                    else:
                        print(f"failed with non-timeout error: {exc!r}")
                        print(f"Exception type: {exc.__class__.__name__}")

    def delete_many(self, data, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                db: pymongo.MongoClient = self.connections[preferred]
                data = db.cep[collection].delete_many(data)
                return data
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                if (
                    "Cannot use MongoClient after close" in exc._message
                    or "BulkWriteError" in exc._message
                ):
                    pass
                else:
                    print(f"failed with non-timeout error: {exc!r}")

    def query(self, query: MongoQuery, collection: str, preferred_db: str):

        try:

            with pymongo.timeout(configs.data_operation_timeout):
                if query.limit <= 0:
                    raise Exception("Limit cannot be 0 or lower than 0!")

                db: pymongo.MongoClient = self.connections[preferred_db]
                if configs.USE_SIMPLE_QUERYING:
                    data = db.cep[collection].find(
                        filter=query.query, projection=query.columns, limit=query.limit
                    )
                    raise Exception("Should be disabled at the moment!")
                    return list(data)

                if query.aggregate:
                    if len(query.aggregate) == 0:
                        raise Exception(
                            "Aggregate should have at least one aggregation in it!"
                        )
                    data = db.cep[collection].aggregate(query.aggregate)
                    raise Exception("Not tested yet!")
                else:
                    data = db.cep[collection].find(
                        filter=query.query, projection=query.columns, limit=query.limit
                    )

                return list(data)
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                if (
                    "Cannot use MongoClient after close" in exc._message
                    or "BulkWriteError" in exc._message
                ):
                    pass
                else:
                    print(f"failed with non-timeout error: {exc!r}")
            return list()

    def carry_data(self, data_carry_model: DataCarryModel):

        self.hold_till_connection_exists(data_carry_model.target_host)

        fin_deleted_count = 0
        fin_migrated_count = 0

        source_data = self.query(
            data_carry_model.query,
            data_carry_model.collection,
            data_carry_model.source_host,
        )

        if source_data:
            print(
                f"[DATABASE_MANAGEMENT_SERVICE] Data exists for migration, collection: {data_carry_model.collection}, source: {data_carry_model.source_host}, destination: {data_carry_model.target_host}, time: {datetime.now(tz=timezone.utc)}"
            )

        if source_data:
            fin_migrated_count = len(source_data)
            self.insert_many(
                source_data, data_carry_model.collection, data_carry_model.target_host
            )

        if source_data and data_carry_model.remove_source:

            delete_query = {}

            res = self.delete_many(
                delete_query, data_carry_model.collection, data_carry_model.source_host
            )
            if res:
                fin_deleted_count = res.deleted_count
            if configs.DEBUG_MODE and res != None:
                print(
                    f"{res.deleted_count} records deleted from topic collection: {data_carry_model.collection}"
                )

        if not source_data:
            if configs.DEBUG_MODE:
                print("No data to carry over.")

        if fin_migrated_count > 0:
            print(
                f"Migration completed with migrated count: {fin_migrated_count}, deleted count: {fin_deleted_count}"
            )

        return fin_deleted_count, fin_migrated_count
