import time
from paho.mqtt import client as mqtt_client
from cep_library import configs

class MQTTProducer:    
    def __init__(self, client_id:str, host_name:str="") -> None:
        self.client_id: str = client_id
        self.host_name = host_name
        
        if configs.MQTT_LOG_ACTIVE:
            print(f"Producer connecting to {configs.MQTT_HOST}, {configs.MQTT_PORT}, with client_id: {self.host_name}:{client_id}")
        self.client:mqtt_client.Client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION1,
            client_id=client_id,
            protocol=mqtt_client.MQTTProtocolVersion.MQTTv5,
            reconnect_on_failure=configs.MQTT_RECONNECT_ON_FAILURE
            )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.currently_working:bool=False
        
        while True:
            try:
                self.client.connect(configs.MQTT_HOST, configs.MQTT_PORT, 60)
                break
            except Exception as _:
                print(f"[MQTTProducer] {self.host_name}:{self.client_id} Waiting to connect to MQTT broker...")
                pass
            time.sleep(1.0)
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTProducer] {self.host_name}:{self.client_id} created.")
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            if configs.MQTT_LOG_ACTIVE:
                print(f"{self.host_name}:{self.client_id} Connected with result code " + str(reason_code))
            pass
        if reason_code > 0:
            print("[MQTTProducer] Error with result code "+str(reason_code))
        
    def on_disconnect(self, client, userdata, reason_code, properties):
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTProducer] {self.host_name}:{self.client_id} Client disconnected...")
        
    def close(self) -> None:
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTProducer] {self.host_name}:{self.client_id} STOPPING THE DATA PRODUCER IN MQTT PRODUCER!!!!")
        self.client.disconnect()        
        
    def produce(self, topic:str, msg:bytes) -> None:
        self.client.publish(
            topic=topic, 
            payload=msg, 
            qos=configs.MQTT_QOS_LEVEL, 
            retain=configs.MQTT_RETAIN_FLAG, 
            properties=None
            )

    
    
    
    
    def run(self):
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTProducer] {self.host_name}:{self.client_id} Running the blocking loop...")
        try:
            self.client.loop_forever(retry_first_connection=configs.MQTT_RECONNECT_FIRST_TRY)
        except Exception as e:
            print(f"[MQTTProducer] {self.host_name}:{self.client_id} Unexpected error occured during loop: ", str(e))
        
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTProducer] {self.host_name}:{self.client_id} Timeout or disconnection happened...")
