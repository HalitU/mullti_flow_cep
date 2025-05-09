from datetime import datetime, timezone
import time
from typing import List
from paho.mqtt import client as mqtt_client
from cep_library import configs
from paho.mqtt.subscribeoptions import SubscribeOptions

class MQTTConsumer:    
    def __init__(self, client_id:str, core_topic:str, topic_names:List[str], target, target_args=None, qos:int=0, shared_subscription:bool=False, shared_sub_group:str="", host_name:str="") -> None:
        self.core_topic:str = core_topic
        self.host_name = host_name
        self.topics: List[str] = topic_names
        self.target = target
        self.target_args = target_args
        self.qos: int = qos
        self.client_id: str = client_id
        self.shared_subscription: bool = shared_subscription
        self.shared_sub_group: str = shared_sub_group
        self.currently_working:bool=False
        self.unsubscribed:bool=False
        self.last_msg_date:datetime=datetime.now(timezone.utc)
        
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTConsumer] Consumer connecting to {configs.MQTT_HOST}, {configs.MQTT_PORT}, with client id: {self.host_name}:{self.client_id}")
        self.client:mqtt_client.Client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION1,
            client_id=self.client_id,
            protocol=mqtt_client.MQTTProtocolVersion.MQTTv5,
            reconnect_on_failure=configs.MQTT_RECONNECT_ON_FAILURE,
            manual_ack=configs.MQTT_USE_MANUAL_ACK
            )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_disconnect = self.on_disconnect
        self.client.on_unsubscribe = self.on_unsubscribe
        
        self.establish_connection()
        
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTConsumer] {self.host_name}:{self.client_id} created.")
    
    def establish_connection(self):
        while True:
            try:
                self.client.connect(configs.MQTT_HOST, configs.MQTT_PORT, 60, clean_start=configs.MQTT_CLEAN_START)
                break
            except Exception as _:
                print(f"{self.host_name}:{self.client_id} Waiting to connect to MQTT broker...")
                pass
            time.sleep(1.0)        
    
    def on_connect(self, client:mqtt_client.Client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            
            pass
        if reason_code > 0:
            print(f"[MQTTConsumer] {self.host_name}:{self.client_id} Error with result code " + str(reason_code))
    
        
        
        topic_tuple = []
        for t in self.topics:
            if configs.MQTT_USE_SHARED_SUB and self.shared_subscription:
                s_t: str = f"$share/{self.shared_sub_group}/{t}"
            else:
                s_t: str = t
            
            topic_tuple.append((s_t, SubscribeOptions(qos=configs.MQTT_CONSUMER_QOS_LEVEL, noLocal=True)))
        client.subscribe(topic_tuple)

    def on_disconnect(self, client, userdata, reason_code, properties):
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTConsumer] {self.host_name}:{self.client_id} Client disconnected...")
        
    def on_subscribe(self, client, userdata, mid, reason_codes, properties):
        print(f"[MQTTConsumer] {self.host_name}:{self.client_id} Subscribed: " + str(mid) + " " + str(reason_codes))
        
    def on_unsubscribe(self, client, userdata, mid, reason_code_list, properties):
        
        
        self.unsubscribed = True
        print(f"[MQTTConsumer] {self.host_name}:{self.client_id} un-subscribed: " + str(mid) + " " + str(reason_code_list))

    
    def on_message(self, client, userdata, msg):
        
        if self.unsubscribed:
            print("Processing while being unsubscribed yet...")
        self.currently_working = True
        self.last_msg_date = datetime.now(timezone.utc)
        self.target(msg.payload, self.target_args)
        self.currently_working = False
        
    
    
    
    def close(self):
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTConsumer] Closing consumer: {self.host_name}:{self.client_id}")
        self.client.disconnect()
        
        
    
    def unsubscribe(self) -> None:
        topic_tuple: List[str] = []
        for t in self.topics:
            if configs.MQTT_USE_SHARED_SUB and self.shared_subscription:
                s_t: str = f"$share/{self.shared_sub_group}/{t}"
            else:
                s_t: str = t
            
            topic_tuple.append(s_t)
        res = self.client.unsubscribe(topic_tuple)
        print(f"Unsubscribe request result: {str(res)}")
    
    
    
    
    
    def run(self):
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTConsumer] {self.host_name}:{self.client_id} Running the blocking loop...")
        try:
            self.client.loop_forever(retry_first_connection=configs.MQTT_RECONNECT_FIRST_TRY)
        except Exception as e:
            if 'BulkWriteError' == type(e).__name__:
                pass
            else:
                print(f"{self.host_name}:{self.client_id} Unexpected error occured during loop: ", str(e))
        if configs.MQTT_LOG_ACTIVE:
            print(f"[MQTTConsumer] Consumer {self.host_name}:{self.client_id} timeout or disconnection happened...")
        
        
        
