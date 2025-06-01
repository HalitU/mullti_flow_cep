import os
import sys


def get_int_from_env(env_str: str, default: int) -> int:
    return int(str(os.getenv(env_str))) if os.getenv(env_str) is not None else default


def get_bool_from_env(env_str: str, default: bool) -> bool:
    return bool(str(os.getenv(env_str))) if os.getenv(env_str) is not None else default


def get_float_from_env(env_str: str, default: float) -> float:
    return float(str(os.getenv(env_str))) if os.getenv(env_str) is not None else default


def get_str_from_env(env_str: str, default: str) -> str:
    return str(os.getenv(env_str)) if os.getenv(env_str) is not None else default


env_mongo_self: str = (
    str(os.getenv("MONGO_SELF_HOST"))
    if os.getenv("MONGO_SELF_HOST") is not None
    else "mongo"
)
env_mongo_host: str = (
    str(os.getenv("MONGO_ONE_HOST"))
    if os.getenv("MONGO_ONE_HOST") is not None
    else "127.0.0.1"
)
env_mongo_port: str = (
    str(os.getenv("MONGO_ONE_PORT"))
    if os.getenv("MONGO_ONE_PORT") is not None
    else "27017"
)
data_carry_enabled = (
    bool(os.getenv("DATA_CARRY_ENABLED"))
    if os.getenv("DATA_CARRY_ENABLED") is not None
    else False
)
data_operation_timeout: int = (
    int(str(os.getenv("DATA_OPERATION_TIMEOUT")))
    if os.getenv("DATA_OPERATION_TIMEOUT") is not None
    else 30
)
data_max_connection = (
    int(str(os.getenv("DATA_MAX_CONNECTION")))
    if os.getenv("DATA_MAX_CONNECTION") is not None
    else 30
)
collection_lifetime = (
    int(str(os.getenv("COLLECTION_LIFETIME")))
    if os.getenv("COLLECTION_LIFETIME") is not None
    else 60
)
data_carry_window = (
    int(str(os.getenv("DATA_CARRY_WINDOW")))
    if os.getenv("DATA_CARRY_WINDOW") is not None
    else 30
)

server_query_type = (
    int(str(os.getenv("SERVER_QUERY_TYPE")))
    if os.getenv("SERVER_QUERY_TYPE") is not None
    else 1
)

print(
    f"Is data migration enabled: {data_carry_enabled} , data operation timeout(seconds): {data_operation_timeout} \
      , max allowed db connection count: {data_max_connection}, collection lifetime(seconds): {collection_lifetime} \
          , data carry window: {data_carry_window}, server query type: {server_query_type}"
)

CP_RAM_LIMIT = (
    float(str(os.getenv("CP_RAM_LIMIT")))
    if os.getenv("CP_RAM_LIMIT") is not None
    else 5000000.0
)

print(f"Optimizer RAM limit: {CP_RAM_LIMIT}")

simulation_duration = (
    int(str(os.getenv("SIM_DURATION")))
    if os.getenv("SIM_DURATION") is not None
    else 300
)

query_with_timedelta = (
    int(str(os.getenv("QUERY_WITHIN_TIMEDELTA")))
    if os.getenv("QUERY_WITHIN_TIMEDELTA") is not None
    else 1000
)

print(
    f"Simulation raw producer settings; sim duration: {simulation_duration}, queries are within timedelta: {query_with_timedelta}"
)

env_server_name: str = (
    str(os.getenv("SERVER_NAME"))
    if os.getenv("SERVER_NAME") is not None
    else "127.0.0.1"
)
env_server_port = (
    int(str(os.getenv("SERVER_PORT"))) if os.getenv("SERVER_PORT") is not None else 8000
)

env_distribution_type = (
    int(str(os.getenv("DISTRIBUTION_TYPE")))
    if os.getenv("DISTRIBUTION_TYPE") is not None
    else 0
)
print("Env server name is: ", os.getenv("SERVER_NAME"))

env_host_name: str = (
    str(os.getenv("HOST_NAME")) if os.getenv("HOST_NAME") is not None else "client_one"
)
env_host_ip: str = (
    str(os.getenv("HOST_IP")) if os.getenv("HOST_IP") is not None else "127.0.0.1"
)
env_host_port = (
    int(str(os.getenv("HOST_PORT"))) if os.getenv("HOST_PORT") is not None else 8080
)
env_host_x: int = (
    int(str(os.getenv("HOST_X"))) if os.getenv("HOST_X") is not None else 100
)
env_host_y: int = (
    int(str(os.getenv("HOST_Y"))) if os.getenv("HOST_Y") is not None else 100
)

evaluation_period = (
    int(str(os.getenv("EVAL_PERIOD_SECS")))
    if os.getenv("EVAL_PERIOD_SECS") is not None
    else 30
)

kafka_alteration_topics = ["events.management.alteration"]
kafka_database_topics = ["events.management.database"]
kafka_resource_topics = ["events.management.resource"]
kafka_producer_topics = ["events.management.producer"]
kafka_consumer_topics = ["events.management.consumers"]

data_pattern_json_file = os.getenv("data_pattern_json_file")
raw_producer_type = get_str_from_env("RAW_PRODUCER_TYPES", "")
if raw_producer_type == "":
    raw_producer_types = None
else:
    raw_producer_types = raw_producer_type.split(",")
    print("raw data producing tasks: ", raw_producer_types)
    raw_producer_types = [str(idd) for idd in raw_producer_types]

event_consumer_type = get_str_from_env("EVENT_CONSUMER_TYPES", "")
if event_consumer_type == "":
    event_consumer_types = None
else:
    event_consumer_types = event_consumer_type.split(",")
    print("raw data producing tasks: ", event_consumer_types)
    event_consumer_types = [str(idd) for idd in event_consumer_types]

cep_multiprocessor_available = (
    bool(os.getenv("CEP_MULTIPROCESSOR_AVAILABLE"))
    if os.getenv("CEP_MULTIPROCESSOR_AVAILABLE") is not None
    else False
)

print(f"Is multiprocessing available: {cep_multiprocessor_available}")

DEBUG_MODE = (
    bool(os.getenv("DEBUG_MODE")) if os.getenv("DEBUG_MODE") is not None else False
)


OS_TYPE = int(str(os.getenv("OS_TYPE"))) if os.getenv("OS_TYPE") is not None else 0

IMAGE_FETCH_LIMIT = (
    int(str(os.getenv("IMAGE_FETCH_LIMIT")))
    if os.getenv("IMAGE_FETCH_LIMIT") is not None
    else 1
)

USE_HISTORIC_DIST_AFTER_N = (
    int(str(os.getenv("USE_HISTORIC_DIST_AFTER_N")))
    if os.getenv("USE_HISTORIC_DIST_AFTER_N") is not None
    else 0
)

CP_RUNTIME_SECONDS = (
    int(str(os.getenv("CP_RUNTIME_SECONDS")))
    if os.getenv("CP_RUNTIME_SECONDS") is not None
    else 2
)

MQTT_HOST: str = (
    str(os.getenv("MQTT_HOST")) if os.getenv("MQTT_HOST") is not None else "mqttserver"
)
MQTT_PORT: int = (
    int(str(os.getenv("MQTT_PORT"))) if os.getenv("MQTT_PORT") is not None else 1883
)

EVAL_ACTIVE: int = (
    int(str(os.getenv("EVAL_ACTIVE"))) if os.getenv("EVAL_ACTIVE") is not None else 0
)


DISABLE_VSM: int = (
    int(str(os.getenv("DISABLE_VSM"))) if os.getenv("DISABLE_VSM") is not None else 0
)

print(f"Is VSM activated: {DISABLE_VSM == 0}")

MIN_COST_AFTER_N_DIST: int = (
    int(str(os.getenv("MIN_COST_AFTER_N_DIST")))
    if os.getenv("MIN_COST_AFTER_N_DIST") is not None
    else 2
)
ENFORCE_PRODUCTION_LOCATION: int = get_int_from_env("ENFORCE_PRODUCTION_LOCATION", 0)
DIFFERENT_DEVICE_READ_PENATLY: float = get_float_from_env(
    "DIFFERENT_DEVICE_READ_PENATLY", 1.5
)
DEVICE_CHANGE_PENALTY: float = get_float_from_env("DEVICE_CHANGE_PENALTY", 1)
DEVICE_NON_CHANGE_MULTIPLIER: int = get_int_from_env("DEVICE_NON_CHANGE_MULTIPLIER", 1)
MANIPULATE_CP_PARAMS: int = get_int_from_env("MANIPULATE_CP_PARAMS", 1)
DIFFERENCE_BALANCE_WEIGHT: int = get_int_from_env("DIFFERENCE_BALANCE_WEIGHT", 3)

DIFFERENCE_EQUALIZER_WEIGHT: int = get_int_from_env("DIFFERENCE_EQUALIZER_WEIGHT", 3)

OPT_USE_CRR_VALUES_ONLY: int = get_int_from_env("OPT_USE_CRR_VALUES_ONLY", 0)

MIN_REQUIRED_ACTIVE_VAR: int = get_int_from_env("MIN_REQUIRED_ACTIVE_VAR", 1)

RAW_IMAGE_SCALE_PERCENT: int = get_int_from_env("RAW_IMAGE_SCALE_PERCENT", 3)

DEVICE_ACTION_LIMIT: int = get_int_from_env("DEVICE_ACTION_LIMIT", 0)

DEVICE_ACTION_MIN_LIMIT: int = get_int_from_env("DEVICE_ACTION_MIN_LIMIT", 0)
DEVICE_INTAKE_MIN_LIMIT: int = get_int_from_env("DEVICE_INTAKE_MIN_LIMIT", 0)

DATABASE_TYPE: int = get_int_from_env("DATABASE_TYPE", 0)
MQTT_QOS_LEVEL: int = get_int_from_env("MQTT_QOS_LEVEL", 0)

if MQTT_QOS_LEVEL > 2:
    raise Exception("Qos level cannot be higher than 2!!")

print(f"Chosen QoS level is :{MQTT_QOS_LEVEL}")

STAT_NORMALIZER_LOW: float = (
    float(str(os.getenv("STAT_NORMALIZER_LOW")))
    if os.getenv("STAT_NORMALIZER_LOW") is not None
    else 10.0
)
STAT_NORMALIZER_HIGH: float = (
    float(str(os.getenv("STAT_NORMALIZER_HIGH")))
    if os.getenv("STAT_NORMALIZER_HIGH") is not None
    else 100.0
)

ALARM_BROADCAST_DURATION: float = (
    float(str(os.getenv("ALARM_BROADCAST_DURATION")))
    if os.getenv("ALARM_BROADCAST_DURATION") is not None
    else 0.25
)


CEP_SAT_OPT_FUNCTION: int = (
    int(str(os.getenv("CEP_SAT_OPT_FUNCTION")))
    if os.getenv("CEP_SAT_OPT_FUNCTION") is not None
    else 1
)
EVENT_ACTIVATION_MULTIPLIER = (
    float(str(os.getenv("CP_SAT_EVENT_ACTIVATION_MULTIPLIER")))
    if os.getenv("CP_SAT_EVENT_ACTIVATION_MULTIPLIER") is not None
    else 1.2
)
CEP_SAT_BANDWIDTH_BALANCER: int = (
    int(str(os.getenv("CEP_SAT_BANDWIDTH_BALANCER")))
    if os.getenv("CEP_SAT_BANDWIDTH_BALANCER") is not None
    else 1
)

STATISTICS_REMOTE_MULTIPLIER: float = (
    float(str(os.getenv("STATISTICS_REMOTE_MULTIPLIER")))
    if os.getenv("STATISTICS_REMOTE_MULTIPLIER") is not None
    else 1.2
)

ALLOWED_PARALLEL_EXECUTION_COUNT: int = (
    int(str(os.getenv("ALLOWED_PARALLEL_EXECUTION_COUNT")))
    if os.getenv("ALLOWED_PARALLEL_EXECUTION_COUNT") is not None
    else 2
)


IS_LOCAL: int = (
    int(str(os.getenv("IS_LOCAL"))) if os.getenv("IS_LOCAL") is not None else 0
)

scenario_case: int = 2
ALLOWED_PARALLEL_EXECUTION_COUNT = 100
INCLUDE_EVENT_DELAY_TO_COST: bool = True
STATISTICS_REMOTE_MULTIPLIER: float = 1.5
CP_SAT_STATISTICS_REMOTE_MULTIPLIER: float = 1.5
EXPECTED_LOAD_TYPE: int = 1
DATA_LOAD_TYPE: int = 2
STATISTICS_EVENT_ACTIVATION_COST_TYPE: int = 0
EVENT_ACTIVATION_MULTIPLIER = 1.25
USE_SIMPLE_QUERYING: bool = False
UPDATE_LATEST_STATS: bool = False
DEFAULT_STAT_PREFERENCE: int = 2
MIN_REQUIRED_ACTIVE_VAR = 1
ENABLE_ALARM_BROADCAST: bool = True
KEEP_RAWDATA_AT_SOURCE: bool = True
ALARM_BROADCAST_DURATION = 0.001
collection_lifetime = 6000
USE_DELAY_SINCE_BEGINNING: bool = True
DEVICE_CHANGE_PENALTY: float = 1.1


NORMALIZATION_APPROACH: int = 0
STAT_NORMALIZER_LOW = 10.0
STAT_NORMALIZER_HIGH = 2000.0
STAT_DEFAULT_VAR: float = (STAT_NORMALIZER_HIGH + STAT_NORMALIZER_LOW) / 2.0
data_carry_enabled = False
query_with_timedelta = 6000000
DEVICE_ACTION_LIMIT = 100


MQTT_USE_SHARED_SUB: bool = True
USE_CUMULATIVE_COST: bool = True
USE_CUMULATIVE_LOAD: bool = True
USE_DROP_PREVENTION: bool = True
USE_UPLINK_DOWNLINK_COST: bool = True
USE_HISTORIC_DIST_AFTER_N = 0
MQTT_LOG_ACTIVE: bool = False
MQTT_USE_MANUAL_ACK: bool = False
MQTT_CONSUMER_QOS_LEVEL = 0
MQTT_RETAIN_FLAG: bool = False
MQTT_CLEAN_START: bool = True
MQTT_RECONNECT_ON_FAILURE = True
MQTT_RECONNECT_FIRST_TRY = False
MQTT_SHARED_EVENT_CLIENT_ID: bool = False
MQTT_USE_PREFIX_FOR_EVENTS: bool = False
WAIT_BEFORE_DEACTIVATIONS: float = 1.0
DEVICE_EXECUTION_LIMIT: int = 1000 # number of events per second
HEARTBEATS_ACTIVE: bool = False
LOGS_ACTIVE: bool = False


GA_USE_BANDWIDTH_MULTIPLIER: bool = False
GA_BANDWIDTH_COST_TYPE: int = 1
GA_COST_TYPE: int = 1
GA_EXLCUDE_EXECUTION_COST: bool = True
GA_USE_HARD_BW_LIMIT: bool = True
GA_USE_ONLY_DEVICE_CAPABILITY: bool = True
GA_USE_OSCILLATION_PENALTY: bool = True
GA_OSCILLATION_PENALTY: float = 1.1
GA_USE_OVERLOAD_PENALTY: bool = False
GA_OVERLOAD_PENALTY: float = 50.0
GENETIC_ALGO_WRITE_AT_DEVICE: bool = True

GA_USE_LINK_COUNT_FOR_BALANCE: bool = False
GA_USE_LINK_COUNT_WITH_OVERLOAD_PENALTY: bool = False
GA_USE_ADVANCED_LINK_COST: bool = False
GA_USE_UP_DOWN_LINK_COST: bool = True
GA_USE_UP_DOWN_LINK_COST_ADVANCED: bool = True

GA_PARAMS_MAX_GENERATION = 20
GA_PARAMS_MAX_POPULATION = 50
GA_PARAMS_MUTATION_SIZE = 20
GA_PARAMS_PARENT_SELECTION = 5
GA_PARAMS_MUTATION_PROB = 0.5


RELAX_USE_REVERSED_ORDER: bool = False
RELAX_USE_OSCILLATION_PENALTY: float = True
RELAX_OSCILLATION_PENALTY: float = 1.1
RELAX_USE_OVERLOAD_PENALTY: bool = False
RELAX_INCLUDE_BANDWIDTH_BALANCER: bool = False
RELAX_MULTIPLY_BANDWIDTH_BALANCER: bool = False
RELAX_MULTIPLY_HIGHEST_LOAD: bool = False

RELAX_USE_FULL_UP_DOWN_LINK_LOAD: bool = True
RELAX_USE_LINK_COUNT_WITH_OVERLOAD_PENALTY: bool = False
RELAX_USE_UP_DOWN_LINK_COUNT_FOR_BALANCE: bool = False
RELAX_USE_RATE_ONLY_FOR_MAX_LINK: bool = False
RELAX_USE_LINK_COUNT_FOR_BALANCE: bool = False
RELAX_USE_PENALTY_ON_LOAD: bool = False
RELAX_USE_ADVANCED_LINK_COST: bool = False
RELAX_USE_ADVANCED_LINK_COST_WITH_LOAD_PENALTY: bool = False
RELAX_USE_UP_DOWN_LINK_LOAD: bool = False
RELAX_USE_HARD_BW_LIMIT: bool = True


CP_SAT_DIVIDER: float = 1
CP_SAT_LINK_DIVIDER: float = 1
CP_EVENT_CHANGE_PENALTY_ACTIVE: bool = True
CP_USE_OSCILLATION_PENALTY: bool = True
CP_WRITE_MULTIPLIER: float = 1.0
CP_READ_MULTIPLIER: float = 1.0
CP_SAT_WORKER_COUNT: int = 1
CP_SAT_PRESOLVE: bool = True
CP_SAT_PRESOLVE_ITERATIONS: int = 0
CP_SAT_HINT_ACTIVE: bool = True
CP_SAT_UPPER_INT_LIMIT: int = 1000000000000
CP_SAT_LOG = True
CP_SAT_ABSOLUTE_GAP: float = 0.01
CP_RUNTIME_SECONDS: int = 15
CP_EVENT_CHANGE_PENALTY: float = 1.1
CP_OSCILLATION_PENALTY: float = 1.1
CP_SAT_TIME_MULTIPLIER: int = 1000
CP_SAT_DEBUG_VALIDATIONS: bool = False
CP_SAT_USE_HARD_DELAY_LIMITS: bool = False
CP_SAT_MAX_BOTTLENECK_DURATION: int = 10000
CP_SAT_MAX_DELAY_DURATION: int = 10000
CP_SAT_USE_NORMALIZED_COST: bool = True
CP_SAT_COST_TYPE: int = 3
CP_USE_UP_DOWN_HARD_LIMIT: bool = True
CP_USE_UP_DOWN_HARD_LIMIT_RATIO: float = 1.0


data_pattern_json_file = (
    "vanet_apps/data/dataconfigs/sensor_data_pattern_less_load.json"
)
USE_MQTT_FOR_DATA_TRANSFER: bool = False
MQTT_QOS_LEVEL = 0
MQTT_CONSUMER_QOS_LEVEL = 0
CUMULATIVE_LOAD_LIMIT: float = 200 * 125 * 4000 # bw limit of the current device i guess
ENABLE_ACTION_ARTIFICAL_DELAY: bool = True
ACTION_ARTIFICAL_DELAY: float = 0.001
ENABLE_ARTIFICAL_TASK_LOAD: bool = True
ARTIFICAL_TASK_LOAD: int = 1024
ARTIFICAL_TASK_LOAD_TYPE: int = 2
ARTIFICAL_TASK_LOAD_MULTIPLIER: int = 16000
MULTIPLY_DATA_SIZE_ALARMING: int = 11000
MULTIPLY_DATA_SIZE_NORMAL: int = 19000
MULTIPLY_AUDIO_DATA_PRODUCTION_COUNT: int = 3
MULTIPLY_VIDEO_DATA_PRODUCTION_COUNT: int = 3
MULTIPLY_DATA_PRODUCTION_COUNT: int = 3
server_query_type = 7
simulation_duration = 600

LOCK_PROD_CTR: bool = False
LOCK_PROD_CTR_VAL: int = 3
USE_FIXED_EVAL: bool = False


GA_USE_MAX_BW_USAGE: bool = True
GA_USE_SUM_BW_USAGE: bool = False
GA_USE_MAX_LINK_USAGE: bool = False
GA_USE_SUM_LINK_USAGE: bool = False
GA_USE_END_TO_END_LINK_MAX: bool = False


RELAX_USE_MAX_BW_USAGE: bool = True
RELAX_USE_SUM_BW_USAGE: bool = False
RELAX_USE_SUM_LINK_USAGE: bool = False
RELAX_USE_MAX_LINK_USAGE: bool = False
RELAX_SPRING_ITERATION_COUNT: int = 20
RELAX_INCLUDE_SWAPPING_STEP: bool = True


CP_SAT_WRITE_AT_EXECUTION_LOC: bool = False
CP_SAT_BW_FIRST_TH: float = 0.8
CP_SAT_ABOVE_BW_MULTP: int = 5
CP_SAT_ABOVE_BW_MULTP_CEIL: int = 1000
CP_SAT_USE_EXPECTED_DELAY_ONLY: bool = False
CP_SAT_USE_OVERALL_DELAY_COST: bool = True
CP_USE_MAX_CONNECTION_LOAD_COST: bool = False
CP_USE_MAX_LINK_LOAD_COST: bool = False
CP_USE_SUM_LINK_LOAD_COST: bool = False
CP_USE_DAG_LINK_LOAD_COST: bool = True
CP_OPT_MAX_DEVICE_LOAD: bool = False
CP_USE_DAG_BOTTLENECK: bool = False
CP_INCLUDE_MAX_CONNECTION_COST: bool = True
CP_USE_TWO_STEP_DAG_LINK_LOAD_COST: bool = False
