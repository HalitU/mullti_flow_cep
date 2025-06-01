# CEP Automation Library

This is an upgraded version of a previous library we have developed in https://github.com/HalitU/cep-vsm-lib.

Its working principle in terms of running the library is pretty much same, and further instructions will be added here within time. The additions here are the overhaul within the management distribution algorithms and the better constrained programming approach.

## Components
/cep directory contains the core background worker that registers the events at the workers, listens the incoming MQTT messages from the central management devices, and executes events

/consumer directory contains the code for registering consumers.

/data contains the code for managing the shared database system

/management contains code for the central management unit, which also does the periodic optimizations.

/mqtt handles the MQTT communications

/raw enables registering the data producing applications.

/stats_helper helps representing statistics collected from the devices into better formats.

configs.py contains different configurations that can be set via environment variables or via code.

requirements.txt contains the minimum library requirements used during the experiments.

## Example

Start the applications, 1 server and 2 client apps.

docker compose -f docker-compose.yml up cep_api_server --build

docker compose -f docker-compose.yml up client_one --build

docker compose -f docker-compose.yml up client_two --build

After the containers are up and running, go to http://localhost:8080/docs#/default/__start_simulation__post
run the method with algorithm_number 46 and hist_data_after 0 to test the proposed constrained based programming solution.

Rest of the algorithm choices can be find within the cep_library/management/server/management_server.py file
