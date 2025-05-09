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
