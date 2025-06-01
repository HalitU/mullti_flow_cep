FROM python:3.10.10-slim

WORKDIR /code

# Installing the pre-requisite libraries
RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install gcc
RUN apt-get -y install iproute2 

# Installing the python libraries
COPY /cep_library/requirements_server.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r requirements_server.txt

# Copying the source code
COPY . .

# Starting the fastapi application
CMD ["python", "web_api_server.py"]
