FROM python:3.10.10-slim

WORKDIR /code

RUN apt-get update
RUN apt-get -y install apt-utils
RUN apt-get -y install gcc

# Installing the python libraries
COPY /cep_library/requirements.txt .
RUN pip install --upgrade pip
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

CMD ["python", "web_api_client.py"]
