FROM docker:24.0.5-dind

#installing dependencies
 
RUN apk add --no-cache \
    python3 py3-pip py3-setuptools bash curl \
    && pip3 install --upgrade pip \
    && pip3 install docker-compose

# Set work directory
WORKDIR /usr/src/app

#copy requirements and install them


COPY requirements.txt ./
RUN pip3 install -r requirements.txt

#copy the docker-compose.yml used to run airflow locally
COPY docker-compose.yml ./

#expose the Airflow webserver port (port:8090)
EXPOSE 8090

#Run docker-compose up to start airflow locally 
CMD ["docker-compose", "up", "--build"]
