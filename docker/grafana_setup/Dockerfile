FROM debian:jessie

ADD dashboard.json /tmp
ADD influxdb_datasource.json /tmp
ADD entrypoint.sh /tmp
ADD add_dashboard.sh /tmp
ADD add_datasource.sh /tmp

RUN apt-get update && apt-get install -y curl

RUN chmod +x /tmp/*.sh

ENTRYPOINT '/tmp/entrypoint.sh'

