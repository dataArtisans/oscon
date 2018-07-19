#!/usr/bin/env bash

curl -u admin:admin http://grafana:3000/api/datasources -d @/tmp/influxdb_datasource.json --header "Content-Type: application/json"
