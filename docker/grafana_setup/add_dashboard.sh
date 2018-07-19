#!/usr/bin/env bash

curl -u admin:admin http://grafana:3000/api/dashboards/db -d @/tmp/dashboard.json --header "Content-Type: application/json"
