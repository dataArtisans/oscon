# oscon
Code from my Apache Flink™ talk at OSCON 2016

You can find the accompanying video and live demonstration here:  https://youtu.be/fstKKxvY23c

# setup

For this demo a local Grafana and InfluxDB instance is needed. These can be setup (including basic grafana configuration) via 

```cd docker && docker-compose up -d```

It will take about one minute until the datasource and dashboard is added to Grafana. Grafana is served on `localhost:3000`. In addition a local Flink cluster is needed (not included in above docker setup).

# Disclaimer
Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org).
