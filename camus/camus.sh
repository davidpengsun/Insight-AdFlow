#!/bin/bash
set -eu



cd /usr/local/hadoop/etc/hadoop/
/usr/local/hadoop/bin/hadoop jar camus-example-0.1.0-SNAPSHOT-shaded.jar com.linkedin.camus.etl.kafka.CamusJob -P /usr/local/camus/camus-example/src/main/resources/camus.properties
