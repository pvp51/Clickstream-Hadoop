#!/bin/sh
hadoop fs -rm -r output*
set -e
mvn clean install
hadoop jar target/click-stream-0.0.1.jar hadoop.BreadCrumbs resources/clickstream-enwiki-2018-10-abridged.tsv