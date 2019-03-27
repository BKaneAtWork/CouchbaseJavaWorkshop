#!/bin/sh
HOST=localhost
USER=travel
PASSWORD=password
BUCKET=travel-sample
CLASS=com.cbworkshop.MainLab

java -classpath target/CbDevWorkshop-0.0.1-SNAPSHOT-jar-with-dependencies.jar -Dcbworkshop.clusteraddress=$HOST -Dcbworkshop.user=$USER -Dcbworkshop.password=$PASSWORD -Dcbworkshop.bucket=$BUCKET $CLASS
