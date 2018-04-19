#!/bin/bash
/data/install/apache/spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
--class edu.zhku.BehaviorHourly
--master yarn
sparkHourly.jar

# 延时10s
sleep 10s


/data/install/apache/spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
--class edu.zhku.UserEverydayUsage
--master yarn
sparkEveryday.jar
