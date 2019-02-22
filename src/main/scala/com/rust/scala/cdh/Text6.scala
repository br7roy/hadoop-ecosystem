package com.rust.scala.cdh

/**
  * spark-submit 参数相关
  *
  * @author Rust
  */
object Text6 {
  /*
  Your spark application required extra Java options as below. -
  XX:+PrintGCDetails-XX:+PrintGCTimeStamps
  Please replace the XXX values correctly
  ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=talse -
  -conf XXX hadoopexam.jar
  Answer:
  See the explanation for Step by Step Solution and configuration.
  Explanation:
  Solution
  XXX: Mspark.executoi\extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
  Notes: ./bin/spark-submit \
  IT Certification Guaranteed, The Easy Way!
  5
  --class <maln-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  -conf <key>=<value> \
  # other options
  < application-jar> \
  [application-arguments]
  Here, conf is used to pass the Spark related contigs which are required for the application to run like
  any specific property(executor memory) or if you want to override the default property which is set
  in Spark-default.conf
  */
}
