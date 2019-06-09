package com.rust.scala.cdh

/**
  * @author Rust
  */
object Text28 {

  //  You have been given below comma separated employee information.
  //    name,salary,sex,age
  //  alok,100000,male,29
  //  jatin,105000,male,32
  //  yogesh,134000,male,39
  //  ragini,112000,female,35
  //  jyotsana,129000,female,39
  //  valmiki,123000,male,29
  //  Use the netcat service on port 44444, and nc above data line by line. Please do the following
  //    activities.
  //  1. Create a flume conf file using fastest channel, which write data in hive warehouse directory, in a
  //    table called flumeemployee (Create hive table as well tor given data).
  //  2. Write a hive query to read average salary of all employees.


  //  1.create hive table
  //  CREATE TABLE flumeemployee
  //(
  //name string, salary int, sex string,
  //age int
  //)
  //ROW FORMAT DELIMITED
  //FIELDS TERMINATED BY ',';

  //  2.configure flume conf
  //  # define sources,channels,sinks
  //agent1.sources = source1
  //agent1.channels = c1
  //agent1.sinks = s1
  //
  //# define source property
  //agent1.sources.source1.type = syslogtcp
  //agent1.sources.source1.port = 44444
  //agent1.sources.source1.bind = 0.0.0.0
  //
  //# define channel property
  //agent1.channels.c1.type = memory
  //agent1.channels.c1.capacity = 1000
  //agent1.channels.c1.transactionCapacity = 100
  //
  //
  //# define sink property
  //agent1.sinks.s1.type = hdfs
  //agent1.sinks.s1.hdfs.path = /user/hive/warehouse/flumeemployee
  //agent1.sinks.s1.hdfs.fileType = DataStream
  //
  //
  //# bind channel to source
  //agent1.sources.source1.channels = c1
  //
  //# bind sink to channel
  //agent1.sinks.s1.channel = c1

  //  run flume-ng
  //  flume-ng agent -conf /home/cloudera/flumeconf -conf-file
  ///home/cloudera/flumeconf/flume2.conf --name agent1

  //  open terminal send message
  //  alok,100000.male,29
  //  jatin,105000,male,32
  //  yogesh,134000,male,39
  //  ragini,112000,female,35
  //  jyotsana,129000,female,39
  //  valmiki,123000,male,29


}
