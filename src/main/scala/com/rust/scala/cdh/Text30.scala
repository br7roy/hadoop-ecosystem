package com.rust.scala.cdh

/**
  * @author Rust
  */
object Text30 {

    //    Data
    //    echo "IBM,100,20160104" >> /tmp/spooldir2/.bb.txt
    //    echo "IBM,103,20160105" >> /tmp/spooldir2/.bb.txt
    //    mv /tmp/spooldir2/.bb.txt /tmp/spooldir2/bb.txt
    //    After few mins
    //    echo "IBM,100.2,20160104" >> /tmp/spooldir2/.dr.txt
    //    echo "IBM,103.1,20160105" >> /tmp/spooldir2/.dr.txt
    //    mv /tmp/spooldir2/.dr.txt /tmp/spooldir2/dr.txt
    //    You have been given below directory location (if not available than create it) /tmp/spooldir2
    //      .
    //        As soon as file committed in this directory that needs to be available in hdfs in
    //      /tmp/flume/primary as well as /tmp/flume/secondary location.
    //    However, note that/tmp/flume/secondary is optional, if transaction failed which writes in this
    //    directory need not to be rollback.
    //    Write a flume configuration file named flumeS.conf and use it to load data in hdfs with following
    //    additional properties .
    //    IT Certification Guaranteed, The Easy Way!
    //      27
    //    1 . Spool /tmp/spooldir2 directory
    //    2 . File prefix in hdfs sholuld be events
    //    3 . File suffix should be .log
    //    4 . If file is not committed and in use than it should have _ as prefix.
    //    5 . Data should be written as text to hdfs


    //    agent1 .sources = source1
    //    agent1.sinks = sink1a sink1bagent1.channels = channel1a channel1b
    //      agent1.sources.source1.channels = channel1a channel1b
    //      agent1.sources.source1.selector.type = replicating
    //    agent1.sources.source1.selector.optional = channel1b
    //    agent1.sinks.sink1a.channel = channel1a
    //    agent1 .sinks.sink1b.channel = channel1b
    //    agent1.sources.source1.type = spooldir
    //    agent1 .sources.sourcel.spoolDir = /tmp/spooldir2
    //      agent1.sinks.sink1a.type = hdfs
    //    agent1 .sinks, sink1a.hdfs. path = /tmp/flume/primary
    //      agent1 .sinks.sink1a.hdfs.tilePrefix = events
    //    agent1 .sinks.sink1a.hdfs.fileSuffix = .log
    //    agent1 .sinks.sink1a.hdfs.fileType = Data Stream
    //      agent1 .sinks.sink1b.type = hdfs
    //    agent1 .sinks.sink1b.hdfs.path = /tmp/flume/secondary
    //      agent1 .sinks.sink1b.hdfs.filePrefix = events
    //    agent1.sinks.sink1b.hdfs.fileSuffix = .log
    //    agent1 .sinks.sink1b.hdfs.fileType = Data Stream
    //      agent1.channels.channel1a.type = file
    //    agent1.channels.channel1b.type = memory


}
