 /*
  * Package com.rust.hadoop.myhadoop.ipc
  * FileName: MyServer
  * Author:   Rust
  * Date:     2018/9/15 20:56
  */
 package com.rust.hadoop.myhadoop.ipc;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.ipc.RPC;

 import java.io.IOException;

 /**
  * FileName:    MyServer
  * Author:      Rust
  * Date:        2018/9/15
  * Description:
  */
 public class MyServer {
	 public static void main(String[] args) {
		 try {
			 Configuration conf = new Configuration();
			 RPC.Server server = new RPC.Builder(conf)
					 .setProtocol(HelloWorldService.class)
					 .setVerbose(true)
					 .setInstance(new HelloWorldServiceImpl())
					 .setBindAddress("localhost")
					 .setNumHandlers(2)
					 .setPort(8888)
					 .build();
			 server.start();
		 } catch (IOException e) {
			 e.printStackTrace();
		 }

	 }
 }
