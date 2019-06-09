 /*
  * Package com.rust.hadoop.myhadoop.ipc
  * FileName: HelloWorldService
  * Author:   Rust
  * Date:     2018/9/15 20:57
  */
 package com.rust.hadoop.myhadoop.ipc;

 import org.apache.hadoop.ipc.ProtocolInfo;
 import org.apache.hadoop.ipc.VersionedProtocol;

 /**
  * FileName:    HelloWorldService
  * Author:      Rust
  * Date:        2018/9/15
  * Description:
  */
 @ProtocolInfo(protocolName = "myProtocol", protocolVersion = 123L)
 public interface HelloWorldService extends VersionedProtocol {
	 long versionID = 123L;

	 String sayHello(String msg);
 }
