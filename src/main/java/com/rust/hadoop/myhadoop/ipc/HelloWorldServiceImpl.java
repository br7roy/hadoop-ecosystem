 /*
  * Package com.rust.hadoop.myhadoop.ipc
  * FileName: HelloWorldServiceImpl
  * Author:   Rust
  * Date:     2018/9/15 21:03
  */
 package com.rust.hadoop.myhadoop.ipc;

 import org.apache.hadoop.ipc.ProtocolSignature;

 import java.io.IOException;

 /**
  * FileName:    HelloWorldServiceImpl
  * Author:      Rust
  * Date:        2018/9/15
  * Description:
  */
 public class HelloWorldServiceImpl implements HelloWorldService {
	 @Override
	 public String sayHello(String msg) {
		 return "hello world";
	 }

	 @Override
	 public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		 return 1;
	 }

	 @Override
	 public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
		 try {
			 return ProtocolSignature.getProtocolSignature(protocol, clientVersion);
		 } catch (ClassNotFoundException e) {
			 e.printStackTrace();
		 }
		 return null;
	 }
 }
