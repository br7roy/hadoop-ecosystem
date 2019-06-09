 /*
  * Package com.rust.flume
  * FileName: SysUdpLog
  * Author:   Takho
  * Date:     19/2/17 14:42
  */
 package com.rust.flume;

 import java.net.DatagramPacket;
 import java.net.DatagramSocket;
 import java.net.Inet6Address;

 /**
  * @author Takho
  */
 public class SysUdpLog {

	 /**
	  * 发送UDP数据报文
	  *
	  * @param args
	  */
	 public static void main(String[] args) throws Exception {

		 DatagramSocket socket = new DatagramSocket();
		 String msg = "hello world i am udp data pack!";
		 DatagramPacket packet = new DatagramPacket(msg.getBytes(), 0, msg.length());
		 packet.setAddress(Inet6Address.getByName("s100"));
		 packet.setPort(8888);
		 socket.send(packet);
		 socket.close();


	 }
 }
