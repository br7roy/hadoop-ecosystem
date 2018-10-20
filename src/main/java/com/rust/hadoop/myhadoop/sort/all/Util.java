 /*
  * Package com.rust.hadoop.myhadoop.mr
  * FileName: Util
  * Author:   Rust
  * Date:     2018/9/19 21:55
  */
 package com.rust.hadoop.myhadoop.sort.all;

 import java.net.InetAddress;
 import java.net.UnknownHostException;

 /**
  * FileName:    Util
  * Author:      Rust
  * Date:        2018/9/19
  * Description:
  */
 public class Util {
	 /**
	  * 获得组名
	  *
	  * @param groupName
	  * @param hash
	  * @return
	  */
	 public static String getGroup(String groupName, int hash) {
		 String hostName = null;
		 try {
			 hostName = InetAddress.getLocalHost().getHostName();
		 } catch (UnknownHostException e) {
			 e.printStackTrace();
			 return null;
		 }
		 long time = System.nanoTime();
		 return "[" + hostName + "]." + hash + "." + time + ":" + groupName;
	 }

	 public static String getGroup2(String groupName, int hash) {
		 String hostName = null;
		 try {
			 hostName = InetAddress.getLocalHost().getHostName();
		 } catch (UnknownHostException e) {
			 e.printStackTrace();
			 return null;
		 }
		 return "[" + hostName + "]." + hash + ":" + groupName;
	 }
 }
