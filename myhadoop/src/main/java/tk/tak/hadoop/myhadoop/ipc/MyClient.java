 /*
  * Package tk.tak.hadoop.myhadoop.ipc
  * FileName: MyClient
  * Author:   Tak
  * Date:     2018/9/15 20:56
  */
 package tk.tak.hadoop.myhadoop.ipc;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.ipc.RPC;

 import java.net.InetSocketAddress;

 /**
  * FileName:    MyClient
  * Author:      Tak
  * Date:        2018/9/15
  * Description:
  */
 public class MyClient {
	 public static void main(String[] args) {
		 Configuration conf = new Configuration();
		 HelloWorldService proxy = null;
		 try {
			 proxy = RPC.getProxy(HelloWorldService.class, HelloWorldService.versionID, new InetSocketAddress(
			 		"localhost",
							 8888),
					 conf);
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
		 String res = proxy.sayHello("client:hello");
		 System.out.println(res);

	 }
 }
