 /*
  * Package com.rust.flume.source
  * FileName: CustomHiveSource
  * Author:   Takho
  * Date:     19/2/20 22:41
  */
 package com.rust.flume.source;

 import com.google.common.collect.Maps;
 import org.apache.flume.Event;
 import org.apache.flume.EventDrivenSource;
 import org.apache.flume.channel.ChannelProcessor;
 import org.apache.flume.event.SimpleEvent;
 import org.apache.flume.source.AbstractSource;

 import java.util.Map;

 /**
  * @author Takho
  */
 public class CustomHiveSource extends AbstractSource implements EventDrivenSource {
	 @Override
	 public synchronized void start() {
		 ChannelProcessor channelProcessor = this.getChannelProcessor();
		 Map<String, String> header = Maps.newHashMapWithExpectedSize(2);
		 header.put("timestamp", "11:55:55 AM,Feb 20,2019");
		 header.put("country", "india");
		 Event e1 = new SimpleEvent();
		 e1.setHeaders(header);
		 e1.setBody("1	helloWorld".getBytes());
		 Event e2 = new SimpleEvent();
		 e2.setHeaders(header);
		 e2.setBody("2	helloWorld".getBytes());
		 Event e3 = new SimpleEvent();
		 e3.setHeaders(header);
		 e3.setBody("3	helloWorld".getBytes());
		 channelProcessor.processEvent(e1);
		 channelProcessor.processEvent(e2);
		 channelProcessor.processEvent(e3);
		 super.start();
	 }

	 @Override
	 public synchronized void stop() {
		 super.stop();
	 }
 }
