 /*
  * Package com.rust.flume.source
  * FileName: MySource
  * Author:   Takho
  * Date:     19/2/17 19:44
  */
 package com.rust.flume.source;

 import com.google.common.collect.Lists;
 import com.google.common.util.concurrent.ThreadFactoryBuilder;
 import org.apache.flume.Channel;
 import org.apache.flume.ChannelSelector;
 import org.apache.flume.Event;
 import org.apache.flume.EventDrivenSource;
 import org.apache.flume.channel.ChannelProcessor;
 import org.apache.flume.event.SimpleEvent;
 import org.apache.flume.source.AbstractSource;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.util.Collections;
 import java.util.List;
 import java.util.concurrent.CountDownLatch;
 import java.util.concurrent.ExecutorService;
 import java.util.concurrent.Executors;

 /**
  * 自定义源
  *
  * @author Takho
  */
 public class MySource extends AbstractSource implements EventDrivenSource {

	 private final static Logger log = LoggerFactory.getLogger(MySource.class);

	 private final CountDownLatch latch = new CountDownLatch(1);

	 private final ExecutorService executorService;

	 public MySource() {
		 executorService = Executors.newFixedThreadPool(100);
		 Runtime.getRuntime().addShutdownHook(new ThreadFactoryBuilder().setNameFormat("MySource-shutdown-hook").setDaemon(true).build().newThread(() -> {
			 log.info("shutdown Thread pool!");
			 executorService.shutdownNow();
		 }));
	 }

	 @Override
	 public synchronized void start() {

		 ChannelProcessor channelProcessor = this.getChannelProcessor();

		 List<Runnable> tasks = buildTask(channelProcessor);
		 tasks.forEach(executorService::submit);

		 super.start();
	 }

	 private List<Runnable> buildTask(ChannelProcessor channelProcessor) {
		 List<Runnable> tasks = Lists.newArrayListWithExpectedSize(100000);
		 Runnable r = null;
		 for (int i = 0; i < 100000; i++) {
			 int finalI = i;
			 r = () -> {
				 Event event = new SimpleEvent();
				 event.setBody(("tom" + finalI).getBytes());
				 event.setHeaders(Collections.singletonMap("hello", "world"));
				 channelProcessor.processEvent(event);
				 ChannelSelector selector = channelProcessor.getSelector();
				 List<Channel> channels = selector.getAllChannels();
				 for (Channel channel : channels) {
					 channel.put(event);

				 }
			 };
			 tasks.add(r);
		 }
		 latch.countDown();

		 return tasks;
	 }

	 @Override
	 public synchronized void stop() {
		 try {
			 latch.await();
		 } catch (InterruptedException e) {
			 log.info("waiting all task push fail!", e);
		 }
		 super.stop();

	 }
 }
