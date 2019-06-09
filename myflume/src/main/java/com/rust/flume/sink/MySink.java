 /*
  * Package com.rust.flume.sink
  * FileName: MySink
  * Author:   Takho
  * Date:     19/2/23 18:38
  */
 package com.rust.flume.sink;

 import org.apache.flume.Channel;
 import org.apache.flume.Event;
 import org.apache.flume.EventDeliveryException;
 import org.apache.flume.Transaction;
 import org.apache.flume.sink.AbstractSink;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.util.Map;

 import static org.apache.flume.Sink.Status.BACKOFF;
 import static org.apache.flume.Sink.Status.READY;

 /**
  * 自定义Sink
  *
  * @author Takho
  */
 public class MySink extends AbstractSink {
	 private static final Logger logger = LoggerFactory.getLogger(MySink.class);


	 @Override
	 public Status process() throws EventDeliveryException {
		 Channel channel = super.getChannel();
		 Transaction transaction = channel.getTransaction();
		 try {
			 transaction.begin();
			 Event event = channel.take();
			 if (event == null) {
				 transaction.commit();
				 return BACKOFF;
			 }

			 Map<String, String> headers = event.getHeaders();
			 byte[] body = event.getBody();
			 logger.info("header:{},body:{}", headers, new String(body));
			 transaction.commit();
		 } catch (Exception e) {
			 transaction.rollback();
			 throw new EventDeliveryException("MySink run fail:", e);
		 } finally {

			 transaction.close();
		 }
		 return READY;


	 }

 }
