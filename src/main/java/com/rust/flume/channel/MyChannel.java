 /*
  * Package com.rust.flume.channel
  * FileName: MyChannel
  * Author:   Takho
  * Date:     19/2/23 20:53
  */
 package com.rust.flume.channel;

 import org.apache.flume.ChannelException;
 import org.apache.flume.Event;
 import org.apache.flume.Transaction;
 import org.apache.flume.channel.AbstractChannel;
 import org.apache.flume.event.SimpleEvent;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.sql.Connection;
 import java.sql.DriverManager;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;

 /**
  * @author Takho
  */
 public class MyChannel extends AbstractChannel {
	 private final static Logger logger = LoggerFactory.getLogger(MyChannel.class);

	 static {
		 try {
			 Class.forName("com.mysql.jdbc.Driver");
		 } catch (ClassNotFoundException e) {
			 e.printStackTrace();
		 }
	 }



	 @Override
	 public void put(Event event) throws ChannelException {
		 Transaction transaction = getTransaction();
		 try {
			 Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.231.1:3306/test", "root", "root");

			 transaction.begin();

			 String msg = new String(event.getBody());

			 String sql = "insert into t1 (msg) values ( '" + msg + "' )";

			 PreparedStatement preparedStatement = connection.prepareStatement(sql);
			 preparedStatement.executeUpdate();
			 preparedStatement.close();

			 transaction.commit();
		 } catch (Exception e) {
			 transaction.rollback();
			 throw new ChannelException("persist event fail:", e);
		 } finally {
			 transaction.close();
		 }

	 }

	 @Override
	 public Event take() throws ChannelException {
		 Event event = new SimpleEvent();
		 Transaction transaction = getTransaction();
		 PreparedStatement preparedStatement = null;
		 ResultSet resultSet = null;
		 Connection connection = null;
		 try {
			 connection = DriverManager.getConnection("jdbc:mysql://192.168.231.1:3306/test", "root", "root");


			 connection.setAutoCommit(false);

			 transaction.begin();


			 String sql = "select id,msg from t1";

			 preparedStatement = connection.prepareStatement(sql);
			 resultSet = preparedStatement.executeQuery();
			 boolean next = resultSet.next();
			 String res = "";
			 int id = 0;
			 if (next) {
				 id = resultSet.getInt(1);
				 res = resultSet.getString(2);
			 }
			 connection.prepareStatement("delete  from t1 where id  =" + id).executeUpdate();
			 event.setBody(res.getBytes());

			 transaction.commit();
		 } catch (Exception e) {
			 try {
				 if (connection != null) {
					 connection.rollback();
				 }
			 } catch (SQLException e1) {
				 throw new ChannelException("close connection fail:", e);
			 }
			 transaction.rollback();
			 throw new ChannelException("take event fail:", e);
		 } finally {
			 try {
				 if (preparedStatement != null) {
					 preparedStatement.close();

				 }
			 } catch (SQLException e) {
				 throw new ChannelException("close prepareStatement fail:", e);
			 }
			 try {
				 if (resultSet != null)
					 resultSet.close();
			 } catch (SQLException e) {
				 throw new ChannelException("close resultSet fail:", e);
			 }
			 transaction.close();
		 }
		 return event;
	 }

	 @Override
	 public Transaction getTransaction() {
		 return new Transaction() {
			 @Override
			 public void begin() {

			 }

			 @Override
			 public void commit() {

			 }

			 @Override
			 public void rollback() {

			 }

			 @Override
			 public void close() {

			 }
		 };
	 }
 }
