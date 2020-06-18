 /*
  * Package tk.tak
  * FileName: TestCRUD
  * Author:   Tak
  * Date:     18/11/21 0:17
  */
 package tk.tak.hive;

 import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

 /**
  * @author Tak
  */
 public class TestCRUD {

	 private Connection conn;

	 @Before
	 public void setup() throws Exception {
		 Class.forName("org.apache.hive.jdbc.HiveDriver");
		 conn = DriverManager.getConnection("jdbc:hive2://192.168.231.100:10000/hive1", "", "");
	 }

	 @After
	 public void tearDown() throws Exception {
		 if (conn != null) {
			 conn.close();
		 }
	 }


	 @Test
	 public void insert() throws Exception {
		 Statement statement = conn.createStatement();
		 boolean ret = statement.execute("create table hive1.users2(id int ,name string,age int )");
		 System.out.println(ret);
	 }

	 @Test
	 public void batchInsert() throws Exception {
		 try (PreparedStatement ppst = conn.prepareStatement("insert into hive1.users values (?,?,?)")) {
			 ppst.setInt(1, 1);
			 ppst.setString(2, "tom");
			 ppst.setInt(3, 12);
			 ppst.executeUpdate();
			 ppst.setInt(1, 2);
			 ppst.setString(2, "tom2");
			 ppst.setInt(3, 13);
			 ppst.executeUpdate();
			 ppst.setInt(1, 3);
			 ppst.setString(2, "tom3");
			 ppst.setInt(3, 14);
			 ppst.executeUpdate();
		 }
	 }

	 /**
	  * 删除记录
	  *
	  * @throws Exception
	  */
	 @Test
	 public void delete() throws Exception {
		 try (Statement statement = conn.createStatement()) {
			 statement.executeUpdate("delete from hive1.users2");
		 }
	 }

	 /**
	  * 统计
	  *
	  * @throws Exception
	  */
	 @Test
	 public void count() throws Exception {
		 try (Statement statement = conn.createStatement()) {
			 ResultSet resultSet = statement.executeQuery("select count(*) from hive1.users");
			 resultSet.next();
			 System.out.println(resultSet.getInt(1));
		 }
	 }

	 /**
	  * 删除表
	  *
	  * @throws Exception
	  */
	 @Test
	 public void dropTable() throws Exception {
		 try (Statement statement = conn.createStatement()) {
			 statement.execute("drop table hive1.users2");
		 }
	 }
	 /**
	  * 查询分区表
	  *
	  * @throws Exception
	  */
	 @Test
	 public void findPartition() throws Exception {
		 try (Statement statement = conn.createStatement()) {
			 ResultSet resultSet = statement.executeQuery("select * from hive1.test5");
			 while (resultSet.next()) {
				 int id = resultSet.getInt(1);
				 String name = resultSet.getString(2);
				 int age = resultSet.getInt(3);
				 String province = resultSet.getString(4);
				 String city = resultSet.getString(5);
				 System.out.println(id + "，" + name + "，" + age + "," + province + "," + city);
			 }


		 }
	 }
 }
