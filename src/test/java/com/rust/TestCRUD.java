 /*
  * Package com.rust
  * FileName: TestCRUD
  * Author:   Takho
  * Date:     18/12/20 23:12
  */
 package com.rust;

 import com.google.common.collect.Lists;
 import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
 import org.apache.hadoop.hbase.client.Delete;
 import org.apache.hadoop.hbase.client.Get;
 import org.apache.hadoop.hbase.client.Put;
 import org.apache.hadoop.hbase.client.Result;
 import org.apache.hadoop.hbase.client.ResultScanner;
 import org.apache.hadoop.hbase.client.Scan;
 import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
 import java.util.List;

 /**
  * HBase的增删改查操作
  *
  * @author Takho
  */
 public class TestCRUD {

	 private Connection connection;
	 private Admin admin;

	 @Before
	 public void setUp() throws IOException {
		 Configuration configuration = HBaseConfiguration.create();
		 connection = ConnectionFactory.createConnection(configuration);
		 admin = connection.getAdmin();
	 }

	 @After
	 public void tearDown() throws IOException {
		 admin.close();
		 connection.close();
	 }


	 /**
	  * drop表
	  * @throws Exception
	  */
	 @Test
	 public void dropTable()throws Exception {

		 Admin admin = connection.getAdmin();

		 admin.disableTable(TableName.valueOf("t1"));
		 admin.deleteTable(TableName.valueOf("t1"));


	 }


	 /**
	  * 创建名称空间
	  */
	 @Test
	 public void createNamespace() throws IOException {

		 //	创建名字空间描述符
		 NamespaceDescriptor ns1 = NamespaceDescriptor.create("ns1").build();

		 admin.createNamespace(ns1);

	 }


	 /**
	  * 删除名称空间
	  */
	 @Test
	 public void deleteNamespace() throws IOException {

		 admin.deleteNamespace("ns1");
	 }


	 /**
	  * 创建表
	  */
	 @Test
	 public void createTable() throws Exception {
		 //	创建表描述符
		 HTableDescriptor desc = new HTableDescriptor("ns1:t1");
		 //	创建列描述符
		 HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes("cf1"));
		 // 添加列族描述符
		 desc.addFamily(colDesc);

		 //	创建表
		 admin.createTable(desc);
	 }

	 /**
	  * 增加数据
	  * @throws Exception
	  */
	 @Test
	 public void putForInsert()throws Exception {
		 Table table = connection.getTable(TableName.valueOf("ns1:t1"));
		 Put put = new Put(Bytes.toBytes("row1"));
		 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("no001"));
		 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
		 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("12"));
		 table.put(put);
		 table.close();
	 }



	 /**
	  * 增加数据
	  * @throws Exception
	  */
	 @Test
	 public void putForUpdate()throws Exception {
		 Table table = connection.getTable(TableName.valueOf("ns1:t1"));
		 Put put = new Put(Bytes.toBytes("row1"));
		 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("no001"));
		 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tomas"));
		 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("12"));
		 table.put(put);
		 table.close();
	 }

	 @Test
	 public void putList()throws Exception {
		 Table table = connection.getTable(TableName.valueOf("ns1:t1"));
		 List<Put> list = Lists.newArrayList();
		 Put put = null;
		 for (int i = 0; i < 100; i++) {
			 put = new Put(Bytes.toBytes("row" + i));
			 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("no001"));
			 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
			 put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(10+i));
			 list.add(put);
		 }
		 table.batch(list);
		 table.close();
	 }

	 /**
	  * 查询数据
	  */
	 @Test
	 public void get() throws Exception {

		 Table table = connection.getTable(TableName.valueOf("ns1:t1"));
		 Get get = new Get(Bytes.toBytes("row98"));
		 // get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"));
		 get.addFamily(Bytes.toBytes("cf1"));
		 Result result = table.get(get);
		 String no = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no")));
		 String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
		 String age = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")));
		 System.out.println(no + "，" + name + "，" + age);


	 }


	 /**
	  * 分页查询数据
	  */
	 @Test
	 public void findAll() throws Exception {
		 Table table = connection.getTable(TableName.valueOf("ns1:t1"));
		 Scan scan = new Scan(Bytes.toBytes("row21"), Bytes.toBytes("row80"));
		 ResultScanner resultScanner = table.getScanner(scan);
		 resultScanner.forEach(result->{
			 String no = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no")));
			 String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
			 String age = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")));
			 System.out.println(no + "，" + name + "，" + age);
		 });
		 table.close();
	 }


	 /**
	  * 删除数据
	  * @throws Exception
	  */
	 @Test
	 public void delete() throws Exception {

		 Table t = connection.getTable(TableName.valueOf("ns1:t1"));

		 Delete delete = new Delete(Bytes.toBytes("row80"));

		 delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
		 t.delete(delete);
		 t.close();

	 }




	 public static void main(String[] args) {
		 String sixTeen = "\\x00\\x00\\x00\\x0c";






	 }

 }
