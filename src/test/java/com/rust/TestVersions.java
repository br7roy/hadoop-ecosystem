 /*
  * Package com.rust
  * FileName: TestVersions
  * Author:   Takho
  * Date:     19/3/19 0:51
  */
 package com.rust;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.hbase.Cell;
 import org.apache.hadoop.hbase.HBaseConfiguration;
 import org.apache.hadoop.hbase.NamespaceDescriptor;
 import org.apache.hadoop.hbase.TableName;
 import org.apache.hadoop.hbase.client.Admin;
 import org.apache.hadoop.hbase.client.Connection;
 import org.apache.hadoop.hbase.client.ConnectionFactory;
 import org.apache.hadoop.hbase.client.Get;
 import org.apache.hadoop.hbase.client.Result;
 import org.apache.hadoop.hbase.client.Table;
 import org.apache.hadoop.hbase.util.Bytes;
 import org.junit.After;
 import org.junit.Before;
 import org.junit.Test;

 import java.io.IOException;
 import java.util.List;

 /**
  * @author Takho
  */
 public class TestVersions {
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
	  *
	  * @throws Exception
	  */
	 @Test
	 public void dropTable() throws Exception {

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
	  *
	  */
	 @Test
	 public void get() throws Exception {

		 Table table = connection.getTable(TableName.valueOf("ns1:t1"));

		 Get get = new Get(Bytes.toBytes("row2"));
		 get.addFamily(Bytes.toBytes("cf1"));
		 // get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		 get.setMaxVersions(3);
		 Result result = table.get(get);
		 List<Cell> cells = result.listCells();
		 for (Cell cell : cells) {
			 // byte rowByte = CellUtil.getRowByte(cell, 0);
			 String rowKey = Bytes.toString(cell.getRow());
			 String family = Bytes.toString(cell.getFamily());
			 String col = Bytes.toString(cell.getQualifier());
			 long timeStamp = cell.getTimestamp();
			 String value = Bytes.toString(cell.getValueArray());
			 System.out.println(rowKey + "-" + family + "-" + col + "-" + timeStamp + "=" + value);
		 }

	 }


 }
