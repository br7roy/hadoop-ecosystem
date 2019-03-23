/*
 * Package:  com.rust.hbase
 * FileName: TestCounter
 * Author:   Takho
 * Date:     19/3/23 21:07
 * email:    bryroy@gmail.com
 */
package com.rust.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 计数器
 * @author Takho
 */
public class TestCounter {

	private Connection connection = null;
	private Admin admin = null;

	@Before
	public void init() throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(configuration);
		admin = connection.getAdmin();
	}

	@After
	public void tearDown() throws Exception {
		admin.close();
		connection.close();
	}


	/**
	 * 点击量可以用这个方法计算
	 */
	@Test
	public void counter1() throws Exception{
		HTable t = (HTable) connection.getTable(TableName.valueOf("ns1:t3"));
		// Increment increment = new Increment(Bytes.toBytes("row1"));
		long l = t.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("no"), 1L);
		System.out.println(l);
		// Result increment1 = t.increment(increment);
		// System.out.println(increment1);
		t.close();


	}






}
