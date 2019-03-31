/*
 * Package com.rust
 * FileName: TestCRUD
 * Author:   Takho
 * Date:     18/12/20 23:12
 */
package com.rust.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * HBase的增删改查操作
 *
 * @author Takho
 */
public class TestAdmin {

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
	 * 合并
	 */
	@Test
	public void compact() throws Exception {
		admin.compact(TableName.valueOf("ns1:t4"));
	}


	/**
	 * 查询状态
	 */
	@Test
	public void status() throws Exception {
		ClusterStatus clusterStatus = admin.getClusterStatus();
		int regionsCount = clusterStatus.getRegionsCount();
		System.out.println("regionsCount = " + regionsCount);
	}
}
