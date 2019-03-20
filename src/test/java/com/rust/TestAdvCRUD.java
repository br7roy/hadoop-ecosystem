/*
 * Package com.rust
 * FileName: TestVersions
 * Author:   Takho
 * Date:     19/3/19 0:51
 */
package com.rust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
public class TestAdvCRUD {
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
		Scan scan = new Scan(get);
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

	/**
	 *
	 */
	@Test
	public void get2() throws Exception {

		Table table = connection.getTable(TableName.valueOf("ns1:t1"));

		Get get = new Get(Bytes.toBytes("row2"));
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		get.setMaxVersions(2);
		get.setTimeRange(1553000795410L, 1553085922260L);

		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		for (Cell cell : cells) {
			// byte rowByte = CellUtil.getRowByte(cell, 0);
			short rowLength = cell.getRowLength();
			byte[] bs = new byte[rowLength];
			for (short len = 0; len < rowLength; ++len) {
				byte rowByte = CellUtil.getRowByte(cell, len);
				bs[len] = rowByte;
			}
			String rowKey = Bytes.toString(bs);
			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			String col = Bytes.toString(CellUtil.cloneQualifier(cell));
			long timeStamp = cell.getTimestamp();
			String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
			System.out.println(rowKey + "-" + family + "-" + col + "-" + timeStamp + "=" + value);
		}

	}

	/**
	 * 测试客户端缓冲区
	 */
	@Test
	public void clientBuffer() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		//	关闭自动清理，减少RPC调用服务端
		table.setAutoFlushTo(false);
		int times = 100;
		long start = System.currentTimeMillis();
		for (int i = 0; i < times; i++) {
			Put put = new Put(Bytes.toBytes("row" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
			table.put(put);
		}
		//	批量提交数据至RegionServer
		table.flushCommits();
		table.close();
		System.out.printf("elapse:%s", System.currentTimeMillis() - start + "ms");
	}

	/**
	 * 原子性的更改值得
	 *
	 * @throws Exception
	 */
	@Test
	public void casPut() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		Put put = new Put(Bytes.toBytes("row2"));
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("casVal"));
		table.checkAndPut(Bytes.toBytes("row2"), Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom2"), put);
		table.close();
	}







}
