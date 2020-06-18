/*
 * Package tk.tak
 * FileName: TestCRUD
 * Author:   Tak
 * Date:     18/12/20 23:12
 */
package tk.tak.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * HBase的增删改查操作
 *
 * @author Tak
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

	/**
	 * 使用snappy压缩
	 */
	@Test
	public void createTable()throws Exception {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("ns1:t6"));
		HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("cf1"));
		family.setCompressionType(Algorithm.SNAPPY);
		desc.addFamily(family);
		admin.createTable(desc);
	}

	/**
	 * 性能优化之预拆分
	 */
	@Test
	public void preSplit()throws Exception {
		HTableDescriptor t = new HTableDescriptor(TableName.valueOf("ns1:t8"));
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf1");
		t.addFamily(hColumnDescriptor);
		admin.createTable(t, Bytes.toBytes("row-200"), Bytes.toBytes("row-800"), 5);
	}
}
