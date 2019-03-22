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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
	 * put 带缓存
	 */
	@Test
	public void putWithManualCommit() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		//	关闭自动清理，减少RPC调用服务端
		table.setAutoFlushTo(false);
		int times = 1000;
		long start = System.currentTimeMillis();
		for (int i = 0; i < times; i++) {
			Put put = new Put(Bytes.toBytes("row" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes(i % 100));
			table.put(put);
		}
		//	批量提交数据至RegionServer
		table.flushCommits();
		table.close();
		System.out.printf("elapse:%s", System.currentTimeMillis() - start + "ms");
	}

	/**
	 * 原子性的更改值
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


	/**
	 * 校验数据是否存在
	 */
	@Test
	public void checkExists() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		Get get = new Get(Bytes.toBytes("row2"));
		get.setTimeRange(1545321149770L, 1553162228879L);
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		boolean exists = table.exists(get);
		System.out.println(exists);

	}


	/**
	 * 扫描器
	 * 不用缓存，每一次迭代，发送一次RPC请求至server
	 */
	@Test
	public void scan() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		Scan scan = new Scan(Bytes.toBytes("row33"), Bytes.toBytes("row77"));
		ResultScanner scanner = table.getScanner(scan);
		scanner.spliterator().forEachRemaining(k -> {
			List<Cell> columnCells = k.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			columnCells.forEach(cell -> {
				byte[] valueArray = cell.getValueArray();
				String val = Bytes.toString(valueArray, cell.getValueOffset(), cell.getValueLength());
				System.out.println(val);
			});

		});
/*
		scanner.forEach(k->{

			String val = Bytes.toString(k.getva)
		});

		boolean exists = table.exists(get);
		System.out.println(exists);
*/

	}

	/**
	 * 扫描器
	 * 使用缓存,每次请求，返回一组数据
	 */
	@Test
	public void scanWithCache() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		Scan scan = new Scan(Bytes.toBytes("row33"), Bytes.toBytes("row77"));
		scan.setCaching(1);
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		long start = System.currentTimeMillis();
		ResultScanner scanner = table.getScanner(scan);
		scanner.spliterator().forEachRemaining(k -> {
			List<Cell> columnCells = k.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			columnCells.forEach(cell -> {
				byte[] valueArray = cell.getValueArray();
				String val = Bytes.toString(valueArray, cell.getValueOffset(), cell.getValueLength());
				System.out.println(val);
			});

		});
		table.close();
		System.out.printf("elapse:%s", System.currentTimeMillis() - start + "ms");
	}


	/**
	 * batch操作
	 */
	@Test
	public void batch() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		Scan scan = new Scan(Bytes.toBytes("row33"), Bytes.toBytes("row77"));
		scan.setCaching(1000);
		scan.setBatch(2);
		scan.addFamily(Bytes.toBytes("cf1"));
		long start = System.currentTimeMillis();
		ResultScanner scanner = table.getScanner(scan);
		scanner.forEach(k -> {
			List<Cell> names = k.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			names.forEach(cell -> {
				// String name = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				// System.out.println(name);
				byte[] bytes = CellUtil.cloneValue(cell);
				String string = Bytes.toString(bytes);
				System.out.print("name:" + string);
			});

			List<Cell> ages = k.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
			ages.forEach(cell -> {
				// int age = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				// System.out.println("age:" + age);
				byte[] bytes = CellUtil.cloneValue(cell);
				int string = Bytes.toInt(bytes);
				System.out.print("age:" + string);
			});
			List<Cell> nos = k.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("no"));
			nos.forEach(cell -> {
				// String noa = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				// System.out.println(noa);
				byte[] bytes = CellUtil.cloneValue(cell);
				int string = Bytes.toInt(bytes);
				System.out.print("no:" + string);
			});
			System.out.println();
		});
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.printf("elapse:%s", System.currentTimeMillis() - start + "ms");
	}


	@Test
	public void simpleFilter() throws Exception {

		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		Scan scan = new Scan();

		byte[] cf1 = Bytes.toBytes("cf1");
		byte[] age = Bytes.toBytes("age");

		ValueFilter f1 = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(20)));
		ValueFilter f2 = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(50)));

		FilterList fall = new FilterList(Operator.MUST_PASS_ONE, f1, f2);

		//	依赖列对比器
		ValueFilter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("88"));

		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(cf1, age, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(30)));

		scan.setFilter(fall);
		ResultScanner scanner = table.getScanner(scan);
		scanner.iterator().forEachRemaining(result -> {
			Map<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("cf1"));
			map.forEach((key, value) -> System.out.println(Bytes.toString(key) + ":" + Bytes.toInt(value)));
		});


	}

	/**
	 * 复杂过滤器组合
	 */
	@Test
	public void complexFilter() throws Exception {

		byte[] cf1 = Bytes.toBytes("cf1");
		byte[] names = Bytes.toBytes("name");
		byte[] age = Bytes.toBytes("age");
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		Scan scan = new Scan();

		//	name like 't%'t开头
		SingleColumnValueFilter ftl = new SingleColumnValueFilter(cf1, names, CompareOp.EQUAL, new RegexStringComparator("^q"));

		//	age > 20
		SingleColumnValueFilter ftr = new SingleColumnValueFilter(cf1, age, CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(20)));

		//	ftl and ftr
		FilterList fTop = new FilterList(Operator.MUST_PASS_ALL, ftl, ftr);

		//	name like '%t' t结尾
		SingleColumnValueFilter fbl = new SingleColumnValueFilter(cf1, names, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("somet")));

		//	age <= 20
		SingleColumnValueFilter fbr = new SingleColumnValueFilter(cf1, age, CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(20)));


		//	fbl and fbr
		FilterList fBottom = new FilterList(Operator.MUST_PASS_ALL, fbl, fbr);

		//	fTop or fBottom
		FilterList fall = new FilterList(Operator.MUST_PASS_ONE, fTop, fBottom);

		scan.setFilter(fall);


		ResultScanner scanner = table.getScanner(scan);

		scanner.iterator().forEachRemaining(result -> {
			System.out.println(result);
/*			Map<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("cf1"));
			final int[] omg = new int[]{0};
			final boolean[] flg = new boolean[]{false};
			final StringBuilder appender = new StringBuilder(",");

			map.forEach((key, value) -> {
						if (omg[0]++ % 3 == 0) {
							if (!appender.toString().equals(",")) {
								appender.delete(0, appender.length());
								appender.append(",");
							}
							System.out.println("\r\n");

						} else {
							appender.delete(0, appender.length());
						}

						System.out.print(Bytes.toString(key) + ":" + Bytes.toInt(value) + appender);


					}
			);*/
		});


	}


}