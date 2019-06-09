/*
 * Package:  com.rust
 * FileName: TestFilter
 * Author:   Takho
 * Date:     19/3/23 12:37
 * email:    bryroy@gmail.com
 */
package com.rust.hbase;

import com.google.common.collect.Lists;
import com.rust.hbase.filter.MyFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * 测试HBase的各种过滤器
 *
 * @author Takho
 */
public class TestFilter {

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
			Map<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("cf1"));
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


			});
		});
	}

	/**
	 * 前缀过滤器:判断rowKey
	 *
	 * @throws Exception
	 */
	@Test
	public void testPrefixFilter() throws Exception {
		byte[] cf = Bytes.toBytes("cf1");

		byte[] names = Bytes.toBytes("name");
		byte[] age = Bytes.toBytes("age");
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		Scan scan = new Scan();
		byte[] prefix = Bytes.toBytes("row88");
		PrefixFilter filter = new PrefixFilter(prefix);

		scan.setFilter(filter);

		outputResult(table.getScanner(scan));
	}


	/**
	 * 分页过滤器,类似于mysql limit
	 */
	@Test
	public void pageFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		PageFilter pageFilter = new PageFilter(5);

		Scan scan = new Scan().setFilter(pageFilter).setStartRow(Bytes.toBytes("row800"));
		outputResult(table.getScanner(scan));

	}

	/**
	 * 只返回坐标值(rowKey,columnFamily:qualifier,timestamp)
	 */
	@Test
	public void keyOnlyFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		KeyOnlyFilter filter = new KeyOnlyFilter();
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row800")).setStopRow(Bytes.toBytes("row900"));
		outputResult(table.getScanner(scan));

	}

	/**
	 * 类似KeyOnlyFilter,但是只返回第一个列
	 */
	@Test
	public void firstKeyOnlyFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row800")).setStopRow(Bytes.toBytes("row900"));
		outputResult(table.getScanner(scan));

	}


	/**
	 * 包含结束行过滤器，构造是rowKey
	 */
	@Test
	public void inclusiveStopFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		InclusiveStopFilter filter = new InclusiveStopFilter(Bytes.toBytes("row888"));
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row800")).setStopRow(Bytes.toBytes("row900"));
		outputResult(table.getScanner(scan));

	}

	/**
	 * 时间戳过滤器
	 * 检索cell的时间戳符合列表中定义的时间戳
	 */
	@Test
	public void timeStampFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));

		List<Long> list = Lists.newArrayList();
		list.add(1553177568054L);
		list.add(1553312430515L);
		TimestampsFilter filter = new TimestampsFilter(list);
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row1")).setStopRow(Bytes.toBytes("row2"));
		outputResult(table.getScanner(scan));

	}


	/**
	 * columnCountGetFilter，检索开头的n列数据，而且只返回一行
	 */
	@Test
	public void columnCountGetFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		ColumnCountGetFilter filter = new ColumnCountGetFilter(2);
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row1")).setStopRow(Bytes.toBytes("row2"));
		outputResult(table.getScanner(scan));
	}


	/**
	 * 类似于columnCountGetFilter
	 * (1,2)== (limit,offset)
	 * offset	:	跳过的列数
	 * limit	:	取的列数
	 */
	@Test
	public void columnPaginationFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		ColumnPaginationFilter filter = new ColumnPaginationFilter(1, 2);
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row1")).setStopRow(Bytes.toBytes("row2"));
		outputResult(table.getScanner(scan));
	}

	/**
	 * 对列名进行过滤,过滤列名以指定参数为前缀的column
	 */
	@Test
	public void columnPrefixFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("n"));
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row1")).setStopRow(Bytes.toBytes("row2"));
		outputResult(table.getScanner(scan));
	}

	/**
	 * 随机选中行，几率是参数
	 * -1	:	永远选不中
	 * 1	:	一定选中
	 */
	@Test
	public void randomRowFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		RandomRowFilter filter = new RandomRowFilter(-1f);
		Scan scan = new Scan().setFilter(filter).setStartRow(Bytes.toBytes("row1")).setStopRow(Bytes.toBytes("row2"));
		outputResult(table.getScanner(scan));
	}


	/**
	 * 装饰模式，需要对制定的过滤器进行包装
	 */
	@Test
	public void skipFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		SingleColumnValueFilter f1 = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("age"), CompareOp.EQUAL, Bytes.toBytes(33));

		//	跳跃过滤器
		SkipFilter filter = new SkipFilter(f1);
		Scan scan = new Scan().setFilter(filter);
		outputResult(table.getScanner(scan));
	}


	/**
	 * 装饰模式，需要对指定的过滤器进行包装
	 */
	@Test
	public void whileMatcherFilter() throws Exception {
		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		RowFilter f1 = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row100")));

		WhileMatchFilter filter = new WhileMatchFilter(f1);
		Scan scan = new Scan();
		outputResult(table.getScanner(scan));
	}


	@Test
	public void customFilter() throws Exception {

		HTable table = (HTable) connection.getTable(TableName.valueOf("ns1:t1"));
		Scan scan = new Scan();
		//	设20作为这个过滤器的基准
		MyFilter filter = new MyFilter(Bytes.toBytes(20));
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		scanner.iterator().forEachRemaining(result -> {
			String row = Bytes.toString(result.getRow());
			byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
			int age = Bytes.toIntUnsafe(value, 0);
			System.out.printf("row:%s,age:%s", row, age);
			System.out.println("\r");
		});
		table.close();

	}


	private void outputResult(ResultScanner scanner) {
		scanner.iterator().forEachRemaining(result -> {
			// System.out.println(result);
			String row = Bytes.toString(result.getRow());
			byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"));

		/*	String age = Bytes.toStringBinary(value, 0, value.length);
			age = hexStr2Str(age);*/

			int age = Bytes.toIntUnsafe(value, 0);


			byte[] value1 = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			String name = Bytes.toString(value1);


			byte[] value2 = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no"));
			int no = Bytes.toInt(value2);
			System.out.printf("rowKey:%s,name:%s,age:%s,no:%s", row, name, age, no);
			System.out.println("\r");

		});
	}

}
