/*
 * Package:  com.rust.hbase
 * FileName: MyFilter
 * Author:   Takho
 * Date:     19/3/23 16:25
 * email:    bryroy@gmail.com
 */
package com.rust.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 自定义过滤器
 *
 * @author Takho
 */
public class MyFilter extends FilterBase {

	private byte[] value = null;
	private boolean rowFilter = true;

	public MyFilter(byte[] value, boolean rowFilter) {
		this.value = value;
		this.rowFilter = rowFilter;
	}

	public MyFilter(byte[] value) {
		this.value = value;
	}

	/**
	 * Filters that are purely stateless and do nothing in their reset() methods can inherit
	 * this null/empty implementation.
	 * <p>
	 * {@inheritDoc}
	 */
	@Override
	public void reset() throws IOException {
		this.rowFilter = false;
	}

	/**
	 * Filters that never filter by rows based on previously gathered state from
	 * {@link #filterKeyValue(org.apache.hadoop.hbase.Cell)} can inherit this implementation that
	 * never filters a row.
	 * <p>
	 * {@inheritDoc}
	 */
	@Override
	public boolean filterRow() throws IOException {
		return rowFilter;
	}

	/**
	 * @return The filter serialized using pb
	 */
	public byte[] toByteArray() {
		return value;
	}

	/**
	 * @param pbBytes A pb serialized {@link org.apache.hadoop.hbase.filter.KeyOnlyFilter} instance
	 * @return An instance of {@link org.apache.hadoop.hbase.filter.KeyOnlyFilter} made from <code>bytes</code>
	 * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
	 * @see #toByteArray
	 */
	public static MyFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
		return new MyFilter(pbBytes);
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) throws IOException {
		byte[] bytes = CellUtil.cloneValue(v);

		int res = Bytes.compareTo(bytes, this.value);

		//	小于等于设定value的值包含进结果集
		if (res <= 0) {
			return ReturnCode.INCLUDE;
		} else {
			//	大于设定value的值过滤掉，直接进下一行
			return ReturnCode.NEXT_ROW;
		}

	}
}
