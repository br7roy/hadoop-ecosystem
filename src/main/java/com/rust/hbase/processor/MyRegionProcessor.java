/*
 * Package com.rust.hbase.processor
 * FileName: MyRegionProcessor
 * Author:   Rust
 * Date:     2019/3/25 17:37
 * Description:
 * email: br7roy@gmail.com
 *===============================================================================================
 *   author：          time：                             version：           desc：
 *   Rust           2019/3/25  17:37                      1.0
 *===============================================================================================
 */
package com.rust.hbase.processor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

/**
 * 自定义协处理器
 *
 * @author Rust
 */
public class MyRegionProcessor extends BaseRegionObserver {
	public MyRegionProcessor() {
		super();
		log("new MyRegionProcessor");
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		super.stop(e);
		log("stop:" + this.toString());
	}

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		super.start(e);
		log("start:" + this.toString());
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
		super.prePut(e, put, edit, durability);
		log("prePut:" + put + "," + edit + "," + durability);
		NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
		map.entrySet().iterator().forEachRemaining(entry -> {
			String cf = Bytes.toString(entry.getKey());
			List<Cell> cells = entry.getValue();
			cells.forEach(cell -> {
				String qualifer = Bytes.toString(cell.getQualifier());
				String cf1 = Bytes.toString(cell.getFamily());
				String row = Bytes.toString(cell.getRow());
				int value = Bytes.toIntUnsafe(CellUtil.cloneValue(cell), 0);
				log(row + "/" + cf1 + "/" + qualifer + "/" + value);
				if (qualifer.equals("age")) {
					if (value < 0 || value > 100) {
						log("age:" + row + "/" + cf1 + "/" + qualifer + "/" + value + "is invalid !!");
						throw new RuntimeException("age is invalid !!");

					}
				}

			});
		});


/*		familyCellMap.values().iterator().forEachRemaining(result -> {
			result.forEach(cell -> {
				String value = Bytes.toString(CellUtil.cloneQualifier(cell));
				log(value);
				if ("age".equals(value)) {
					int age = Bytes.toInt(CellUtil.cloneValue(cell));
					if (age < 0 || age > 100) {
						log("age:" + age + "is invalid !!");
						throw new RuntimeException("age is invalid !!");
					}
				}
			});

		});*/


	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
		super.postPut(e, put, edit, durability);
		log("postPut:" + put + "," + edit + "," + durability);
	}

	private void log(String log) {
		try (FileOutputStream fos = new FileOutputStream("/home/ubuntu/co.log", true)) {
			fos.write(log.getBytes());
			fos.write("\r\n".getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}


	}


}
