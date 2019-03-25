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

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.FileOutputStream;
import java.io.IOException;

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
