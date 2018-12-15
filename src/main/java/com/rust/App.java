package com.rust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello HBase!
 *
 */
public class App 
{
    public static void main( String[] args )throws Exception
    {
        // 创建配置对象
        Configuration configuration = HBaseConfiguration.create();
        // 通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        // 通过连接获取table信息
        Table t1 = connection.getTable(TableName.valueOf("t1"));
        // 设定row no
        Put put = new Put(Bytes.toBytes("row1"));
        // 设置字段值
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
        t1.put(put);

        // 释放资源
        t1.close();
        connection.close();

    }
}
