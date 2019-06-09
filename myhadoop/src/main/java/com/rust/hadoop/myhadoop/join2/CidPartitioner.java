 /*
  * Package com.rust.hadoop.myhadoop.join2
  * FileName: CidPartitioner
  * Author:   Rust
  * Date:     2018/10/23 21:00
  */
 package com.rust.hadoop.myhadoop.join2;

 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Partitioner;

 /**
  * 分区函数
  * 决定了哪些数据进入同一个reducer
  * @author Rust
  */
 public class CidPartitioner extends Partitioner<ComboKey, Text> {

  /**
   * 相同的customerId进同一个reducer
   * @param comboKey
   * @param text
   * @param numPartitions
   * @return
   */
  @Override
  public int getPartition(ComboKey comboKey, Text text, int numPartitions) {
   return comboKey.getCustomerId() % numPartitions;
  }
 }
