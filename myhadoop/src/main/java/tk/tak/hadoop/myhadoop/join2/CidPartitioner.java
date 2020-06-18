 /*
  * Package tk.tak.hadoop.myhadoop.join2
  * FileName: CidPartitioner
  * Author:   Tak
  * Date:     2018/10/23 21:00
  */
 package tk.tak.hadoop.myhadoop.join2;

 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Partitioner;

 /**
  * 分区函数
  * 决定了哪些数据进入同一个reducer
  * @author Tak
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
