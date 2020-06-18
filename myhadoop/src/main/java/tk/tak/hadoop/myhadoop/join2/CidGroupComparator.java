 /*
  * Package tk.tak.hadoop.myhadoop.join2
  * FileName: CidGroupComparator
  * Author:   Tak
  * Date:     2018/10/23 20:51
  */
 package tk.tak.hadoop.myhadoop.join2;

 import org.apache.hadoop.io.WritableComparable;
 import org.apache.hadoop.io.WritableComparator;

 /**
  * 分组的对比器
  * 分组的意义是，这个key和其他的哪些key在同一个组里（聚合在一起，同一次进入一个reduce)
  * 他决定了key 和其他哪些key在一个组里面，进同一个reduce
  * @author Tak
  */
 public class CidGroupComparator extends WritableComparator {
  public CidGroupComparator() {
   super(ComboKey.class, true);
  }

  /**
   * 只比较客户ID，不比较标记位，让customerId相同的数据进如一个reduce中聚合
   * @param a
   * @param b
   * @return
   */
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
   ComboKey k1 = (ComboKey)a;
   ComboKey k2 = (ComboKey) b;
   return k1.getCustomerId() - k2.getCustomerId();
  }
 }
