 /*
  * Package tk.tak.hadoop.myhadoop.sort.secondary
  * FileName: YearGroupComparator
  * Author:   Tak
  * Date:     2018/10/20 22:28
  */
 package tk.tak.hadoop.myhadoop.sort.secondary;

 import org.apache.hadoop.io.WritableComparable;
 import org.apache.hadoop.io.WritableComparator;

 /**
  * 按照年度实现分组的对比器
  *
  * @author Tak
  */
 public class YearGroupComparator extends WritableComparator {

	 // 注册特定类型的对比器
	 public YearGroupComparator() {
		 super(CombineKey.class, true);
	 }

	 @Override
	 public int compare(WritableComparable a, WritableComparable b) {
		 CombineKey k1 = (CombineKey) a;
		 CombineKey k2 = (CombineKey) b;
		 // return k1.compareTo(k2);
		 return k1.getYear() - k2.getYear();
	 }
 }
