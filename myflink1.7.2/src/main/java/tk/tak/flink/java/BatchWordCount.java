 /*
  * Package tk.tak.flink
  * FileName: BatchWordCount
  * Author:   Tak
  * Date:     2019/2/26 9:54
  * Description:
  * History:
  *===============================================================================================
  *   author：          time：                             version：           desc：
  *   Tak           2019/2/26  9:54                      1.0
  *===============================================================================================
  */
 package tk.tak.flink.java;

 import org.apache.flink.api.common.functions.FlatMapFunction;
 import org.apache.flink.api.java.ExecutionEnvironment;
 import org.apache.flink.api.java.operators.DataSource;
 import org.apache.flink.api.java.operators.ReduceOperator;
 import org.apache.flink.api.java.tuple.Tuple2;
 import org.apache.flink.api.java.typeutils.PojoField;
 import org.apache.flink.api.java.typeutils.PojoTypeInfo;

 import java.util.List;

 /**
  * 有界数据 WordCountJava版本
  * 使用自定义类会报错
  * {@link org.apache.flink.api.common.operators.Keys｝
  * line:214
  * Field expression must be equal to '*' or '_'
  *
  * @author Tak
  */
 public class BatchWordCount {


	 public static void main(String[] args) throws Exception {
		 final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		 DataSource<String> ds = env.readTextFile("D:\\project\\gitlab\\flink-java\\src\\main\\resources\\wc.txt");

		 ReduceOperator<Tuple2<String, Integer>> reduce = ds.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
			 String[] split = s.split("\\s");
			 for (String as : split) {
				 collector.collect(as);
			 }
		 }).map(o -> new Tuple2<>(o, 1)).groupBy(0).reduce((t2, t1) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1));
		 reduce.print();
		 env.execute("Batch Word count Job");
	 }


	 public static class Wc extends PojoTypeInfo<Wc> {
		 private static final long serialVersionUID = 4294202797471269236L;
		 public String name;
		 public int count;

		 public Wc(Class<Wc> typeClass, List<PojoField> fields) {
			 super(typeClass, fields);
		 }


		 //		 @Override
		 public int compareTo(Wc o) {
			 return Integer.compare(o.count, this.count);
		 }

		 @Override
		 public String toString() {
			 return "Wc{" +
					 "name='" + name + '\'' +
					 ", count=" + count +
					 '}';
		 }
	 }
 }
