 /*
  * Package com.rust.hadoop.myhadoop.mr.chain
  * FileName: ReducerA
  * Author:   Rust
  * Date:     2018/10/27 15:06
  */
 package com.rust.hadoop.myhadoop.mr.chain;

 import org.apache.commons.collections4.FluentIterable;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;

 import java.io.IOException;
 import java.net.InetAddress;
 import java.util.List;
 import java.util.Objects;
 import java.util.concurrent.ExecutorService;
 import java.util.concurrent.Executors;
 import java.util.function.BiPredicate;
 import java.util.function.Consumer;
 import java.util.function.Function;
 import java.util.stream.Stream;

 /**
  * @author Rust
  */
 public class ReducerA extends Reducer<Text, IntWritable, Text, IntWritable> {
	 @Override
	 protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			 InterruptedException {

		 /* Runnable runnable = values::iterator;
		 int count = 0;
		 values.forEach(k -> {
			 int i = k.get();
			 count = ++i;
		 });*/


		 context.write(key, new IntWritable((int) Stream.of(values).count()));
		 context.getCounter("r", "RA." + InetAddress.getLocalHost().getHostName()).increment(1L);
		 System.out.println("Reducer." + this.hashCode() + ":" + key.toString());
	 }

	 public static void main(String[] args) {
		 FluentIterable<String> of = FluentIterable.of("1", "2", "3", "4", "5");
		 Consumer<Runnable> consumer = Thread::new;
		 Runnable r = null;
		 consumer.accept(r = () -> System.out.println("hello"));
		 ExecutorService es = Executors.newSingleThreadExecutor();
		 es.submit(r);
		 List<Runnable> runnables = es.shutdownNow();


		 //Consumer<String> consumer = x -> System.out.println(x);
		 // 对象引用::实例方法名
		 Consumer<String> c2 = System.out::println;
		 c2.accept("This is Major Tom");


		 // 类名::静态方法名
		 //Function<Long, Long> f = x -> Math.abs(x);
		 Function<Long, Long> f = Math::abs;
		 Long result = f.apply(-3L);

		 boolean equals = Objects.equals("1", "2");

		 BiPredicate<String, String> bp = Objects::equals;

		 boolean test = bp.test("1", "2");


		 // 引用构造器
		 // 在引用构造器的时候，构造器参数列表要与接口中抽象方法的参数列表一致,格式为 类名::new。如

		 //Function<Integer, StringBuffer> fun = n -> new StringBuffer(n);
		 Function<Integer, StringBuffer> fun = StringBuffer::new;
		 StringBuffer buffer = fun.apply(10);


		 // 引用数组
		 // 引用数组和引用构造器很像，格式为 类型[]::new，其中类型可以为基本类型也可以是类。如

		// Function<Integer, int[]> fun = n -> new int[n];
		 Function<Integer, int[]> f2 = int[]::new;
		 int[] arr = f2.apply(10);

		 Function<Integer, Integer[]> fun2 = Integer[]::new;
		 Integer[] arr2 = fun2.apply(10);


	 }
 }
