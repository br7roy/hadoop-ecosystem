 /*
  * Package com.rust
  * FileName: WcApp
  * Author:   Takho
  * Date:     19/1/2 21:19
  */
 package com.rust.spark.java;

 import org.apache.spark.SparkConf;
 import org.apache.spark.api.java.JavaPairRDD;
 import org.apache.spark.api.java.JavaRDD;
 import org.apache.spark.api.java.JavaSparkContext;
 import org.apache.spark.api.java.function.FlatMapFunction;
 import org.apache.spark.api.java.function.Function2;
 import org.apache.spark.api.java.function.PairFunction;
 import scala.Tuple2;

 import java.io.ByteArrayInputStream;
 import java.util.Arrays;
 import java.util.Iterator;
 import java.util.List;

 /**
  * @author Takho
  */
 public class WcApp {
	 public static void main(String[] args) {


		 // Create a Java Spark Context
		 SparkConf conf = new SparkConf();
		 conf.setMaster("local[4]");
		 conf.setAppName("WcApp");
		 JavaSparkContext sc = new JavaSparkContext(conf);
		 // Load our input data.
		 JavaRDD<String> input = sc.textFile("file:///D:/a.txt", 4);
		 // Split up into words.
		 JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			 public Iterator<String> call(String x) {
				 List<String> strings = Arrays.asList(x.split(" "));
				 return strings.iterator();
			 }
		 });
		 // Transform into pairs and count.
		 JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			 public Tuple2<String, Integer> call(String x) {
				 return new Tuple2(x, 1);
			 }
		 }).reduceByKey(new Function2<Integer, Integer, Integer>() {
			 public Integer call(Integer x, Integer y) {
				 return x + y;
			 }
		 });
		 // Save the word count back out to a text file, causing evaluation.
		 counts.saveAsTextFile("D:/out.txt");

		 new ByteArrayInputStream("{\"name\":\"tom\",\"age\":\"12\"}".getBytes());
	 }

 }
