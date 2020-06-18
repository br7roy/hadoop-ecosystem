package tk.tak.spark_exercise.exercise

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分组求平均值
 * @author Tak
 */
object AvgWithGroupCase {
  def main(args:Array[String] ): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val iScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val rdd = sc.makeRDD(iScores)
    rdd.groupByKey().map(f=>{
      var num =0
      var sum = 0.0
      for (elem <- f._2) {
        sum=elem+sum
        num+=1
      }
      val avg = sum/num
      (f._1,f"$avg%1.2f")
    }).collect().foreach(println)


    //    或者用mapvalues
    rdd.groupByKey().mapValues(it=>{
      var sum = 0.0
      var cnt = 0
      for (elem <- it) {
        sum+=elem
        cnt+=1
      }
      var avg = sum/cnt
      print(f"$avg%1.2f")
      avg
    }).collect().foreach(println)
    //    使用combineByKey
    rdd
      .combineByKey(
        score=>(score,1),
        (res1:Tuple2[Double,Int], ele:Double)=>(res1._1+ele,res1._2+1),
        (x1:Tuple2[Double,Int], y1:Tuple2[Double,Int])=>(x1._1+y1._1,x1._2+y1._2))
      .mapValues(f =>
    {
      var avg = f._1/f._2
      f"$avg%1.2f"
    }).collect().foreach(println)

  }

}
