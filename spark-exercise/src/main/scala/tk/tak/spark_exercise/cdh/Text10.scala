package tk.tak.spark_exercise.cdh

/**
  * @author Tak
  */
object Text10 extends Init {

  def main(args: Array[String]): Unit = {
    val list = List(("Deeapak", "male", 4000), ("Deepak", "male", 2000), ("Deepika", "female",
      2000), ("Deepak", "female", 2000), ("Deepak", "male", 1000), ("Neeta", "female", 2000))
    //    case class People(name: String, sex: String, age: Int)

    val df = spark.createDataFrame(list)
    df.toDF("name", "sex", "cost").createOrReplaceTempView("people")

    df.sqlContext.sql("SELECT name ,sex, sum(cost) FROM people GROUP BY name ,sex").show()


    //////////////////////
    //标准答案
    /////////////////////
    //   Create an RDD out of this list
    val rdd = sc.parallelize(list)
    //   Convert this RDD in pair RDD
    val byKey = rdd.map({ case (name, sex, cost) => (name, sex) -> cost })
    //   Now group by Key
    val byKeyGrouped = byKey.groupByKey()
    //  sum the cost for each group
    val result = byKeyGrouped.map(r => (r._1._1, r._1._2, r._2.sum))
    val r2 = byKeyGrouped.map({ case ((name, id), count) => ((name, id), count.sum) })
    result.collect()
    r2.collect()


  }


}
