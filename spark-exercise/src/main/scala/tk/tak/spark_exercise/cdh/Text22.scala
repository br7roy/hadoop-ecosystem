package tk.tak.spark_exercise.cdh

/**
  * @author Tak
  */
object Text22 extends Init {

  //--------------------------------------------------------------------------------------------------
  //  1 . Content.txt: Contain a huge text file containing space separated words.
  //  2 . Remove.txt: Ignore/filter all the words given in this file (Comma Separated).
  //  Write a Spark program which reads the Content.txt file and load as an RDD, remove all the words
  //    from a broadcast variables (which is loaded as an RDD of words from Remove.txt).
  //  And count the occurrence of the each word and save it as a text file in HDFS.
  //--------------------------------------------------------------------------------------------------


  def main(args: Array[String]): Unit = {

    val cRDD = sc.textFile(this.getClass.getResource("Context.txt").toURI.toString)
    val rRDD = sc.textFile(this.getClass.getResource("Remove.txt").toURI.toString)

    val ss = rRDD.flatMap(_.split(",")).map(r => r.replaceAll(" ","")).collect()
    val broad = sc.broadcast(ss)


    val res = cRDD.flatMap(_.split(" ")).filter(cc => !broad.value.contains(cc)).map( r=>(r,1)).reduceByKey((x,y)=>x+y)

    cRDD.saveAsTextFile("/user/ubuntu/cdhtest/text22")


  }


}
