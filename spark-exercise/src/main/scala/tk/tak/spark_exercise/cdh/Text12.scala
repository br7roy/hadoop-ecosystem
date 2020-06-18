package tk.tak.spark_exercise.cdh

import java.nio.file.{Files, Paths}

/**
  * @author Tak
  */
object Text12 extends Init with App {


//  val list = Files.readAllLines(Paths.get(this.getClass.getResource("content.txt").toURI))

  val rdd = sc.textFile(Paths.get(this.getClass.getResource("content.txt").toURI).toString)
  val rd2 = rdd.filter( r => r.length>=2 && !r.isEmpty)
  rd2.repartition(1).saveAsTextFile("/user/ubuntu/cdhtest")








}
