package tk.tak.spark_exercise.cdh

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

/**
  * @author Tak
  */
object Text8 extends Init {
  //patientID,name,dateOfBirth,lastVisitDate
  //1001,Ah Teck,1991-12-31,2012-01-20
  //1002,Kumar,2011-10-29,2012-09-20
  //1003,Ali,2011-01-30,2012-10-21
  //Accomplish following activities.
  //1 . Find all the patients whose lastVisitDate between current time and '2012-09-15'
  //2 . Find all the patients who born in 2011
  //3 . Find all the patients age
  //4 . List patients whose last visited more than 60 days ago
  //5 . Select patients 18 years old or younger
  def main(args: Array[String]): Unit = {

    case class Patient(patientID: Integer, name: String, dateOfBirth: Timestamp, lastVisitDate: Timestamp)

    implicit val cc: Encoder[Patient] = Encoders.kryo[Patient]
    //  放在spark-shell上需要使用句隐式传值
//        import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/ubuntu/patient.csv")
    //    重点来了，在这一步先调用printSchema函数，看清dataFrame的数据结构，看一下样例类是否符合，不符合，肯定要改样例类！
    //    我这里已经测过了，明确了样例类的主构造器参数类型
    val ddf = df.as[Patient]


    ddf.createOrReplaceTempView("pp")



    //  Find all the patients whose lastVisitDate between current time and '2012-09-15'
    spark.sql("SELECT * FROM pp WHERE TO_DATE(CAST(unix_timestamp(lastVisitDate,'yyyy-MM-dd') as timestamp)) between '2012-09-15' and current_timestamp() order by lastVisitDate").show
    //  Find all the patients who born in 2011
    spark.sql("select * from pp where dateOfBirth like '2011%'").show
    //  Find all the patients age
    spark.sql("select name , datediff(current_date(),to_date(cast(unix_timestamp(dateOfBirth,'yyyy-MM-dd') as timestamp)))/365 as age from pp").show
    //  List patients whose last visited more than 60 days ago
    spark.sql("select * from pp where  datediff(current_date(),to_date(cast(unix_timestamp(lastVisitDate,'yyyy-MM-dd') as timestamp)))/356 > 60").show
    //  Select patients 18 years old or younger
    spark.sql("select name,DATE_SUB(current_date(), 18*365) from pp ").show


  }

}
