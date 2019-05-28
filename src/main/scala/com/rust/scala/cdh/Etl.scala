package com.rust.scala.cdh

import java.sql.Timestamp
import java.util.UUID

import com.rust.scala.cdh.Etl.spark
import org.apache.spark.sql.{Encoder, Encoders, Row, SaveMode}

/**
	* 读取csv数据，添加字段，存入mysql
	*
	* @author Rust
	*/
object Etl extends Init {


	def main(args: Array[String]): Unit = {
		case class Market(object_id: String, market_type: String, cycle_type: String, startdate: String, enddate: String,
		                  market_feature: String, create_user_id: String, create_time: Timestamp, update_user_id: String,
		                  update_time: Timestamp, delete_flag: Int)


		val path = "file:\\c:\\users\\futanghang\\desktop\\a1.csv"
		val rdd1 = spark.read.option("header", value = true).csv(path)



		val tRd = rdd1.rdd.map(r => Market(UUID.randomUUID().toString, "G", "1", r.getString(0).replaceAll("/", ""), r
      .getString(1)
			.replaceAll("/", ""), r.getString(2), "system", new Timestamp(System.currentTimeMillis()), "system"
			, new Timestamp(System.currentTimeMillis()), 0))
		implicit val rr = Encoders.kryo(classOf[Market])
		val df = spark.createDataFrame(tRd)
		df.write.format("jdbc").mode(SaveMode.Overwrite).option("url",
      "jdbc:mysql://localhost:3306/fof?useUnicode=true&characterEncoding=utf-8").option("dbtable", "fof" +
      ".fof_market_cycle").option("user", "root").option("password", "root").option("batchsize", 1000).option("numPartitions", "20").save()

		val path2 = "file:\\c:\\users\\futanghang\\desktop\\a2.csv"
		val rdd2 = spark.read.option("header", value = true).csv(path2)

		val tRd2 = rdd2.rdd.map(r => Market(UUID.randomUUID().toString, "G", "2", r.getString(0).replaceAll("/", ""), r
      .getString(1)
			.replaceAll("/", ""), r.getString(2), "system", new Timestamp(System.currentTimeMillis()), "system"
			, new Timestamp(System.currentTimeMillis()), 0))
		val df2 = spark.createDataFrame(tRd2)
		df2.write.format("jdbc").mode(SaveMode.Append).option("url",
      "jdbc:mysql://localhost:3306/fof?useUnicode=true&characterEncoding=utf-8").option("dbtable", "fof" +
      ".fof_market_cycle").option("user", "root").option("password", "root").option("batchsize", 1000).option("numPartitions", "20").save()

	}

}
