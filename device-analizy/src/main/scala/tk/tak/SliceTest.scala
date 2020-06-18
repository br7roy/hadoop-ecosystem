package tk.tak

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 讲rdd切片转换dataframe
 *
 * @author fth
 */
object SliceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val data = sc.textFile("haha.text")
    //    全量数据集合
    val arr = data.collect()
    //    剔除第一行字段名
    val row = arr.slice(1, arr.length)
    //    形成rdd
    val rdd = sc.parallelize(row)

    //    列名
    val schema = StructType({
      val sp = arr(0).split(",")
      val res = sp.map(fieldName => {
        StructField(fieldName, StringType, true)
      })
      res
    })
    //    加工rdd,以逗号分裂字符串包装成Row类型
    val rowRdd = rdd.flatMap(text => text.split(",")).map(p => Row(p: _*))
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(rowRdd, schema)
    df.show


  }

}
