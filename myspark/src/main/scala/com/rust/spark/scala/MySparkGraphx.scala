package com.rust.spark.scala

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Takho
  */
object MySparkGraphx {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MySparkGraphx").setMaster("local[4]")
    val sc = new SparkContext(conf)


    class VertexProperty()
    case class UserProperty(name: String) extends VertexProperty
    case class ProductProperty(name: String, price: Double) extends VertexProperty

    // user和顶点关联
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))


    // rdd和变关联
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))


    // 关系没有user时，使用的默认用户
    val defaultUser = ("John Doe", "Missing")


    // 初始化图
    val graph = Graph(users, relationships, defaultUser)

    graph.vertices.filter { case (id: VertexId, (name: String, pos: String)) => pos == "postdoc" }.count()


  }
}
