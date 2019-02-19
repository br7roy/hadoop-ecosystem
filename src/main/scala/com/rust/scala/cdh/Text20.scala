package com.rust.scala.cdh

/**
  * @author Rust
  */
object Text20 extends Init {
  def main(args: Array[String]): Unit = {

    case class People(name: String, age: Init,
                      gender: String)
    val people = new StringBuilder
    people.append("{'name':'John', 'age':28,'gender':'M'}")
    people.append("{'name':'Lolita', 'age':33,'gender':'F'}")
    people.append("{'name':'Dont Know', 'age':18,'gender':'T'}")
    val peopleRDD = sc.parallelize(people.toList)

    var st = (0, 0)
    peopleRDD.aggregate(st)((x, y) => (x(0) + y("age")), x(1) + 1))


  }

}
