package com.rust.scala.cdh

/**
  * @author Rust
  */
object Text9 extends Init {

  //3070811,1963,1096,,"US","CA",,1,
  //3022811,1963,1096,,"US","CA",,1,56
  //3033811,1963,1096,,"US","CA",,1,23
  //Below is the code snippet to process this tile.
  //val field= sc.textFile("spark15/f ilel.txt")
  //val mapper = field.map(x=> A)
  //mapper.map(x => x.map(x=> {B})).collect
  //Please fill in A and B so it can generate below final output
  //Array(Array(3070811,1963,109G, 0, "US", "CA", 0,1, 0)
  //,Array(3022811,1963,1096, 0, "US", "CA", 0,1, 56)
  //,Array(3033811,1963,1096, 0, "US", "CA", 0,1, 23)
  def main(args: Array[String]): Unit = {

    super.init()
    //        val field = sc.textFile("D:\\project\\gitlab\\spark-exercise\\src\\main\\resources\\t9.txt")
    val field = sc.textFile("/user/ubuntu/t9.txt")
    val mapper = field.map(x => x.split(","))
    val res = mapper.map(x => x.map(x => {
      if (x.isEmpty) 0 else x
    })).collect()


  }

}
