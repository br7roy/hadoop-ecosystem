package com.xwc

/**
 * @author Administrator
 */
object App extends Init {
  def main(args: Array[String]): Unit = {

    var df = hiveContext.sql("select * from probd_hive.maclog limit 100")
    df.show()





  }

}
