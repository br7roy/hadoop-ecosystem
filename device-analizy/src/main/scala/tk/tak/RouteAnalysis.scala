package tk.tak

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
 * 轨迹分析
 *
 * @author Tak
 */
object RouteAnalysis {

  case class MacInfo(mac: String, scode: String, stime: Long, etime: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("test").setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)
    val p1 = "E:\\dev\\esfile\\mainRdd.txt"
//    val p2 = "E:\\dev\\esfile\\mingzhong.txt"
//    val p2 = "E:\\dev\\esfile\\weimingzhong.txt"
    val p2 = "E:\\dev\\esfile\\otherRdd.txt"

    var mainRdd = sc.textFile(p1).map(r => r.split(",")).map(r => MacInfo(r(0), r(1), r(2).toLong, r(3).toLong)).map(r => ((r.mac + "&" + r.scode + "&" + r.stime), r)).combineByKey(List(_), (x: List[MacInfo], y: MacInfo) => y :: x, (x: List[MacInfo], y: List[MacInfo]) => x ::: y).sortBy(r => r._2.head.stime)
    var otherRdd = sc.textFile(p2).map(r => r.split(",")).map(r => MacInfo(r(0), r(1), r(2).toLong, r(3).toLong)).map(r => ((r.mac + "&" + r.scode + "&" + r.stime), r)).combineByKey(List(_), (x: List[MacInfo], y: MacInfo) => y :: x, (x: List[MacInfo], y: List[MacInfo]) => x ::: y).sortBy(r => r._2.head.stime)

    var arr = mainRdd.collect

    val lagTime = 60000L
    val extTime = 60000L
    var rd: RDD[(String, List[MacInfo])] = null
    for (mGuiji <- arr) {
      breakable {
        val tmp = otherRdd.filter(r => {
          mGuiji._1.substring(mGuiji._1.indexOf("&") + 1).equals(r._1.substring(r._1.indexOf("&") + 1))
        }).filter(closeOtherRoute => {
          // 紧密附属轨迹的开始 大于 著轨迹的开始时间 - 前端传来的允许误差时间
          closeOtherRoute._2.head.stime >= mGuiji._2.head.stime - extTime &&
            // 紧密附属轨迹的结束时间 小于 著轨迹的结束时间 + 前端出来的允许误差时间
            closeOtherRoute._2.last.etime <= mGuiji._2.last.etime + extTime
        }).filter(fushuGuiji => {
          val elapse = mGuiji._2.last.etime - mGuiji._2.head.stime
          val fushuElapse = fushuGuiji._2.last.etime - fushuGuiji._2.head.stime
          if (elapse == 0) {
            // 附属轨迹要比著轨迹-延迟时间的开始时间要晚
            mGuiji._2.head.stime - lagTime <= fushuGuiji._2.head.stime &&
              // 附属轨迹要比朱轨迹+ 延迟时间的开始时间要早
              fushuGuiji._2.head.stime <= mGuiji._2.head.stime + lagTime &&
              // 附属轨迹的场所内持续时间也要等于0
              fushuGuiji._2.last.etime - fushuGuiji._2.head.stime == 0 &&
              // 附属轨迹的出现次数要登陆著轨迹的出现次数
              fushuGuiji._2.size == mGuiji._2.size
          } else {
            var mMinElapse = elapse - 2 * lagTime
            if (mMinElapse < 0) mMinElapse = 0L
            var mMaxElapse = elapse + 2 * lagTime
            // 附属轨迹要比著轨迹-延迟时间的开始时间要晚
            mGuiji._2.head.stime - lagTime <= fushuGuiji._2.head.stime &&
              // 附属轨迹要比朱轨迹+ 延迟时间的开始时间要早
              fushuGuiji._2.head.stime <= mGuiji._2.head.stime + lagTime &&
              // 附属轨迹的场所内持续时间必须要 等于 著轨迹的持续时间加上2倍的延迟时间
            mMinElapse <= fushuElapse &&
              fushuElapse <= mMaxElapse &&
              // 附属轨迹的出现次数要登陆著轨迹的出现次数
              fushuGuiji._2.size == mGuiji._2.size
          }
        })

        if (tmp.isEmpty()) {
          break()
        }
        if (rd != null) {
          rd = rd.union(tmp)
        } else rd = tmp
      }
    }
    println("------------------:"+rd.count())
    var nRd = rd.distinct()
    println("------------------:"+nRd.count())
    nRd.collect.foreach(println)
    mainRdd = mainRdd.map(r => (r._1.substring(r._1.indexOf("&")+1),r._2))
    nRd = nRd.map(r => (r._1.substring(r._1.indexOf("&")+1),r._2))
    val res = mainRdd.cogroup(nRd).filter( r => r._2._1.nonEmpty)
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    res.collect.foreach(println)


  }
object ReduceRes{
  def main(args: Array[String]): Unit = {

  val conf = new SparkConf().setAppName("test").setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)
    val rdd = sc.parallelize(Array(("38-2B-CB-26-72-BD&37021239000098&1550136969",List(MacInfo("38-2B-CB-26-72-BD","37021239000098",1550136969,1550136969), MacInfo("38-2B-CB-26-72-BD","37021239000098",1550136969,1550136969))),
      ("38-2B-CB-26-72-BD&37021239000245&1550136982",List(MacInfo("38-2B-CB-26-72-BD","37021239000245",1550136982,1550136982), MacInfo("38-2B-CB-26-72-BD","37021239000245",1550136982,1550136982))),
      ("E4-9A-DC-F1-0A-B3&37021239000247&1550137071",List(MacInfo("E4-9A-DC-F1-0A-B3","37021239000247",1550137071,1550137071), MacInfo("E4-9A-DC-F1-0A-B3","37021239000247",1550137071,1550137071))),
      ("38-2B-CB-26-72-BD&37021239000089&1550137073",List(MacInfo("38-2B-CB-26-72-BD","37021239000089",1550137073,1550137073), MacInfo("38-2B-CB-26-72-BD","37021239000089",1550137073,1550137073))),
      ("38-2B-CB-26-72-BD&37021239000246&1550137347",List(MacInfo("38-2B-CB-26-72-BD","37021239000246",1550137347,1550137347), MacInfo("38-2B-CB-26-72-BD","37021239000246",1550137347,1550137347))),
      ("A4-04-50-2C-19-1F&37021239000249&1550137448",List(MacInfo("A4-04-50-2C-19-1F","37021239000249",1550137448,1550137448), MacInfo("A4-04-50-2C-19-1F","37021239000249",1550137448,1550137448))),
      ("38-2B-CB-26-72-BD&37021239000249&1550137448",List(MacInfo("38-2B-CB-26-72-BD","37021239000249",1550137448,1550137448), MacInfo("38-2B-CB-26-72-BD","37021239000249",1550137448,1550137448))),
      ("E0-EE-1B-D2-2E-12&37021239000249&1550137448",List(MacInfo("E0-EE-1B-D2-2E-12","37021239000249",1550137448,1550137448), MacInfo("E0-EE-1B-D2-2E-12","37021239000249",1550137448,1550137448))),
      ("38-2B-CB-26-72-BD&37021227120517&1550137699",List(MacInfo("38-2B-CB-26-72-BD","37021227120517",1550137699,1550137699), MacInfo("38-2B-CB-26-72-BD","37021227120517",1550137699,1550137699))),
      ("30-84-54-D5-1E-79&37021227120517&1550137699",List(MacInfo("30-84-54-D5-1E-79","37021227120517",1550137699,1550137699), MacInfo("30-84-54-D5-1E-79","37021227120517",1550137699,1550137699))),
      ("9C-FB-D5-5C-85-94&37021227120517&1550137699",List(MacInfo("9C-FB-D5-5C-85-94","37021227120517",1550137699,1550137699), MacInfo("9C-FB-D5-5C-85-94","37021227120517",1550137699,1550137699))),
      ("C4-0B-CB-1B-09-DA&37021227120517&1550137699",List(MacInfo("C4-0B-CB-1B-09-DA","37021227120517",1550137699,1550137699), MacInfo("C4-0B-CB-1B-09-DA","37021227120517",1550137699,1550137699))),
      ("38-2B-CB-26-72-BD&37021224120228&1550137783",List(MacInfo("38-2B-CB-26-72-BD","37021224120228",1550137783,1550137783), MacInfo("38-2B-CB-26-72-BD","37021224120228",1550137783,1550137783))),
      ("C4-82-5C-B8-89-9A&37021224120228&1550137783",List(MacInfo("C4-82-5C-B8-89-9A","37021224120228",1550137783,1550137783), MacInfo("C4-82-5C-B8-89-9A","37021224120228",1550137783,1550137783))),
      ("38-2B-CB-26-72-BD&37021239000091&1550137833",List(MacInfo("38-2B-CB-26-72-BD","37021239000091",1550137833,1550137833), MacInfo("38-2B-CB-26-72-BD","37021239000091",1550137833,1550137833))),
      ("38-2B-CB-26-72-BD&37021239000246&1550137964",List(MacInfo("38-2B-CB-26-72-BD","37021239000246",1550137964,1550137964), MacInfo("38-2B-CB-26-72-BD","37021239000246",1550137964,1550137964))),
      ("E0-EE-1B-CD-8A-4D&37021239000247&1550137988",List(MacInfo("E0-EE-1B-CD-8A-4D","37021239000247",1550137988,1550137988), MacInfo("E0-EE-1B-CD-8A-4D","37021239000247",1550137988,1550137988))),
      ("38-2B-CB-26-72-BD&37021239000247&1550137988",List(MacInfo("38-2B-CB-26-72-BD","37021239000247",1550137988,1550137988), MacInfo("38-2B-CB-26-72-BD","37021239000247",1550137988,1550137988))),
      ("38-2B-CB-26-72-BD&37021239000245&1550138115",List(MacInfo("38-2B-CB-26-72-BD","37021239000245",1550138115,1550138115), MacInfo("38-2B-CB-26-72-BD","37021239000245",1550138115,1550138115))),
      ("38-2B-CB-26-72-BD&37021239000098&1550138154",List(MacInfo("38-2B-CB-26-72-BD","37021239000098",1550138154,1550138154), MacInfo("38-2B-CB-26-72-BD","37021239000098",1550138154,1550138154)))))
    val goupRdd = rdd.groupBy(r => r._1.substring(0,r._1.indexOf("&")))


  }


}
}
