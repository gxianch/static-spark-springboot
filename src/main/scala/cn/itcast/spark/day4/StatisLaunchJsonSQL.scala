package cn.itcast.spark.day4

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.json.JSONObject

/**
  * Created by root on 2016/5/19.
  */
object StatisLaunchJsonSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
     val sdf = new SimpleDateFormat("yyyyMMdd")
//    val file = new File("E:\\BaiduNetdiskDownload\\201812\\")
//    val files = file.listFiles()
//    import scala.collection.JavaConversions._
//    for (subfile <- files) {
//        println(subfile.getAbsolutePath)
//        println(subfile.getName)
//      val launchRdd = sc.textFile(subfile.getAbsolutePath).map(line =>{
//              val jsonObject = new JSONObject(line)
//              val mac = jsonObject.getString("mac")
//              Launch1(mac,subfile.getName().substring(0,7))
//            })
//    }
//    val launchRdd = sc.textFile(subfile.getAbsolutePath).map(line =>{
//      val jsonObject = new JSONObject(line)
//      val mac = jsonObject.getString("mac")
//      Launch1(mac,subfile.getName().substring(0,7))
//    })
//    launchRdd.foreach(println)
    val launchFileRdd = sc.textFile("E:\\BaiduNetdiskDownload\\201813\\*launch.log")
    val launchRdd =launchFileRdd.map(line =>{
      val jsonObject = new JSONObject(line)
      val mac = jsonObject.getString("mac")
      Launch1(mac,sdf.format(new Date()))
    })
    launchRdd.foreach(println)
//    import sqlContext.implicits._
//    val launchDf = launchRdd.toDF
//
//    launchDf.createOrReplaceTempView("launch")
//      val total= sqlContext.sql("select count(*) from launch group by date ").count()
//      println(total)
//       sqlContext.sql("set total=6")
//    根据总天数计算开机率,求月开机率传入月日志即可，求三个月开机率传入三个月的日志
//      sqlContext.sql(s"select d.mac,count(*) count,count(*)/${total} as rate from (select distinct date,  mac from launch) d group by d.mac order by count desc").show()


//   var count= sqlContext.sql("select * from launch order by mac ").count()
//    println(count)
//    count= sqlContext.sql("select distinct mac,date from launch order by mac ").count()
//    println(count)
      //计算开机率
      //根据日期去重获取mac地址
  //  sqlContext.sql("select distinct date,  mac from launch order by date").show()
 //  val df= sqlContext.sql("select distinct date,  mac from launch order by date")
   // df.join("select count(*) from launch group by date")
    //每个盒子的开机数
//    sqlContext.sql("select d.mac,count(*) count from " +
//      " (select distinct date,  mac from launch) d group by d.mac order by count desc").show()

    //获取所有日期天数
  //  val total= sqlContext.sql("select count(*) from launch group by date ").count()
  //  println(total)
 //   sqlContext.sql("set total=6")
    //根据总天数计算开机率,求月开机率传入月日志即可，求三个月开机率传入三个月的日志
  //  sqlContext.sql(s"select d.mac,count(*) count,count(*)/${total} as rate from (select distinct date,  mac from launch) d group by d.mac order by count desc").show()

    // MapOutputTrackerMasterEndpoint stopped! 不支持
//    sqlContext.sql("select p.mac,p.count," +
//      "from (select d.mac,count(*) count,c.total from (select distinct date,  mac from launch) d left join (select count(*) total from launch group by date) c as total group by d.mac) p order by p.count desc").show()
//    sqlContext.sql("SELECT p.mac,p.count from (SELECT d.mac, count(*) count FROM ( SELECT DISTINCT date, mac FROM launch ) d   GROUP BY d.mac ) p ORDER BY p.count DESC")

    sc.stop()

  }
}

//case class Play(mac: String, version: String, typecode: String,sp: String,region: String,area: String,
//                mediaName:String,columnName:String,contentName:String,contentId:Int,definition:String,
//                mediaId:Int,columnId:Int,showTimeLength:Long,endPlayTime:String,startPlayTime:String,
//                playType:Int,playTimeLength:Long,chargeType:Int
//               )
case class Launch1(mac: String, date:String)