package cn.itcast.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by root on 2016/5/19.
  */
object StatisSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val playRdd = sc.textFile("D:\\2019\\1\\*").map(line =>{
      val fields = line.split("\001")
      Play(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5),
        fields(6), fields(7), fields(8), fields(9).toInt, fields(10),
        fields(11).toInt,fields(12).toInt, fields(13).toLong, fields(14),fields(15),
        fields(16).toInt, fields(17).toLong,fields(18).toInt
      )
    })

    import sqlContext.implicits._
    val playDf = playRdd.toDF

    playDf.createOrReplaceTempView("play")

 //   sqlContext.sql("select * from play order by mac ").show()
//    sqlContext.sql("select * from play where columnId=3 order by mac,mediaId").show()
 //   sqlContext.sql("select hash(p.mac) from play p limit 1").show()
 //   sqlContext.sql("select count(1) from play").show()
    //group by mac 去重
  //  sqlContext.sql("select count(1) from (select mac from play group by mac )").show()
    //媒资观看次数排行
  //  sqlContext.sql("select p.mediaName,p.mediaId ,count(1) count from play p group by p.mediaName,p.mediaId  order by count desc").show()
    //媒资观看时间排行
 //   sqlContext.sql("select p.mediaName,sum(p.playTimeLength) sum from play p group by p.mediaName order by sum desc").show()
  //  sqlContext.sql("select * from play where mediaId = 17536").show()
    //增加一列时间（截取开始时间）
    val mediaId = 17536
    sqlContext.sql(s"select m.mediaId,m.mediaName,m.hour,count(*) count from " +
      s"(select mediaId,mediaName,substr(startPlayTime,12,2) as hour from play where mediaId = ${mediaId}) m group by m.mediaId,m.mediaName,m.hour order by m.hour desc")
      .show()
    sc.stop()

  }
}

case class Play(mac: String, version: String, typecode: String,sp: String,region: String,area: String,
                mediaName:String,columnName:String,contentName:String,contentId:Int,definition:String,
                mediaId:Int,columnId:Int,showTimeLength:Long,endPlayTime:String,startPlayTime:String,
                playType:Int,playTimeLength:Long,chargeType:Int
               )