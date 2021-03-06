package com.rczl.service

import com.rczl.config.JedisConnectionPool
import com.rczl.entity.Play
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by root on 2016/5/19.
  */
object Test {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val mediaId = 20300
    val logfile ="E:\\data\\2019\\1\\20190116play-vod.log"
    val playRdd = sc.textFile(logfile).map(line =>{
      val fields = line.split("\001")
      Play(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5),
        fields(6).toInt,fields(7), fields(8).toInt,fields(9), fields(10).toInt, fields(11),
        fields(12).toLong, fields(13),fields(14),
        fields(15).toLong, fields(16).toInt,fields(17).toInt,fields(18)
      )
    })
    import sqlContext.implicits._
    val playDf = playRdd.toDF

    playDf.createOrReplaceTempView("play")
    //    playDf.show()
  val timerdd =  playDf.sqlContext.sql(s"select mediaId,mediaName,substr(month,9,2) as hour from play where mediaId = ${mediaId} ")
    timerdd.write.mode(SaveMode.Overwrite).format("csv")
          .save("src\\main\\resources\\timerdd.csv")
    val resultDf = playDf.sqlContext.sql(s"select m.mediaId,m.mediaName,m.hour,count(*)  count from (select mediaId,mediaName,substr(endPlayTime,12,2) as hour from play where mediaId = ${mediaId} ) m group by m.mediaId,m.mediaName,m.hour order by m.hour desc")
    resultDf.write.mode(SaveMode.Overwrite).format("csv")
      .save("src\\main\\resources\\resultDf.csv")
    //    val a = resultDf.collect()
    //    import scala.collection.JavaConverters._
    //    val b = a.toList
    //    var c = b.toString()
    //结束执行并打印后怎么执行这个语句，mediaId = List([17536,淑女之家,23,1], [17536,淑女之家,2...
    //    select m.mediaId,m.mediaName,m.hour,count(*)  count from (select mediaId,mediaName,substr(startPlayTime,12,2) as hour from play where mediaId = List([17536,淑女之家,23,1], [17536,淑女之家,22,1], [17536,淑女之家,21,2], [17536,淑女之家,20,2], [17536,淑女之家,19,2], [17536,淑女之家,18,1], [17536,淑女之家,12,1], [17536,淑女之家,11,1], [17536,淑女之家,10,2])) m group by m.mediaId,m.mediaName,m.hour order by m.hour desc
    //      ----------------------------------------------------^^^
    val jedis = JedisConnectionPool.getContion()
    jedis.del("mediaId:"+mediaId)
    resultDf.collect().foreach(row => {
      val line = row.toString()
      val data = line.substring(1,line.indexOf("]"))
      val field =data.split(",")

      jedis.rpush("mediaId:"+field(0),field(1)+","+field(2)+","+field(3))
    }
    )
    jedis.close()
    sc.stop()
  }
}
