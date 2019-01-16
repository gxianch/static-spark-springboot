package com.rczl.service

import java.util

import com.rczl.config.JedisConnectionPool
import com.rczl.entity.Play
import com.zhuinden.sparkexperiment.JedisUtil
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by root on 2016/5/14.
  */
class PlayTrackStatic(var sc:SparkSession, val logfile:String,val mediaId: String) {
  def track():Unit = {
    val sqlContext = new SQLContext(sc.sparkContext)
    val playRdd = sc.sparkContext.textFile(logfile).map(line =>{
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
   val resultDf = playDf.sqlContext.sql(s"select m.mediaId,m.mediaName,m.hour,count(*)  count from (select mediaId,mediaName,substr(month,9,2) as hour from play where mediaId = ${mediaId} ) m group by m.mediaId,m.mediaName,m.hour order by m.hour desc")
//    resultDf.show()
//    val a = resultDf.collect()
//    import scala.collection.JavaConverters._
//    val b = a.toList
//    var c = b.toString()
    //结束执行并打印后怎么执行这个语句，mediaId = List([17536,淑女之家,23,1], [17536,淑女之家,2...
//    select m.mediaId,m.mediaName,m.hour,count(*)  count from (select mediaId,mediaName,substr(startPlayTime,12,2) as hour from play where mediaId = List([17536,淑女之家,23,1], [17536,淑女之家,22,1], [17536,淑女之家,21,2], [17536,淑女之家,20,2], [17536,淑女之家,19,2], [17536,淑女之家,18,1], [17536,淑女之家,12,1], [17536,淑女之家,11,1], [17536,淑女之家,10,2])) m group by m.mediaId,m.mediaName,m.hour order by m.hour desc
//      ----------------------------------------------------^^^
    val jedis=  JedisUtil.getJedis()
    jedis.del("mediaId:"+mediaId)
      resultDf.collect().foreach(row => {
        val line = row.toString()
        val data = line.substring(1,line.indexOf("]"))
        val field =data.split(",")
        jedis.rpush("mediaId:"+field(0),field(1)+","+field(2)+","+field(3))
      }
    )
    jedis.expire("mediaId:"+mediaId,60*60*24*7)
    jedis.close()
  }
}


