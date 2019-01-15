package com.rczl.service

import java.util

import com.rczl.entity.{Launch, Play}
import org.apache.spark.sql.{SQLContext, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Created by root on 2016/5/14.
  */
class PlayStatic(var sc:SparkSession, val logfile:String) {
  def rank(): util.List[Play] ={
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
    playDf.show()
    import scala.collection.JavaConverters._
    return playRdd.take(10).toList.asJava
  }
}


