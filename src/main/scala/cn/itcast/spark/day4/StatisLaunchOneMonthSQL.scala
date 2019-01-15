package cn.itcast.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by root on 2016/5/19.
  * 一个月的开机率
  */
object StatisLaunchOneMonthSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")

 //   customer_user.show()
  //  customer_user.cache()

    val launchRdd = sc.textFile("E:\\BaiduNetdiskDownload\\data\\201812\\launch201812.log").map(line =>{
      val fields = line.split("\001")
      Launch(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5))
    })

    import sqlContext.implicits._
    val launchDf = launchRdd.toDF

    launchDf.createOrReplaceTempView("launch")

//   var count= sqlContext.sql("select * from launch order by mac limit 10 ")
//    count.write.format("csv").save("launch")
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
  val countRDD = sqlContext.sql("select count(*) from launch group by date ")
//    countRDD.printSchema()

    //获取所有日期天数
    val total= countRDD.count()
 //   println(total)
 //   sqlContext.sql("set total=6")
    //根据总天数计算开机率,求月开机率传入月日志即可，求三个月开机率传入三个月的日志
  val maccount =  sqlContext.sql(s"select d.mac,count(*) count,count(*)/${total} as rate   from (select distinct date,  mac from launch) d group by d.mac order by count desc")
    //增加一列表示月份
//    import org.apache.spark.sql.functions._
//    val  macrate =maccount.withColumn("month",lit("201812"))

    maccount.show()
    //写入csv
//      .write.format("csv").save("launch")
    //左外连接 MAC有重复
//   val customer_mac_join_df= customer_user.join(macrate, customer_user("mac") === macrate("mac"), "left")
//    因此，网上有很多关于如何在JOIN之后删除列的，后来经过仔细查找，才发现通过修改JOIN的表达式，
// 完全可以避免这个问题。而且非常简单。主要是通过Seq这个对象来实现。
//   val customer_mac_join_df= customer_user.join(macrate, Seq("mac"), "left")
//    customer_mac_join_df.show()
//    customer_mac_join_df.write.format("parquet")
//    //  .mode(SaveMode.Append)
//      .save("src\\main\\resources\\customer_mac_join.parquet")
//    sc.stop()

  }
}

//case class Play(mac: String, version: String, typecode: String,sp: String,region: String,area: String,
//                mediaName:String,columnName:String,contentName:String,contentId:Int,definition:String,
//                mediaId:Int,columnId:Int,showTimeLength:Long,endPlayTime:String,startPlayTime:String,
//                playType:Int,playTimeLength:Long,chargeType:Int
//               )