package cn.itcast.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by root on 2016/5/19.
  */
object StatisLaunchMonthSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("static").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val jdbcDF = sqlContext.read
      .format("jdbc")
      .option("url", "jdbc:mysql://120.24.162.19:3306/statis-spark")
      .option("dbtable", "v_customer_user")
      .option("user", "root")
      .option("password", "dp20160706")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    //  jdbcDF.show()
    // println(jdbcDF.count())
    jdbcDF.createOrReplaceTempView("v_customer_user")
    //fxQp3uG3X8bZ	PROV10	CITY27	AREA110	201	fxQp3uG3X8bZ	金逸宾馆	0C:F0:B4:0D:F2:D5	1
    //mac地址带分号:,需要替换
      val customer_user= jdbcDF.sqlContext.sql("select v.id,v.provinceId,v.cityId,v.countyId," +
      "v.areaCode,v.customerId,v.customerName,regexp_replace(v.mac, ':', '') as mac,v.status from v_customer_user v")

    customer_user.show()


    val launchRdd = sc.textFile("E:\\BaiduNetdiskDownload\\data\\month\\").map(line =>{
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
    println(total)
 //   sqlContext.sql("set total=6")
    //根据总天数计算开机率,求月开机率传入月日志即可，求三个月开机率传入三个月的日志
  val maccount =  sqlContext.sql(s"select d.mac,count(*) count,count(*)/${total} as rate   from (select distinct date,  mac from launch) d group by d.mac order by count desc")
    //增加一列表示月份
    import org.apache.spark.sql.functions._
    val  macrate =maccount.withColumn("month",lit("201812"))

    macrate.show()
    //写入csv
//      .write.format("csv").save("launch")
   val customer_mac_join_df= customer_user.join(macrate, customer_user("mac") === macrate("mac"), "left")
    customer_mac_join_df.show()

    sc.stop()

  }
}

//case class Play(mac: String, version: String, typecode: String,sp: String,region: String,area: String,
//                mediaName:String,columnName:String,contentName:String,contentId:Int,definition:String,
//                mediaId:Int,columnId:Int,showTimeLength:Long,endPlayTime:String,startPlayTime:String,
//                playType:Int,playTimeLength:Long,chargeType:Int
//               )