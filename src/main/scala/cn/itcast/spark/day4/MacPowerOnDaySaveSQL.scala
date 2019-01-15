package cn.itcast.spark.day4

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  * 盒子一个月的开机率
  * 根据月份日志修改textFile路径,一次读取一个月的数据
  * 然后统计这个月的开机天数，开机率，存入JDBC,采用Append模式追加，每一个月的日志不能执行多次
  */
object MacPowerOnDaySaveSQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MacCountSaveOneMonth").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val launchRdd = sc.textFile("E:\\BaiduNetdiskDownload\\data\\month\\201812.log").map(line =>{
      val fields = line.split("\001")
      Launch(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5))
    })
    import sqlContext.implicits._
    val launchDf = launchRdd.toDF
    launchDf.createOrReplaceTempView("launch")
 //   launchDf.write.mode(SaveMode.Overwrite).format("parquet").save("src\\main\\resources\\launch.parquet")
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
    import org.apache.spark.sql.functions._
  val addMonthCol =  sqlContext.sql("select * from launch")
    //增加一列表示月份
  val addMonth=  addMonthCol.withColumn("month",lit(substring(addMonthCol("date"),0,6)))
    addMonth.createOrReplaceTempView("launchAddMonth")
    //获取所有日期天数
    val total= countRDD.count()
    //   println(total)
 //   sqlContext.sql("set total=6")
    //根据总天数计算开机率,///* order by count desc*/
  val mac_power_on =  sqlContext.sql(s"select d.mac,count(*) powerOnDayNum,round(count(*)/${total} ,3) as powerOnRate, d.month  from (select distinct date,  mac,month from launchAddMonth) d group by d.mac , d.month ")
   // maccount.show()
    //mac,powerOnDayNum,powerOnRate,month
    //数据最适合存储到Hbase,键值对存储，以mac和month为键,但是硬件资源条件限制
    //存储到JDBC
    mac_power_on.write.mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:mysql://120.24.162.19:3306/statis-spark")
      .option("dbtable", "mac_power_on")
      .option("user", "root")
      .option("password", "dp20160706")
      .option("driver", "com.mysql.jdbc.Driver")
      .save()
    //保存到本地
//    mac_power_on.write.mode(SaveMode.Append).format("csv")
//      .save("src\\main\\resources\\macPowerOnDaySave.csv")

  //  mac_power_on.write.mode(SaveMode.Overwrite).format("parquet").save("src\\main\\resources\\macPowerOnDaySave.parquet")
    sc.stop()
  }
}
