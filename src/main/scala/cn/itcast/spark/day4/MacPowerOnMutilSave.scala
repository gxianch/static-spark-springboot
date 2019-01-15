package cn.itcast.spark.day4

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  * 盒子一个月的开机率
  * 根据月份日志修改textFile路径,一次读取一个月的数据
  * 然后统计这个月的开机天数，开机率，存入JDBC,采用Append模式追加，每一个月的日志不能执行多次
  */
object MacPowerOnMutilSave {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MacCountSaveOneMonth").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val launchRdd = sc.textFile("E:\\BaiduNetdiskDownload\\data\\month\\*.log").map(line =>{
      val fields = line.split("\001")
      Launch(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5))
    })
    import sqlContext.implicits._
    val launchDf = launchRdd.toDF
    launchDf.createOrReplaceTempView("launch")
    launchDf.write.mode(SaveMode.Overwrite).format("parquet").save("src\\main\\resources\\launch.parquet")
  val countRDD = sqlContext.sql("select count(*) from launch group by date ")
    import org.apache.spark.sql.functions._
  val addMonthCol =  sqlContext.sql("select * from launch")
    //增加一列表示月份
  val addMonth=  addMonthCol.withColumn("month",lit(substring(addMonthCol("date"),0,6)))
    addMonth.createOrReplaceTempView("launchAddMonth")
    //获取所有日期天数
    val total= countRDD.count()
    //根据总天数计算开机率,
  val mac_power_on =  sqlContext.sql(s"select d.mac,count(*) powerOnDayNum,round(count(*)/${total} ,3) as powerOnRate, d.month  from (select distinct date,  mac,month from launchAddMonth) d group by d.mac , d.month order by powerOnDayNum desc")
    mac_power_on.write.mode(SaveMode.Overwrite).format("parquet").save("src\\main\\resources\\macPowerOnMutilSave.parquet")
    sc.stop()
  }
}
