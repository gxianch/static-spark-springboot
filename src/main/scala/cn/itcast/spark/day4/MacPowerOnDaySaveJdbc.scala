package cn.itcast.spark.day4

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  *
  * 
  */
object MacPowerOnDaySaveJdbc {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
 //   sc.setLogLevel("warn")

    val mac_power_on = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/macPowerOnDaySave.parquet`")
    mac_power_on .show()
    mac_power_on.createOrReplaceTempView("mac_power_on")
   // 支持4中写入方式Append、Overwrite、ErrorIfExists、Ignore
    mac_power_on.write.mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:mysql://120.24.162.19:3306/statis-spark")
      .option("dbtable", "mac_power_on")
      .option("user", "root")
      .option("password", "dp20160706")
      .option("driver", "com.mysql.jdbc.Driver")
      .save()

    sc.stop()
  }
}
