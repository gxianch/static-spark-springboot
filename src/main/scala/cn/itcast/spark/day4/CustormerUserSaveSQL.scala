package cn.itcast.spark.day4

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  * 保存酒店mac表到本地v_customer_user
  */
object CustormerUserSaveSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[4]")
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
   // customer_user.show()
    customer_user.write.format("parquet").save("src\\main\\resources\\customer_user.parquet")
    sc.stop()
  }
}
