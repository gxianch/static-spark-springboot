package cn.itcast.spark.day4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  * 保存酒店mac表到本地v_customer_user
  */
object CustormerUserReadSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val customer_user = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/customer_user.parquet`")
    customer_user.createOrReplaceTempView("v_customer_user")
    customer_user.show()
    sc.stop()
  }
}
