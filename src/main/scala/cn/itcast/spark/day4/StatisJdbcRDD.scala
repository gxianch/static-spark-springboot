package cn.itcast.spark.day4

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZX on 2016/4/12.
  */
object StatisJdbcRDD {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StatisJdbcRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
   //val sqlContext = new HiveContext(sc)
    //读取整个表的数据
    val jdbcDF = new SQLContext(sc).read
      .format("jdbc")
      .option("url", "jdbc:mysql://120.24.162.19:3306/boss")
      .option("dbtable", "v_customer_user")
      .option("user", "root")
      .option("password", "dp20160706")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
  //  jdbcDF.show()
   // println(jdbcDF.count())
    jdbcDF.createOrReplaceTempView("v_customer_user")
    jdbcDF.sqlContext.sql("select * from v_customer_user where customerId='jw9yTA3rSNw0'").show()
    sc.stop()
  }
}
