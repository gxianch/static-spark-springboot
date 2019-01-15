package com.rczl.service

import com.rczl.entity.Launch
import org.apache.spark.sql.{SQLContext, SparkSession}
import redis.clients.jedis.Jedis
/**
  * Created by root on 2016/5/14.
  */
class LauchStatic(var sc:SparkSession,val filepath:String,val logDate:String){
  def apply(): String ={
    val sqlContext = new SQLContext(sc.sparkContext)
    val jdbcDF = sqlContext.read
    .format("jdbc")
    .option("url", "jdbc:mysql://120.24.162.19:3306/statis-spark")
    .option("dbtable", "v_customer_user")
    .option("user", "root")
    .option("password", "dp20160706")
    .option("driver", "com.mysql.jdbc.Driver")
    .load()
    jdbcDF.createOrReplaceTempView("v_customer_user")
    val customer_user= jdbcDF.sqlContext.sql("select v.id,v.provinceId,v.cityId,v.countyId," +
      "v.areaCode,v.customerId,v.customerName,regexp_replace(v.mac, ':', '') as mac,v.status from v_customer_user v")
    customer_user.show()
    val macTotalNum =  customer_user.sqlContext.sql("select count(*) from (select distinct mac from v_customer_user)")
      .head().getLong(0)
    val launchRdd = sc.sparkContext.textFile(filepath+logDate+".log").map(line =>{
      val fields = line.split("\001")
      Launch(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5))
    })
    import sqlContext.implicits._
    val launchDf = launchRdd.toDF

    launchDf.createOrReplaceTempView("launch")
    launchDf.show()
    //每个盒子的开机数
    val countRDD = sqlContext.sql("select count(*) from launch group by date ")
    import org.apache.spark.sql.functions._
    val addMonthCol =  sqlContext.sql("select * from launch")
    //增加一列表示月份
    val addMonth=  addMonthCol.withColumn("month",lit(substring(addMonthCol("date"),0,6)))
    addMonth.createOrReplaceTempView("launchAddMonth")
    //获取所有总天数
    val total= countRDD.count()
    //根据总天数计算开机率
    val mac_power_on =  sqlContext.sql(s"select d.mac,count(*) powerOnDayNum,round(count(*)/${total} ,3) as powerOnRate, d.month  from (select distinct date,  mac,month from launchAddMonth) d group by d.mac , d.month ")
    mac_power_on.createOrReplaceTempView("mac_count")
    mac_power_on.show()
    val month = mac_power_on.sqlContext.sql("select month from mac_count limit 1").head().getString(0)
    //没有开机的就没有发送数据，由总数减去不为0
    val openNum = mac_power_on.sqlContext.sql("select count(*) from mac_count where powerOnRate != 0").head().getLong(0)
    val threeNum = mac_power_on.sqlContext.sql("select count(*) from mac_count where powerOnRate<0.3 and powerOnRate>0").head().getLong(0)
    val sixNum=  mac_power_on.sqlContext.sql("select count(*) from mac_count where powerOnRate>=0.3 and powerOnRate<0.6").head().getLong(0)
    val highsixNum=  mac_power_on.sqlContext.sql("select count(*) from mac_count where powerOnRate>=0.6").head().getLong(0)

    val jedis = new Jedis("111.23.6.233",23308)
    val Value = "{\"total\":"+macTotalNum+
      ",\"zero\":" +(macTotalNum-openNum)+
      ",\"three\":"+threeNum +
      ",\"six\":"+sixNum +
      ",\"one\":"+highsixNum +
      "}"
    jedis.hset("powerOnRate:mac",month,Value)
    jedis.close()


    return Value
  }
}


