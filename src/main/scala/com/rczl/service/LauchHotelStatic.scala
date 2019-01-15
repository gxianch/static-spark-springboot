package com.rczl.service

import com.rczl.entity.Launch
import org.apache.spark.sql.{SQLContext, SparkSession}

class LauchHotelStatic(var sc:SparkSession,val filepath:String, val logDate:String){
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
  //  customer_user.show()
    //获取总的酒店数
    val totalHotelNum =  customer_user.sqlContext.sql("select count(*) from (select distinct customerId from v_customer_user)").head().getLong(0)

    val launchRdd = sc.sparkContext.textFile(filepath+logDate+".log").map(line =>{
      val fields = line.split("\001")
      Launch(fields(0), fields(1), fields(2),fields(3), fields(4), fields(5))
    })
    import sqlContext.implicits._
    val launchDf = launchRdd.toDF

    launchDf.createOrReplaceTempView("launch")
  //  launchDf.show()
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
    mac_power_on.createOrReplaceTempView("mac_power_on")
  //  mac_power_on.show()
    val yearmonth = mac_power_on.sqlContext.sql("select month from mac_power_on limit 1").head().getString(0)
    val customer_mac_join_df= customer_user.join(mac_power_on, Seq("mac"), "left")
    customer_mac_join_df.createOrReplaceTempView("customer_mac_join")
    //左外连接右边count为NULL值，需要设置为0
    val nvl_join =  customer_mac_join_df.sqlContext.sql(s"select  j.mac,j.id,j.provinceId,j.cityId,j.areaCode,j.customerId,j.customerName,j.status," +
      "nvl(j.powerOnDayNum,0) count,nvl(j.powerOnRate,0) rate from customer_mac_join j order by customerId")
    //  nvl_join.show()
    nvl_join.createOrReplaceTempView("nvl_join")
    //   nvl_join.sqlContext.sql("select customerId,sum(count) as total from nvl_join  group by customerId order by total desc")
    val hotelMonthRate= nvl_join.sqlContext.sql("select customerId,count(*) macNum,sum(count) as dayNum,round(sum(count)/count(*)/30,2) as hotelMonthRate from nvl_join  group by customerId ")
    // val customerIdTotal= nvl_join.sqlContext.sql("select hr.*,cm.customerName from (select customerId,count(*) macNum,sum(count) as dayNum,round(sum(count)/count(*)/30,2) as hotelMonthRate from nvl_join  group by customerId) hr inner join customer_mac_join cm where hr.customerId=cm.customerId ")
    //      .show()
    hotelMonthRate.createOrReplaceTempView("hotelMonthRate")
    val zero= hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate=0").head().getLong(0)
    val threeNum=    hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate<0.3 and hotelMonthRate>0").head().getLong(0)
    val sixNum=  hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate>=0.3 and hotelMonthRate<0.6").head().getLong(0)
    val highsixNum =   hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate>=0.6").head().getLong(0)

    import redis.clients.jedis.Jedis
    val jedis = new Jedis("111.23.6.233",23308)
    val Value = "{\"total\":"+totalHotelNum+
      ",\"zero\":" +zero+
      ",\"three\":"+threeNum +
      ",\"six\":"+sixNum +
      ",\"one\":"+highsixNum +
      "}"
    jedis.hset("powerOnRate:hotel",yearmonth,Value)
    jedis.close()

    return Value
  }
}


