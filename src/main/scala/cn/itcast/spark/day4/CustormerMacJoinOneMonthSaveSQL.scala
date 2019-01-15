package cn.itcast.spark.day4

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  * 酒店一个月内的的平均开机率  酒店总的盒子开机天数/盒子总数*天数
  * 201812总酒店数:8756
  * 酒店开机率为0的酒店数量:1669
  * 为0<x<0.3的酒店数量:3974
  * 为0.3<=x<0.6的酒店数量:1940
  * >0.6的酒店数量:1173
  */
object CustormerMacJoinOneMonthSaveSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val customer_user = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/customer_user.parquet`")
    customer_user.createOrReplaceTempView("v_customer_user")
    //获取总的酒店数

  val totalHotelNum =  customer_user.sqlContext.sql("select count(*) from (select distinct customerId from v_customer_user)").head().getLong(0)

    val mac_count_month = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/macPowerOnDaySave.parquet`")
    mac_count_month.createOrReplaceTempView("mac_count_month")
    val yearmonth = mac_count_month.sqlContext.sql("select month from mac_count_month limit 1").head().getString(0)
    //左外连接 MAC有重复
    //   val customer_mac_join_df= customer_user.join(macrate, customer_user("mac") === macrate("mac"), "left")
    //    因此，网上有很多关于如何在JOIN之后删除列的，后来经过仔细查找，才发现通过修改JOIN的表达式，
    // 完全可以避免这个问题。而且非常简单。主要是通过Seq这个对象来实现。

    val customer_mac_join_df= customer_user.join(mac_count_month, Seq("mac"), "left")
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
    //201812酒店开机率低于0.3的数量
    val zero= hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate=0").head().getLong(0)
    val threeNum=    hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate<0.3 and hotelMonthRate>0").head().getLong(0)
    val sixNum=  hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate>=0.3 and hotelMonthRate<0.6").head().getLong(0)
    val highsixNum =   hotelMonthRate.sqlContext.sql("select count(*) from hotelMonthRate where hotelMonthRate>=0.6").head().getLong(0)

    val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream("src/main/resources/"+yearmonth+"月酒店平均开机率.txt" ), "utf-8"))
    out.println("总酒店数: "+totalHotelNum)
    out.println("酒店平均开机率为0的酒店数量: "+zero+" 百分比: "+zero.toFloat/totalHotelNum.toFloat*100+"%")
    out.println("酒店平均开机率为0<x<0.3的酒店数量: "+threeNum+" 百分比: "+threeNum.toFloat/totalHotelNum.toFloat*100+"%")
    out.println("酒店平均开机率为0.3<=x<0.6的酒店数量: "+sixNum+" 百分比: "+sixNum.toFloat/totalHotelNum.toFloat*100+"%")
    out.println("酒店平均开机率为>0.6的酒店数量: "+highsixNum+" 百分比: "+highsixNum.toFloat/totalHotelNum.toFloat*100+"%")
    out.close()

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
    /*
    |  customerId|macNum|totalDay|          hotelrate|
+------------+------+-----+-------------------+
|ofQQJxWsCGBx|  2290|19143|0.27864628820960696|
     */
    //    val customer_mac_join_df= sqlContext.sql("select v.*,nvl(m.count,0),m.rate,m.month from v_customer_user v left join mac_count_month m " +
//      "where v.mac=m.mac order by v.customerId")
//    customer_mac_join_df.show()
    //    customer_mac_join_df.write.format("parquet")
//      //  .mode(SaveMode.Append)
//      .save("src\\main\\resources\\customer_mac_join.parquet")
    sc.stop()
  }
}
