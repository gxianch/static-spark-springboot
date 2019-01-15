package cn.itcast.spark.day4

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by root on 2016/5/19.
  * 盒子一个月内的的开机率  盒子开机天数/天数
  * 201812总盒子数:267857
  * 盒子开机率为0的盒子数量:80129  30%  总数-其它  267857-187728
  * 为0<x<0.3的盒子数量:76346 28.5%
  * 为0.3<=x<0.6的盒子数量:44614  16.7%
  * >0.6的盒子数量:66768   25%
  * 
  */
object MacPowerOnMutilMonthSave {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("statis").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")
    sc.setLogLevel("warn")
    val customer_user = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/customer_user.parquet`")
    customer_user.createOrReplaceTempView("v_customer_user")
    val macTotalNum =  customer_user.sqlContext.sql("select count(*) from (select distinct mac from v_customer_user)").head().getLong(0)

    val mac_count = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/macPowerOnMutilSave.parquet`")
    mac_count.createOrReplaceTempView("mac_count")
   // val month = mac_count.sqlContext.sql("select month from mac_count limit 1").head().getString(0)
    //没有开机的就没有发送数据，由总数减去不为0
    val openNum = mac_count.sqlContext.sql("select count(*) from mac_count where powerOnRate != 0").head().getLong(0)
    val threeNum = mac_count.sqlContext.sql("select count(*) from mac_count where powerOnRate<0.3 and powerOnRate>0").head().getLong(0)
    val sixNum=  mac_count.sqlContext.sql("select count(*) from mac_count where powerOnRate>=0.3 and powerOnRate<0.6").head().getLong(0)
    val highsixNum=  mac_count.sqlContext.sql("select count(*) from mac_count where powerOnRate>=0.6").head().getLong(0)

    val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream("src/main/resources/201811-12月盒子开机率.txt" ), "utf-8"))
    out.println("总盒子数: "+macTotalNum)
    out.println("盒子开机率为0的盒子数量: "+(macTotalNum-openNum)+" 百分比: "+(macTotalNum*2-openNum).toFloat/macTotalNum.toFloat*100*2+"%")
    out.println("盒子开机率为0<x<0.3的盒子数量: "+threeNum+" 百分比: "+threeNum.toFloat/macTotalNum.toFloat*100*2+"%")
    out.println("盒子开机率为0.3<=x<0.6的盒子数量: "+sixNum+" 百分比: "+sixNum.toFloat/macTotalNum.toFloat*100*2+"%")
    out.println("盒子开机率为>0.6的盒子数量: "+highsixNum+" 百分比: "+highsixNum.toFloat/macTotalNum.toFloat*100*2+"%")
    out.close()

    val jedis = new Jedis("111.23.6.233",23308)
    val Value = "{\"total\":"+macTotalNum+
      ",\"monthnum\":"+2 +
      ",\"0\":" +(macTotalNum-openNum)+
      ",\"0-3\":"+threeNum +
      ",\"3-6\":"+sixNum +
      ",\"6-1\":"+highsixNum+
      "}"
    val key = "powerOnRate:mac:201811-12"
    jedis.set(key,Value)
    jedis.close()

    sc.stop()
  }
}
