package cn.itcast.spark.day4

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/19.
  * 保存多个月的登陆记录 launch到parquet，序列化减少存储,由700M-150M
  */
object LaunchMutilMonthSave {
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
    println(launchDf.count())
    launchDf.write.mode(SaveMode.Overwrite).format("parquet").save("src\\main\\resources\\launch.parquet")
    sc.stop()
  }
}
