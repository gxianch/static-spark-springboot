package cn.itcast.spark.day5
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object DirectKafkaPlay {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
  //  val Array(brokers, topics, groupId) = args
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val topics = "play"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "10.10.1.110:9092,10.10.1.110:9093,10.10.1.110:9094",
      "group.id" -> "testplay",
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.foreachRDD(rdd => {
      rdd.foreach( println)

    })
    //    messages.foreachRDD(rdd => {
    //      if (!rdd.isEmpty()) {
    //        // 先处理消息
    //        processRdd(rdd)
    //        // 再更新offsets
    //        km.updateZKOffsets(rdd)
    //      }
    //    })

    ssc.start()
    ssc.awaitTermination()
  }
}
