package cn.itcast.spark.day4

import redis.clients.jedis.Jedis

object JedisDemo {
  def main(args: Array[String]): Unit = {
       val jedis = new Jedis("111.23.6.233",23308)
//    val jedis = new Jedis("10.10.1.63",23308)
    val mac201811 = "{\"total\":267857,\"zero\":84566,\"three\":70471,\"six\":45811,\"one\":67009}"
    val mac201812 = "{\"total\":267857,\"zero\":80129,\"three\":76346,\"six\":44614,\"one\":66768}"
    val hotel201811 = "{\"total\":8756,\"zero\":1954,\"three\":3719,\"six\":1984,\"one\":1099}"
    val hotel201812 = "{\"total\":8756,\"zero\":1669,\"three\":1669,\"six\":1940,\"one\":1173}"
    jedis.hset("powerOnRate:mac","201811",mac201811);
    jedis.hset("powerOnRate:mac","201812",mac201812);
    jedis.hset("powerOnRate:hotel","201811",hotel201811);
    jedis.hset("powerOnRate:hotel","201812",hotel201812);
//    jedis.set("powerOnRate:mac:201811",mac201811)
//    jedis.set("powerOnRate:mac:201812",mac201812)
//    jedis.set("powerOnRate:hotel:201811",hotel201811)
//    jedis.set("powerOnRate:hotel:201812",hotel201812)
//   val map = jedis.get("powerOnRate:hotel*")
   val map= jedis.hgetAll("powerOnRate:hotel")
    println(1)
    jedis.close()
  }

}
