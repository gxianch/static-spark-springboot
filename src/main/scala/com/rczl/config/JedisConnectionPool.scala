package com.rczl.config

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by lq on 2017/8/29.
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(10)
  //最大空闲连接数
  config.setMaxIdle(5)
  //当调用borrow object 方法时,是否进行有效性验证
  config.setTestOnBorrow(true)
  val pool = new JedisPool(config, "10.10.1.63", 23308)
//  val pool = new JedisPool(config, "111.23.6.233", 23308)

  def getContion(): Jedis = {
    pool.getResource
  }
}
