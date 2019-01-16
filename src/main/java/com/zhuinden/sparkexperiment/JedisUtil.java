package com.zhuinden.sparkexperiment;

import org.springframework.beans.factory.annotation.Configurable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Properties;

public class JedisUtil {
    private static JedisPool pool;
    static {
        try{
            Properties props = new Properties();
            props.load(JedisUtil.class.getClassLoader().getResourceAsStream("application.properties"));
            //创建jedis池配置实例
            JedisPoolConfig config = new JedisPoolConfig();
            //设置池配置项值
            config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxActive")));
            config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
            config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
            config.setTestOnBorrow(Boolean.valueOf(props.getProperty("jedis.pool.testOnBorrow")));
            config.setTestOnReturn(Boolean.valueOf(props.getProperty("jedis.pool.testOnReturn")));
            //根据配置实例化jedis池
            pool = new JedisPool(config, props.getProperty("redis.ip"), Integer.valueOf(props.getProperty("redis.port")));
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Jedis getJedis(){
        return pool.getResource();
    }
}
