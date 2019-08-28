package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object redisPool {

    private val conf: JedisPoolConfig =new JedisPoolConfig()

    private def init():Unit={
      conf.setMaxTotal ( 30 )
      conf.setMaxIdle ( 10 )
    }

    private val pool = new JedisPool(conf,"10.211.55.101", 6379 )


    def getJedis(): Jedis = {
      pool.getResource
    }

  // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
}
