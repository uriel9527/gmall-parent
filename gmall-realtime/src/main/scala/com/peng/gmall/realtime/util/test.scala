package com.peng.gmall.realtime.util

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object test {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = new Jedis("node1", 6379)
    println(jedis.keys("*"))
    println(jedis.get("name"))
    jedis.close()
  }
}
