package com.peng.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.peng.gmall.common.constant.GmallConstants
import com.peng.gmall.realtime.bean.StartupLog
import com.peng.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


object DauApp3 {
  def main(args: Array[String]): Unit = {
    //1.1 构建ssc
    val conf: SparkConf = new SparkConf().setAppName("sstest").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //1.2 组织kafka参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "node1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean) /////一定是lang.Boolean的 true
    )
    val topic = GmallConstants.KAFKA_TOPIC_STARTUP

    //1.3 构建流
    val DStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )


    //2,解析json 转成bean,补全属性
    val beanDStream: DStream[StartupLog] = DStream.map { record =>
      val jsonObj: String = record.value()
      val startupObj: StartupLog = JSON.parseObject(jsonObj, classOf[StartupLog])
      //补全时间
      val formatTime: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupObj.ts))
      val timeArr: Array[String] = formatTime.split(" ")
      startupObj.logDate = timeArr(0)
      startupObj.logHour = timeArr(1)
      startupObj
    }


    //3,两种去重
    //3.1 redis去重
    //连一下redis,取一下存在redis里的mid,broadcast出去
    //filter一下,不包含的留下
    //最外边是在driver里运行,不多线程只运行一次
    //这里是在driver里运行,每个批次运行一次
    val fliterDStream: DStream[StartupLog] = beanDStream.transform { rdd =>
      println("过滤前：" + rdd.count())
      val jedis: Jedis = new Jedis("node1", 6379)
      val dauKey: String = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      jedis.close()

      val dauBcSet: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)

      val fliterRDD: RDD[StartupLog] = rdd.filter { startupObj => //filter就是在executor里执行的了
        !dauBcSet.value.contains(startupObj.mid)
      }
      println("redis过滤后：" + fliterRDD.count())
      fliterRDD
    }
    //3.2 同批次去重
    val groupByMidDStream: DStream[(String, Iterable[StartupLog])] = fliterDStream.map(startupLog => (startupLog.mid, startupLog)).groupByKey()
    //    val realFliterDStream: DStream[StartupLog] = groupByMidDStream.flatMap { case (mid, startupLog) => startupLog.take(1) }
    val realFliterDStream: DStream[StartupLog] = groupByMidDStream.flatMap(_._2.take(1))
    realFliterDStream.foreachRDD(rdd =>
      println("redis和同批次过滤后：" + rdd.count())
    )


    //4,cache一下 防追尾


    //5,本批次不重复的存一下redis
    realFliterDStream.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val jedis: Jedis = new Jedis("node1", 6379)
        for (elem <- partition) {
          val dauKey = "dau:" + elem.logDate
          println(dauKey + "____" + elem.mid)
          jedis.sadd(dauKey, elem.mid)
        }
        jedis.close()
      }
    }

    //6,存到phoenix里
    import org.apache.phoenix.spark._
    realFliterDStream.foreachRDD{ rdd =>
      rdd.saveToPhoenix(
        "gmall_dau",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration(),
        Some("node1:2181")
      )
    }


    //7,开始执行
    ssc.start()
    ssc.awaitTermination()
  }
}
