package com.peng.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.peng.gmall.common.constant.GmallConstants
import com.peng.gmall.realtime.bean.StartupLog
import com.peng.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis


object DauApp2 {


  def main(args: Array[String]): Unit = {

    //    1，通过工具类创建ssc 连接kafka
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //    2，将流中的json数据 组装成对象，并取出日期和小时
    val startupLogDstream: DStream[StartupLog] = inputDstream.map { record =>
      val startupJsonString: String = record.value
      //      val startupJsonString: String = record.value()
      val startupLog: StartupLog = JSON.parseObject(startupJsonString, classOf[StartupLog])
      val DTString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      val DTarr: Array[String] = DTString.split(" ")
      startupLog.logDate = DTarr(0)
      startupLog.logHour = DTarr(1)
      startupLog
    }

    //    //这里的代码是在driver端运行,执行一次,除非多线程调用
    //    startupLogDstream.foreachRDD(RDD =>
    //      //这里的代码是在driver端运行,每批次执行一次
    //      RDD.foreachPartition(Partition =>
    //        //这里的代码是在executor端运行
    //        //每个Partition是个迭代器
    //        Partition.foreach(println)
    //      )
    //    )

    //    3，取出redis中的数据 利用broadcast进行批次间去重
    val filteredDStream: DStream[StartupLog] = startupLogDstream.transform { RDD =>
      println("过滤前：" + RDD.count())
      val jedis = new Jedis("node1", 6379)
      val dauKey = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      val BCdauSet: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      jedis.close()
      val filteredRDD: RDD[StartupLog] = RDD.filter(startupLog => !BCdauSet.value.contains(startupLog.mid))
      println("redis过滤后： " + filteredRDD.count())
      filteredRDD
    }

    //    4，批次内去重
    val startupLogMidGroup: DStream[(String, Iterable[StartupLog])] = filteredDStream.map(startupLog => (startupLog.mid, startupLog)).groupByKey()
    //    startupLogMidGroup.flatMap(logStartupLog=>logStartupLog._2.take(1))
    val realFilteredDStream: DStream[StartupLog] = startupLogMidGroup.flatMap { case (mid, logStartupLog) => logStartupLog.take(1) }

    realFilteredDStream.foreachRDD { RDD =>
      println("redis和同组去重后：" + RDD.count())
    }
    //    5，为了防止后边批次计算速度追上前边批次，需要cache一下
    /////

    //    6，更新redis中的set
    realFilteredDStream.foreachRDD(RDD =>
      RDD.foreachPartition { partition =>
        val jedis: Jedis = new Jedis("node1", 6379)
        for (elem <- partition) {
          val dauKey = "dau:" + elem.logDate
          println(dauKey + "____" + elem.mid)
          jedis.sadd(dauKey, elem.mid)
        }
        jedis.close()
      }
    )


    //    7，开始执行
    ssc.start()
    ssc.awaitTermination()
  }
}
