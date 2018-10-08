package com.anallslsoftax.service

import com.anallslsoftax.dao.StudentDao
import com.anallslsoftax.model.Student
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SlowLog {


  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("mysql-slow").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))

    //获取kafka的配置
    val topic = "mysql-slow"
    //kafka 消费者配置

    val topicSet = topic.split(",").toSet
    val kafaParams = Map[String,String]("bootstrap.servers" -> "10.2.4.55:9092")
    val messages = KafkaUtils.createDirectStream[String, String,StringDecoder,StringDecoder](ssc,kafaParams,topicSet)
    val mv = messages.map(a =>{
      (a._2.split(" ")(0), a._2.split(" ")(1), a._2.split(" ")(2),(a._2.split(" ")(3)))
    })

    val msg = mv.foreachRDD(rdd =>{

      rdd.foreachPartition{items =>

        val students = ArrayBuffer[Student]()

        for ( item <- items) {
          println(item.toString)

          val id = item._1
          val name = item._2
          val gender = item._3
          val age = item._4

          students += Student(id.toLong, name, gender, age.toLong)

        }
        StudentDao.insertBach(students.toArray)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}