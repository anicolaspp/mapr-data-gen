package com.github.anicolaspp

import java.util.Properties
import java.util.concurrent.Executors

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.producer.ProducerConf

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object RDDFunctions {

  implicit class Funcitons(rdd: RDD[String]) {

    def sendToKafka(streamName: String, conf: ProducerConf, numberOfThreads: Int = 100): Unit = {

      rdd.foreachPartition { partition =>
        implicit val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(numberOfThreads))

        val props = new Properties()
        props.setProperty("batch.size", "16384")
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.setProperty("block.on.buffer.full", "true")

        val producer = new KafkaProducer[String, String](props)

        val tasks = partition
          .map(v => new ProducerRecord[String, String](streamName, v))
          .map { record =>
            Future {
              val data = producer.send(record).get()

              println(data.offset())
            }
          }

        val partitionTask = Future.sequence(tasks)

        Await.ready(partitionTask, Duration.Inf)
      }
    }
  }

}
