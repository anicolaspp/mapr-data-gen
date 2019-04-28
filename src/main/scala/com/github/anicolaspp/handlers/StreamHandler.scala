package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.Logger.log
import com.github.anicolaspp.configuration.ParseOptions
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object StreamHandler {
  def writeToStream(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {
    import com.github.anicolaspp.RDDFunctions._

    if (options.getTimeout != null) {

      val ssc = new StreamingContext(spark.sparkContext, Milliseconds(10))

      val stream = new ConstantInputDStream[String](ssc, outputDS.rdd.map(_.toString))

      stream.foreachRDD(_.sendToKafka(options.getOutput, options.getConcurrency))

      log(s"Running for the next ${options.getTimeout}...")

      ssc.start()

      Thread.sleep(options.getTimeout.toMillis)

      ssc.stop()
    } else {
      outputDS.rdd.map(_.toString).sendToKafka(options.getOutput, options.getConcurrency)
    }
  }
}
