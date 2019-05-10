package com.github.anicolaspp.handlers

import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.Format
import com.github.anicolaspp.{Data, formats}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object StreamEvaluator extends FormatEvaluator {
  override def next: FormatEvaluator = ParquetEvaluator

  override def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit =
    if (format == formats.Stream) {
      writeToStream(options, outputDS)
    } else {
      next.eval(format, options, outputDS)
    }

  private def writeToStream(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {
    import com.github.anicolaspp.Functions._

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
