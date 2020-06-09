package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.Format
import com.github.anicolaspp.handlers.FormatEvaluator
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import java.time.LocalDateTime
import java.{util => ju}

object Generator {

  import com.github.anicolaspp.Functions._

  def main(args: Array[String]) {
    val options = new ParseOptions()

    println(options.getBanner)
    log("concat arguments = " + args.foldLeft("")(_ + _))

    options.parse(args)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("MapR Data Gen")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", options.getCompressionType)
    spark.sqlContext.setConf("orc.compress", "none")

    import spark.implicits._

    val inputRDD = genData(options, spark).cache()

    val outputDS = inputRDD.toDS().repartition(options.getPartitions)

    val format = Format.fromString(options.getOutputFileFormat)

    val now = org.joda.time.DateTime.now().getMillis

    FormatEvaluator().eval(format, options, outputDS)

    log(s"data written out successfully to ${options.getOutput} in ${(org.joda.time.DateTime.now().getMillis - now) / 1000L} seconds")

    spark.stop()

  }

  private def genData(options: ParseOptions, spark: SparkSession) = {
    log("Generating Data...")

    /* some calculations */
    require(options.getRowCount % options.getTasks == 0, " Please set rowCount (-r) and tasks (-t) such that " +
      "rowCount%tasks == 0, currently, rows: " + options.getRowCount + " tasks " + options.getTasks)

    val rowsPerTask = options.getRowCount / options.getTasks

    val gen = spark
      .sparkContext
      .parallelize(0 until options.getTasks, options.getTasks)
      .flatMap { _ =>
        val data = ListBuffer.empty[Data]

        (1L to rowsPerTask).foreach(_ => data += Data(options))

        data
      }

    log(s"Generated: ${gen.count()} rows")

    gen
  }

}

















