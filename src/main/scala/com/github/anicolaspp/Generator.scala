package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.producer.ProducerConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import Logger._

object Generator {

  import com.mapr.db.spark.sql._

  def main(args: Array[String]) {
    val options = new ParseOptions()

    println(options.getBanner)
    log("concat arguments = " + args.foldLeft("")(_ + _))

    options.parse(args)

    val spark = SparkSession
      .builder()
      .appName("MapR Data Gen")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", options.getCompressionType)
    spark.sqlContext.setConf("orc.compress", "none")

    import spark.implicits._

    log("Generating Data...")

    val inputRDD = genData(options, spark).cache()

    val outputDS = inputRDD.toDS().repartition(options.getPartitions)

    if (options.getOutputFileFormat == "maprdb") {
      outputDS.write.option("Operation", "Overwrite").saveToMapRDB(options.getOutput)

      outputFromTable(options.getOutput, options.getShowRows)(spark)

    } else if (options.getOutputFileFormat == "mapres") {

      val ssc = new StreamingContext(spark.sparkContext, Milliseconds(100))

      val producerConf = new ProducerConf(bootstrapServers = "".split(",").toList)
        .withKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
        .withValueSerializer("org.apache.kafka.common.serialization.StringSerializer")

      import com.github.anicolaspp.RDDFunctions._

      outputDS.rdd.map(_.toString).sendToKafka(options.getOutput, producerConf, options.getConcurrency)

    } else {

      outputDS.write
        .options(options.getDataSinkOptions)
        .format(options.getOutputFileFormat)
        .mode(SaveMode.Overwrite)
        .save(options.getOutput)

      outputFromParquet(options.getOutput, options.getShowRows, options.getRowCount)(spark)
    }

    log(s"data written out successfully to ${options.getOutput}")

    spark.stop()

  }

  def outputFromTable(fileName: String, showRows: Int)(implicit spark: SparkSession): Unit = {
    if (showRows > 0) {
      val inputDF = spark.loadFromMapRDB(fileName)

      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(spark, inputDF)
      inputDF.show(showRows)

      log(s"RESULTS: table " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")

    }
  }

  def outputFromParquet(fileName: String, showRows: Int, expectedRows: Long)(implicit spark: SparkSession): Unit = {
    if (showRows > 0) {
      val inputDF = spark.read.parquet(fileName)

      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(spark, inputDF)
      inputDF.show(showRows)

      log(s"RESULTS: file " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")

      if (expectedRows > 0) {
        require(items == expectedRows,
          "Number of rows do not match, counted: " + items + " expected: " + expectedRows)
      }
    }
  }

  private def genData(options: ParseOptions, spark: SparkSession) = {
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

object Logger {
  def log(msg: String) = {
    println("----------------------------------------------------------------------------------------------")
    println(s"MapR Data Gen : $msg")
    println("----------------------------------------------------------------------------------------------")
  }
}
