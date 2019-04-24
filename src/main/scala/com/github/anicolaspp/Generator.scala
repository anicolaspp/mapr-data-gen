package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Generator {

  import Logger._
  import com.mapr.db.spark.sql._

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

    if (options.getOutputFileFormat == "maprdb") {
      writeToMapRDB(options, outputDS)

    } else if (options.getOutputFileFormat == "mapres") {

      writeToStream(options, outputDS)

    } else if (options.getOutputFileFormat == "parquet") {

      writeToParquet(options, outputDS)
    } else {
      exit(spark)
    }

    log(s"data written out successfully to ${options.getOutput}")

    spark.stop()

  }

  private def exit(spark: SparkSession) = {
    log("Format not supported")

    spark.stop()
    sys.exit()
  }

  private def writeToParquet(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession) = {
    outputDS.write
      .options(options.getDataSinkOptions)
      .format(options.getOutputFileFormat)
      .mode(SaveMode.Overwrite)
      .save(options.getOutput)

    outputFromParquet(options.getOutput, options.getShowRows, options.getRowCount)
  }

  private def writeToMapRDB(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession) = {
    outputDS.write.option("Operation", "Overwrite").saveToMapRDB(options.getOutput)

    outputFromTable(options.getOutput, options.getShowRows)
  }

  private def writeToStream(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession) = {
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