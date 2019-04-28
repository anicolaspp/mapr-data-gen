package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.handlers.{CsvHandler, JsonHandler, ParquetHandler, StreamHandler}
import org.apache.spark.sql.{Dataset, SparkSession}

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

      StreamHandler.writeToStream(options, outputDS)

    } else if (options.getOutputFileFormat == "parquet") {

      ParquetHandler.writeToParquet(options, outputDS)
    } else if (options.getOutputFileFormat == "csv") {

      CsvHandler.writeToCSV(options, outputDS)

    } else if (options.getOutputFileFormat == "json") {

      JsonHandler.writeToJSON(options, outputDS)

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


  private def writeToMapRDB(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {
    outputDS.write.option("Operation", "Overwrite").saveToMapRDB(options.getOutput)

    outputFromTable(options.getOutput, options.getShowRows)
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