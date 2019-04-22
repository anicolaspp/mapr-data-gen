package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object Generator {

  import com.mapr.db.spark.sql._

  def main(args: Array[String]) {
    val options = new ParseOptions()

    println(options.getBanner)
    println("concat arguments = " + args.foldLeft("")(_ + _))

    options.parse(args)

    val spark = SparkSession
      .builder()
      .appName("Data Generator")
      .enableHiveSupport()
      .getOrCreate()
    
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", options.getCompressionType)
    spark.sqlContext.setConf("orc.compress", "none")

    import spark.implicits._

    val inputRDD = genData(options, spark)

    val outputDS = inputRDD.toDS().repartition(options.getPartitions)

    if (options.getOutputFileFormat == "maprdb") {
      outputDS.write.option("Operation", "Overwrite").saveToMapRDB(options.getOutput)

      outputFromTable(options.getOutput, options.getShowRows)(spark)

    } else {

      outputDS.write
        .options(options.getDataSinkOptions)
        .format(options.getOutputFileFormat)
        .mode(SaveMode.Overwrite)
        .save(options.getOutput)

      outputFromParquet(options.getOutput, options.getShowRows, options.getRowCount)(spark)
    }

    println("----------------------------------------------------------------------------------------------")
    println("ParquetGenerator : " + options.getClassName + " data written out successfully to " + options.getOutput)
    println("----------------------------------------------------------------------------------------------")

    spark.stop()

  }

  def outputFromTable(fileName: String, showRows: Int)(implicit spark: SparkSession): Unit = {
    if (showRows > 0) {
      val inputDF = spark.loadFromMapRDB(fileName)

      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(spark, inputDF)
      inputDF.show(showRows)

      println("----------------------------------------------------------------")
      println(s"RESULTS: table " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")
      println("----------------------------------------------------------------")

    }
  }

  def outputFromParquet(fileName: String, showRows: Int, expectedRows: Long)(implicit spark: SparkSession): Unit = {
    if (showRows > 0) {
      val inputDF = spark.read.parquet(fileName)

      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(spark, inputDF)
      inputDF.show(showRows)

      println("----------------------------------------------------------------")
      println(s"RESULTS: file " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")
      println("----------------------------------------------------------------")

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

    spark
      .sparkContext
      .parallelize(0 until options.getTasks, options.getTasks)
      .flatMap { _ =>
        val data = ListBuffer.empty[Data]

        (0L to rowsPerTask).foreach(_ => data += Data(options))

        data
      }
  }
}
