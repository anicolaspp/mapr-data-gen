package com.github.anicolaspp.handlers

import com.github.anicolaspp.Functions._
import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.{Format, MapRDB}
import com.github.anicolaspp.Data
import com.github.anicolaspp.functions.SparkTools
import org.apache.spark.sql.{Dataset, SparkSession}

object MapRDBEvaluator extends FormatEvaluator {
  override def next: FormatEvaluator = StreamEvaluator

  override def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit =
    if (format == MapRDB) {
      writeToMapRDB(options, outputDS)
    } else {
      next.eval(format, options, outputDS)
    }

  private def writeToMapRDB(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {

    import com.mapr.db.spark.sql._

    outputDS.write.option("Operation", "Overwrite").saveToMapRDB(options.getOutput)

    outputFromTable(options.getOutput, options.getShowRows)
  }

  private def outputFromTable(fileName: String, showRows: Int)(implicit spark: SparkSession): Unit = {
    import com.mapr.db.spark.sql._

    if (showRows > 0) {
      val inputDF = spark.loadFromMapRDB(fileName)

      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(spark, inputDF)
      inputDF.show(showRows)

      log(s"RESULTS: table " + fileName + " contains " + items + " rows and makes " + partitions + " partitions when read")
    }
  }
}
