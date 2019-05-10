package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.Functions._
import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.{CSV, Format}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object CsvEvaluator extends FormatEvaluator {
  override def next: FormatEvaluator = JsonEvaluator

  override def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit =
    if (format == CSV) {
      writeToCSV(options, outputDS)
    } else {
      next.eval(format, options, outputDS)
    }


  private def writeToCSV(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {
    outputDS
      .write
      .options(options.getDataSinkOptions)
      .mode(SaveMode.Overwrite)
      .csv(options.getOutput)

    spark.getStatsFrom(options.getOutput, "csv", options.getShowRows)
  }
}
