package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.Functions._
import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.{Format, JSON}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object JsonEvaluator extends FormatEvaluator {
  override def next: FormatEvaluator = LastEvaluator

  override def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit =
    if (format == JSON) {
      writeToJSON(options, outputDS)
    } else {
      next.eval(format, options, outputDS)
    }
  
  private def writeToJSON(options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit = {
    outputDS
      .write
      .options(options.getDataSinkOptions)
      .mode(SaveMode.Overwrite)
      .json(options.getOutput)

    sparkSession.getStatsFrom(options.getOutput, "json", options.getShowRows)
  }
}
