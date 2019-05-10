package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.Functions._
import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.{Format, Parquet}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


object ParquetEvaluator extends FormatEvaluator {
  override def next: FormatEvaluator = CsvEvaluator

  override def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit =
    if (format == Parquet) {
      writeToParquet(options, outputDS)
    } else {
      next.eval(format, options, outputDS)
    }

  private def writeToParquet(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {
    outputDS
      .write
      .options(options.getDataSinkOptions)
      .mode(SaveMode.Overwrite)
      .parquet(options.getOutput)

    spark.getStatsFrom(options.getOutput, "parquet", options.getShowRows)
  }
}
