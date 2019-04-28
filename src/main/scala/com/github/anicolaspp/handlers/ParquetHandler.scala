package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.configuration.ParseOptions
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.github.anicolaspp.DataFrameFunctions._

object ParquetHandler {
  def writeToParquet(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession): Unit = {
    outputDS
      .write
      .options(options.getDataSinkOptions)
      .mode(SaveMode.Overwrite)
      .parquet(options.getOutput)

    spark.getStatsFrom(options.getOutput, "parquet", options.getShowRows)
  }
}
