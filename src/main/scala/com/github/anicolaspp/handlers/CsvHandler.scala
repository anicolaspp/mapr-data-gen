package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.configuration.ParseOptions
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import com.github.anicolaspp.DataFrameFunctions._

object CsvHandler {
  def writeToCSV(options: ParseOptions, outputDS: Dataset[Data])(implicit spark: SparkSession) = {
    outputDS
      .write
      .options(options.getDataSinkOptions)
      .mode(SaveMode.Overwrite)
      .csv(options.getOutput)

    spark.getStatsFrom(options.getOutput, "csv", options.getShowRows)
  }
}
