package com.github.anicolaspp.handlers

import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.{Data, DataFrameFunctions}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object JsonHandler {

  import DataFrameFunctions._

  def writeToJSON(options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession) = {
    outputDS
      .write
      .options(options.getDataSinkOptions)
      .mode(SaveMode.Overwrite)
      .json(options.getOutput)

    sparkSession.getStatsFrom(options.getOutput, "json", options.getShowRows)
  }
}
