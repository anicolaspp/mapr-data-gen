package com.github.anicolaspp

import com.github.anicolaspp.Logger.log
import org.apache.spark.sql.SparkSession

object DataFrameFunctions {

  implicit class SparkSessionFunctions(sparkSession: SparkSession) {

    def getStatsFrom(source: String, format: String, showRows: Int = 0): Unit = {
      val inputDF = sparkSession.read.format(format).load(source)

      val items = inputDF.count()
      val partitions = SparkTools.countNumPartitions(sparkSession, inputDF)
      inputDF.show(showRows)

      log(s"RESULTS: file " + source + " contains " + items + " rows and makes " + partitions + " partitions when read")
    }
  }

}
