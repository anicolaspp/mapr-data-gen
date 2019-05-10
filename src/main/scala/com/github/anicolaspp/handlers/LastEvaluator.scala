package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.Functions._
import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.Format
import org.apache.spark.sql.{Dataset, SparkSession}

object LastEvaluator extends FormatEvaluator {
  override def next: FormatEvaluator = this

  override def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit = {
    exit(sparkSession)
  }

  private def exit(spark: SparkSession) = {
    log("Format not supported")

    spark.stop()
    sys.exit()
  }
}
