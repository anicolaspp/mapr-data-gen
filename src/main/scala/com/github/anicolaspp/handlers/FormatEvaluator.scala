package com.github.anicolaspp.handlers

import com.github.anicolaspp.Data
import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.formats.Format
import org.apache.spark.sql.{Dataset, SparkSession}

trait FormatEvaluator {

  def next: FormatEvaluator

  def eval(format: Format, options: ParseOptions, outputDS: Dataset[Data])(implicit sparkSession: SparkSession): Unit

}

object FormatEvaluator {
  def apply(): FormatEvaluator = MapRDBEvaluator
}