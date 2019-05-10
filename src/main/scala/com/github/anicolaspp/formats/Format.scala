package com.github.anicolaspp.formats

sealed trait Format

object Format {
  def fromString(format: String): Format =

    if (format == "maprdb") {
      MapRDB
    } else if (format == "mapres") {
      Stream
    } else if (format == "parquet") {
      Parquet
    } else if (format == "csv") {
      CSV
    } else if (format == "json") {
      JSON
    } else {
      Unsupported
    }
}

case object JSON extends Format

case object CSV extends Format

case object Parquet extends Format

case object Stream extends Format

case object MapRDB extends Format

case object Unsupported extends Format