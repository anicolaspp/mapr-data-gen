package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions
import com.github.anicolaspp.Functions._

case class Data(_id: String, intKey: Int, randLong: Long, randDouble: Double, randFloat: Float, randString: String, payload: Array[Byte]) {
  override def toString: String =
    s"""{"_id":"${_id}", "intKey": "$intKey", "randLong": "$randLong", "randDouble": "$randDouble", "randFloat": "$randFloat",
       |"randString": $randString, "payload": "${payload.mkString("[", ",", "]")}"}""".stripMargin.trim
}

object Data {
  def apply(options: ParseOptions): Data =
    Data(
      DataGenerator.getNextString(10, affix = false),
      DataGenerator.getNextInt(options.getRangeInt),
      DataGenerator.getNextLong,
      DataGenerator.getNextDouble,
      DataGenerator.getNextFloat,
      DataGenerator.getNextString(options.getVariableSize, false),
      DataGenerator.getNextByteArray(options.getVariableSize, options.getAffixRandom)
    )
}