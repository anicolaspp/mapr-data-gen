

package com.github.anicolaspp

import java.util

import com.github.anicolaspp.configuration.ParseOptions


/**
  * Created by atr on 14.10.16.
  */
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

object DataSerializer extends org.apache.kafka.common.serialization.Serializer[Data] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ???

  override def serialize(s: String, t: Data): Array[Byte] = ???

  override def close(): Unit = ???
}