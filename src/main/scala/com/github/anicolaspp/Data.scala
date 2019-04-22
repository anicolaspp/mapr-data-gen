

package com.github.anicolaspp

import com.github.anicolaspp.configuration.ParseOptions


/**
  * Created by atr on 14.10.16.
  */
case class Data(_id: String, intKey: Int, randLong: Long, randDouble: Double, randFloat: Float, randString: String, payload: Array[Byte])


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