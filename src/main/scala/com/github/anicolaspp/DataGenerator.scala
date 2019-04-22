package com.github.anicolaspp

import scala.util.Random

object DataGenerator extends Serializable {
  val random = new Random(System.nanoTime())

  /* affix payload variables */
  var affixStringBuilder:StringBuilder = _
  var affixByteArray:Array[Byte] = _

  def getNextString(size: Int, affix: Boolean):String = {
    if(affix){
      this.synchronized {
        if (affixStringBuilder == null) {
          affixStringBuilder = new StringBuilder(
            random.alphanumeric.take(size).mkString)
        }
      }
      /* just randomly change 1 byte - this is to make sure parquet
      * does not ignore the data */
      affixStringBuilder.setCharAt(random.nextInt(size),
        random.nextPrintableChar())
      affixStringBuilder.mkString
    } else {
      random.alphanumeric.take(size).mkString
    }
  }

  def getNextByteArray(size: Int, affix: Boolean):Array[Byte] = {
    val toReturn = new Array[Byte](size)
    if(!affix){
      /* if not affix, then return completely new values in a new array */
      random.nextBytes(toReturn)
    } else {
      this.synchronized{
        if(affixByteArray == null){
          affixByteArray = new Array[Byte](size)
          /* initialize */
          random.nextBytes(affixByteArray)
        }
      }
      /* just randomly change 1 byte - this is to make sure parquet
      * does not ignore the data - char will be casted to byte */
      affixByteArray(random.nextInt(size)) = random.nextPrintableChar().toByte
      /* now we copy affix array */
      Array.copy(affixByteArray, 0, toReturn, 0, size)
    }
    toReturn
  }

  def getNextInt:Int = {
    random.nextInt()
  }

  def getNextInt(max:Int):Int = {
    random.nextInt(max)
  }

  def getNextLong:Long= {
    random.nextLong()
  }

  def getNextDouble:Double= {
    random.nextDouble()
  }

  def getNextFloat: Float = {
    random.nextFloat()
  }

  def getNextValue(s:String, size: Int, affix:Boolean): String ={
    getNextString(size, affix)
  }

  def getNextValue(i:Int): Int = {
    getNextInt
  }

  def getNextValue(d:Double): Double = {
    getNextDouble
  }

  def getNextValue(l:Long): Long = {
    getNextLong
  }

  def getNextValueClass(cls: Any): Any = {
    val data = cls match {
      case _:String => DataGenerator.getNextString(10, false)
      case _ => throw new Exception("Data type not supported: ")
    }
    data
  }

  def getNextValuePrimitive(typeX: Class[_]): Any = {
    val data = typeX match {
      case java.lang.Integer.TYPE => DataGenerator.getNextInt
      case java.lang.Long.TYPE => DataGenerator.getNextLong
      case java.lang.Float.TYPE => DataGenerator.getNextFloat
      case java.lang.Double.TYPE => DataGenerator.getNextDouble
      case _ => println("others")
    }
    data
  }

  def getNextValue(typeX: Class[_]) : Any = {
    if(typeX.isPrimitive)
      getNextValuePrimitive(typeX)
    else
      getNextValueClass(typeX.newInstance())
  }

  /* another example - this is what I wanted in the first place */
  def f[T](v: T) = v match {
    case _: Int    => "Int"
    case _: String => "String"
    case _         => "Unknown"
  }

}
