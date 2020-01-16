package com.atguigu.spark.sql.project.app


import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class Remark extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(StructField("cityName",StringType)::Nil)

  override def bufferSchema: StructType =
    StructType(StructField("map",MapType(StringType,LongType))::StructField("sum",LongType)::Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
      buffer(0) = map + (input.getString(0) ->(map.getOrElse(input.getString(0),0L)+1L))
      buffer(1)=buffer.getLong(1)+1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    buffer1(0)=map2.foldLeft(map1){
      case (map,(cityName,count))=>
        map + (cityName ->(map.getOrElse(cityName,0L)+count))
    }
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    val sum: Long = buffer.getLong(1)
    val cityAndCount: collection.Map[String, Long] = buffer.getMap[String,Long](0)

    //按照点击次数排名,取前2
    val cityAndCountTop2: List[(String, Long)] = cityAndCount.toList.sortBy(-_._2).take(2)
    val cityRemarkList: List[CityRemark] = cityAndCountTop2.map {
      case (cityname, count) =>
        CityRemark(cityname, count.toDouble / sum)
    }
    val result: List[CityRemark] = cityRemarkList :+ CityRemark("其他",cityRemarkList.foldLeft(1D)(_-_.cityRatio))

    result.mkString(",")


  }
}
case class CityRemark(cityName:String,cityRatio:Double){
  private val formatter: DecimalFormat = new DecimalFormat("0.0%")

  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}
