import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AreaClickUDAF extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(StructField("cityName",StringType)::Nil)

  override def bufferSchema: StructType =
    StructType(StructField("map",MapType(StringType,LongType))::StructField("total",LongType)::Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=Map[String,Long]()
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
      val cityName: String = input.getString(0)
      buffer(0)=   map + (cityName -> (map.getOrElse(cityName,0L)+1L))
      buffer(1) = buffer.getLong(1)+1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(1)= buffer1.getLong(1)+buffer2.getLong(1)
    val map1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    buffer1(0) = map2.foldLeft(map1){
      case (map,(cityName,count)) =>
        map + (cityName -> (map.getOrElse(cityName,0L)+count))
    }
  }

  override def evaluate(buffer: Row): Any = {
    val cityAndCount: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val total: Long = buffer.getLong(1)
//    val cityInfoTop2: List[CityInfo] = cityAndCount.map {
    ////      case (cityName, count) => CityInfo(cityName, count.toDouble / total)
    ////    }.toList.sortBy(-_.cityRatio).take(2)
    ////    val infoes: List[CityInfo] = cityInfoTop2 :+ (CityInfo("其他",cityInfoTop2.foldLeft(1D)(_-_.cityRatio)))
    ////    infoes.mkString(",")
    val cityInfoTop2: List[(String, Double)] = cityAndCount.map {
      case (cityName, count) => (cityName, count.toDouble / total)
    }.toList.sortBy(-_._2).take(2)
    val infoes: List[(String, Double)] = cityInfoTop2 :+ ("其他" -> cityInfoTop2.foldLeft(1D)(_-_._2))
    val formatter = new DecimalFormat(".0%")

    infoes.map{
      case (cityName,ratio) => s"$cityName${formatter.format(ratio)}"
    }.mkString(",")


  }
}

/*
样例类的好处:
  增加代码可读性
  利用样例类实现特殊功能(比如个性化排序)
 */
case class CityInfo(cityName:String,cityRatio:Double){
  private val formatter: DecimalFormat = new DecimalFormat(".0%")

  override def toString: String = cityName + ":" + formatter.format(cityRatio)
}