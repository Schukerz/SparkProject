package com.atguigu.spark.core.project.app

import java.text.DecimalFormat

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConversionApp {
  def pageConversion(sc:SparkContext,userVisitActionRDD: RDD[UserVisitAction],str:String): Unit ={

    val request: Array[String] = str.split(",")
    val req1: Array[String] = request.slice(0,request.length-1)
    val req2: Array[String] = request.slice(1,request.length)
    val reqArray: Array[String] = req1.zip(req2).map {
      case (str1, str2) => str1 + "->" + str2
    }
    //获取跳转流的页面分母
    val pageAndCount: collection.Map[Long, Long] = userVisitActionRDD.filter(action => req1.contains(action.page_id.toString))
      .map(action => (action.page_id, 1)).countByKey()
    //获取跳转流的分子
    val userVisitActionGrouped: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
    val pageAndFlow: collection.Map[String, Long] = userVisitActionGrouped.flatMap {
      case (sid, actionIt) => {
        val actionList: List[UserVisitAction] = actionIt.toList.sortBy(_.action_time)
        val list1: List[UserVisitAction] = actionList.slice(0, actionList.length - 1)
        val list2: List[UserVisitAction] = actionList.slice(1, actionList.length)
        list1.zip(list2).map {
          case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
        }.filter(s => reqArray.contains(s))
      }
    }.map((_, 1)).countByKey()
    //计算流转率

    val result: collection.Map[String, String] = pageAndFlow.map {
      case (page, count) =>
        val formatter: DecimalFormat = new DecimalFormat(".00%")
        val firstValue: Long = page.split("->")(0).toLong
        val total: Long = pageAndCount(firstValue)
        val rate = count.toDouble / total
        (page, formatter.format(rate))
    }
    result.foreach(println)
  }

}
