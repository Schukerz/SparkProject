package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.ActionTop10Acc
import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable

object UserVisitActionTop10App {
  def userVisitActionTop10(sc: SparkContext,userVisitActionRDD: RDD[UserVisitAction])={

    //注册累加器
    val acc: ActionTop10Acc = new ActionTop10Acc
    sc.register(acc)
    userVisitActionRDD.foreach(action=>{
      acc.add(action)
    })
    val cidActionAndCount: Map[String, Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
    val categoryCountInfoes: immutable.Iterable[CategoryCountInfo] = cidActionAndCount.map {
      case (cid, map) => {
        CategoryCountInfo(cid,
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L)
        )
      }
    }
    val result: Array[CategoryCountInfo] = categoryCountInfoes.toArray
      .sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount)).take(10)
    result.foreach(println)
    result
  }
}
