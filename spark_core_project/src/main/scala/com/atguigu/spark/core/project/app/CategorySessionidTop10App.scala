package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, SessionAndCount, UserVisitAction}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.TreeSet

object CategorySessionidTop10App {

  def sessionidTop10(userVisitActionRDD: RDD[UserVisitAction],
                     categoryCountInfo: Array[CategoryCountInfo])={
    val cidTop10: Array[String] = categoryCountInfo.map(_.cid)
    val cidSidAndCount: RDD[((String, String), Int)] = userVisitActionRDD.filter(action => cidTop10.contains(action.click_category_id.toString))
      .map(action => {
        ((action.click_category_id.toString, action.session_id), 1)
      })
      .reduceByKey(_ + _)

    val resultRDD: RDD[(String, List[SessionAndCount])] = cidSidAndCount.groupBy(_._1._1).mapValues(it => {

      var set: mutable.TreeSet[SessionAndCount] = new TreeSet[SessionAndCount]()
      it.foreach {
        case ((cid, sid), count) =>
          set += SessionAndCount(sid, count)
          if (set.size > 10) set = set.take(10)

      }
      set.toList
    })
    resultRDD.collect.foreach(println)



  }
}
