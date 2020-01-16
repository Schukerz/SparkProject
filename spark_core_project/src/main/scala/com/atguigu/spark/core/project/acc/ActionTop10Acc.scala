package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

//Map[(String,String),Long]
class ActionTop10Acc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{

  private var map:Map[(String,String),Long] = Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc: ActionTop10Acc = new ActionTop10Acc
    acc.map=map
    acc
  }

  override def reset(): Unit = {map = Map[(String,String),Long]()}

  override def add(v: UserVisitAction): Unit = {

    if(v.click_category_id != -1){

      val categoryClick: (String, String) = (v.click_category_id.toString,"click")
      map += (categoryClick -> (map.getOrElse(categoryClick,0L)+1L))

    }else if(v.order_category_ids != "null"){

      val cids: Array[String] = v.order_category_ids.split(",")
      cids.foreach(cid =>{
        map += ((cid,"order") -> (map.getOrElse((cid,"order"),0L)+1L))
      })

    }else if(v.pay_category_ids != "null"){

      val cids: Array[String] = v.pay_category_ids.split(",")
      cids.foreach(cid =>{
        map += ((cid,"pay") -> (map.getOrElse((cid,"pay"),0L)+1L))
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = other match{
    case o : ActionTop10Acc => {
      this.map = o.map.foldLeft(this.map){
        case (map,(cidAction,count)) =>{
          map + (cidAction ->(map.getOrElse(cidAction,0L)+count))
        }
      }
    }
  }

  override def value: Map[(String, String), Long] = map
}
