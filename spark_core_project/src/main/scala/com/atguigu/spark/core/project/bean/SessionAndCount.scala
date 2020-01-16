package com.atguigu.spark.core.project.bean

case class SessionAndCount(sid:String,count:Long) extends Ordered[SessionAndCount]{
  override def compare(that: SessionAndCount): Int = if(this.count > that.count) -1 else 1
}
