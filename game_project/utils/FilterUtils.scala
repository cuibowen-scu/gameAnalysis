package cn.itcast.game_project.utils

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by cbw on 2017/12/6.
  */
object FilterUtils {
  def filterByTime(time: Double): Boolean = {
    time <= 24
  }

  def filterByNoTypes(fields: Array[String], eventTypes: String*): Boolean = {
    val _type = fields(0)
    for(es <- eventTypes){
      if (es.equals(_type)){
        return false
      }
    }
    true
  }

  def filterByGender(fields: Array[String], str: String): Boolean = {
    val gender = fields(5)
    gender == str
  }

  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")
  def filterByTime(fields:Array[String],startTime:Long,endTime:Long):Boolean ={
    val time = fields(1)
    val logTime = dateFormat.parse(time).getTime
    logTime>=startTime && logTime<endTime
  }
  def filterByType(fields:Array[String],eventType: String):Boolean={
    val _type = fields(0)
    eventType == _type
  }
  def filterByTypes(fields:Array[String],eventTypes: String*):Boolean={
    val _type = fields(0)
    for (es <- eventTypes){
      if (es == _type){
        return true
      }
    }
    false
  }
  def filterByTypeAndTime(fields:Array[String],eventType: String,beginTime:Long,endTime:Long): Boolean ={
    val _type = fields(0)
    val _time = fields(1)
    val logTime = dateFormat.parse(_time).getTime
    eventType == _type && logTime >= beginTime && logTime < endTime
  }
}
