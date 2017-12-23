package cn.itcast.game_project.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by cbw on 2017/12/6.
  */
object TimeUtils {
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()
  def apply(time:String): Long ={
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }
  def getCertainDayTime(amount:Int):Long={
    calendar.add(Calendar.DATE,amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE,-amount)
    time
  }
  def getSqlTime(time:String)={
    val res = time.split(" ")
    res(0).split("\\-")(1)+"-"+res(0).split("\\-")(2)
  }
}
