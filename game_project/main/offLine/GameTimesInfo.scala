package cn.itcast.game_project.main.offLine

import cn.itcast.game_project.constants.EventType
import cn.itcast.game_project.utils.{FilterUtils, SQLUtil, TimeUtils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbw on 2017/12/23.
  */
object GameTimesInfo {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-01 00:00:00"
    val sqlTime = TimeUtils.getSqlTime(queryTime)
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(+1)
    val conf = new SparkConf().setAppName("RankInfo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val splitedLogs = sc.textFile("e://GameLog.txt").map(_.split("\\|"))
    val filterLogs = splitedLogs.filter(fields => FilterUtils.filterByTime(fields,beginTime,endTime)).cache()
    val registAndLoginLogs = filterLogs.filter(fields => FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN))
    //总次数
    val totalTimes = registAndLoginLogs.count()
    val totalUser = registAndLoginLogs.map(fields => fields(3)).distinct().count()
    val averageTimes = (totalTimes.toDouble/totalUser.toDouble).formatted("%.2f")
    //println(averageTimes)
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into average_game_times(day,times) values (?,?)")
      prep.setString(1,sqlTime)
      prep.setDouble(2,averageTimes.toDouble)
      prep.executeUpdate()
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
