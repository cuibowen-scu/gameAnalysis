package cn.itcast.game_project.main.offLine

import cn.itcast.game_project.constants.EventType
import cn.itcast.game_project.utils.{FilterUtils, SQLUtil, TimeUtils}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbw on 2017/12/23.
  */
object GameTimeInfo {
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
    //总活跃用户数
    val totalUser = registAndLoginLogs.map(fields => fields(3)).distinct().count()
    //println(totalUser)
    val registAndLoginAndLogoutLogs = filterLogs.filter(fields => FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN,EventType.LOGOUT))
      .map(fields => (fields(3),fields(0),fields(1)))
    /*println(registAndLoginAndLogoutLogs.collect().toBuffer)
    ArrayBuffer((李明克星,1,2016年2月1日,星期一,10:01:08), (风道,1,2016年2月1日,星期一,10:01:12), (主宰,1,2016年2月1日,星期一,10:01:37),
    */
    val formatLogs = registAndLoginAndLogoutLogs.map(fields => {
      val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")
      val name = fields._1
      val logType = fields._2
      val time = dateFormat.parse(fields._3).getTime
      (name,(logType,time))
    })
    val everyoneLogs = formatLogs.groupByKey
    /*println(everyoneLogs.collect().toBuffer)
    ArrayBuffer((妹特思棒威,CompactBuffer((1,1454297220000), (3,1454303595000), (2,1454303602000),
      (3,1454305675000), (2,1454305681000), (3,1454309654000), (2,1454309660000),....*/
    val everyoneTotalTime = everyoneLogs.mapValues(it => {
      val list = it.toList
      var totalLoginTime = 0L
      var totalLogoutTime = 0L
      for(i <- 0 to it.size-1){
        val tuple = list(i)
        val logType = tuple._1
        val timeLong = tuple._2
        if (logType.equals("3")){
          totalLogoutTime = totalLogoutTime+timeLong
        }else{
          totalLoginTime = totalLoginTime+timeLong
        }
      }
      var finalTime = totalLogoutTime - totalLoginTime
      if(list(0)._1.equals("3")){
        finalTime = finalTime - beginTime
      }
      if(!list(list.size-1)._1.equals("3")){
        finalTime = finalTime + endTime
      }
      finalTime
    })
    /*println(everyoneTotalTime.collect().toBuffer)
    ArrayBuffer((妹特思棒威,45036000), (琳琳,1454375290000), (逗爷,1454342626000),
      (沉寂落寞,1454344364000), (水电费,1454342579000),..........*/
    val timeLogs = everyoneTotalTime.map(_._2)
    //println(timeLogs.collect().toBuffer)
    val formatTimeLogs = timeLogs.map(timeLong => {
      timeLong.toDouble/(1000*60*60).toDouble
    })
    val totalTime = formatTimeLogs.filter(time => FilterUtils.filterByTime(time)).sum
    val averageTime = (totalTime/totalUser).formatted("%.2f")
    //println(averageTime)------------1.18
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into average_game_time(day,time) values (?,?)")
      prep.setString(1,sqlTime)
      prep.setDouble(2,averageTime.toDouble)
      prep.executeUpdate()
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
