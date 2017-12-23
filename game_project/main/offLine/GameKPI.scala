package cn.itcast.game_project.main.offLine

import cn.itcast.game_project.constants.EventType
import cn.itcast.game_project.utils.{FilterUtils, IpUtils, SQLUtil, TimeUtils}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by cbw on 2017/12/6.
  */
object GameKPI {
  def main(args: Array[String]): Unit = {

    val queryTime = "2016-02-09 00:00:00"
    val sqlTime = TimeUtils.getSqlTime(queryTime)
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(+1)
    val d7rrTime = TimeUtils.getCertainDayTime(+7)
    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //原始数据
    val splitedLogs = sc.textFile("e://GameLog.txt").map(_.split("\\|"))
    //根据日期过滤后的数据并cache
    val filterLogs = splitedLogs.filter(fields => FilterUtils.filterByTime(fields,beginTime,endTime)).cache()


    //日新增用户数，Daily New Users 缩写 DNU
    val dnu = filterLogs.filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER)).count()
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into dnu(day,user_num) values (?,?)")
      prep.setString(1,sqlTime)
      prep.setInt(2,dnu.toInt)
      prep.executeUpdate
    }catch {
      case e:Exception => e.printStackTrace
    }
    println(dnu)


    //日活跃用户数 DAU （Daily Active Users）注册的和登录的都是活跃的
    val dau = filterLogs.filter(fields => FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN))
      .map(_(3)).distinct().count()
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into dau(day,user_num) values (?,?)")
      prep.setString(1,sqlTime)
      prep.setInt(2,dau.toInt)
      prep.executeUpdate
    }catch {
      case e:Exception => e.printStackTrace
    }
    println(dau)



    /*留存率：某段时间的新增用户数记为A，经过一段时间后，仍然使用的用户占新增用户A的比例即为留存率
    次日留存率（Day 1 Retention Ratio） Retention [rɪ'tenʃ(ə)n] Ratio ['reɪʃɪəʊ]
    日新增用户在+1日登陆的用户占新增用户的比例*/

    val todayRegUser = filterLogs.filter(fields => FilterUtils.filterByType(fields, EventType.REGISTER))
      .map(x => (x(3),1))
    val nextDayLoginUser = filterLogs.filter(fields => FilterUtils.filterByType(fields, EventType.LOGIN))
      .map(x => (x(3),1)).distinct()
    val d1r:Double = todayRegUser.join(nextDayLoginUser).count()
    println(d1r)
    val d1rr = d1r / todayRegUser.count()
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into d1rr(day,rate) values (?,?)")
      prep.setString(1,sqlTime)
      prep.setDouble(2,d1rr.toDouble)
      prep.executeUpdate
    }catch {
      case e:Exception => e.printStackTrace
    }
    println(d1rr)


    //七日留存 今天注册，7天后仍然登录的
    val next7DayLoginUser = splitedLogs.filter(fields => FilterUtils.filterByTypeAndTime(fields,EventType.LOGIN,beginTime,d7rrTime))
      .map(x => (x(3),1)).distinct()
    val d7r = todayRegUser.join(next7DayLoginUser).count()
    println(d7r)
    val d7rr = d7r / todayRegUser.count()
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into d7rr(day,rate) values (?,?)")
      prep.setString(1,sqlTime)
      prep.setDouble(2,d7rr.toDouble)
      prep.executeUpdate
    }catch {
      case e:Exception => e.printStackTrace
    }
    println(d7rr)



    //地区分布
    val ipRulesRdd = sc.textFile("e://ip.txt").map(line => {
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num,end_num,province)
    })
    //全部的ip映射规则
    val ipRulesArray = ipRulesRdd.collect()
    val ipRulesBroadcast = sc.broadcast(ipRulesArray)
    //全部ip
    val ipsRDD = splitedLogs.filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER))
      .map(fields => (fields(2)))
    //println(ipsRDD.collect().toBuffer)
    val result = ipsRDD.map(ip => {
      val ipNum = IpUtils.ip2Long(ip)
      var index = IpUtils.binarySearch(ipRulesBroadcast.value, ipNum)
      val rand = new Random()
      if (index<0) index = Math.abs(rand.nextInt(ipRulesBroadcast.value.length-1))
      val info = ipRulesBroadcast.value(index)
      info
    }).map(t => (t._3,1)).reduceByKey(_+_)
    result.foreachPartition(IpUtils.data2MySQL(_))



   //等级分布
   val legalLogs = splitedLogs.filter(fields => FilterUtils.filterByNoTypes(fields,EventType.COLLECT_GOODS,EventType.PK))
    val eachNameAndRank = legalLogs.map(fields => (fields(3),fields(6)))
    val nameAndRanks = eachNameAndRank.groupByKey()
    val nameAndRank = nameAndRanks.mapValues(ranks => {
      val ranksList = ranks.toList
      ranksList(ranksList.size-1)
    })
    val allRank = nameAndRank.map(_._2)
    /*println(allRank.collect().toBuffer)
    ArrayBuffer(46, 41, 6, 29, 16, 1, 26, 26, 41, 27, 25, 22, 46, 16, 38, 26, 26, 29, 27,)*/
    val ranks = Array(0,0,0,0,0)
    val allRankAsList = allRank.collect()
    for (i<- 0 to allRankAsList.length-1){
      val rank = allRankAsList(i).toInt
      if (rank<10){
        ranks(0) = ranks(0)+1
      }
      else if (rank<20){
        ranks(1) = ranks(1)+1
      }
      else if (rank<30){
        ranks(2) = ranks(2) +1
      }
      else if(rank<40){
        ranks(3) = ranks(3)+1
      }
      else {
        ranks(4) = ranks(4)+1
      }
    }
    //println(ranks(3))
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into rank(rank_block,count) values(?,?)")
      prep.setString(1,"1-10")
      prep.setInt(2,ranks(0))
      prep.addBatch()
      prep.setString(1,"10-20")
      prep.setInt(2,ranks(1))
      prep.addBatch()
      prep.setString(1,"20-30")
      prep.setInt(2,ranks(2))
      prep.addBatch()
      prep.setString(1,"30-40")
      prep.setInt(2,ranks(3))
      prep.addBatch()
      prep.setString(1,"40-50")
      prep.setInt(2,ranks(4))
      prep.addBatch()
      prep.executeBatch()
    }catch {
      case e:Exception => e.printStackTrace()
    }



    //每日平均游戏次数
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



    //每日平均游戏时长
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

    sc.stop()
  }
}
