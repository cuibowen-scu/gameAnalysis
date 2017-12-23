package cn.itcast.game_project.main.offLine

import cn.itcast.game_project.constants.EventType
import cn.itcast.game_project.utils.{FilterUtils, SQLUtil}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbw on 2017/12/20.
  */
object GenderInfo {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("GenderInfo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val splitedLogs = sc.textFile("e://GameLog.txt").map(_.split("\\|"))
    val allUser = splitedLogs.filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER))
    val maleUser = allUser.filter(fields => FilterUtils.filterByGender(fields,"男"))
    val maleUserNumber = maleUser.count
    val femaleUserNumber = allUser.count() - maleUserNumber
    try{
      val prep = SQLUtil.getConnection().prepareStatement("insert into gender(sex,number) values(?,?)")
      prep.setString(1,"male")
      prep.setInt(2,maleUserNumber.toInt)
      prep.addBatch()
      prep.setString(1,"female")
      prep.setInt(2,femaleUserNumber.toInt)
      prep.addBatch()
      prep.executeBatch()
    }catch {
      case e:Exception => e.printStackTrace()
    }
    sc.stop()
  }
}
