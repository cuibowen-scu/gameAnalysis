package cn.itcast.game_project.main.offLine

import cn.itcast.game_project.constants.EventType
import cn.itcast.game_project.utils.{FilterUtils, SQLUtil}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbw on 2017/12/22.
  */
object RankInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RankInfo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val splitedLogs = sc.textFile("e://GameLog.txt").map(_.split("\\|"))
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
  }
}
