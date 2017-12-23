package cn.itcast.game_project.main.offLine

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import cn.itcast.game_project.constants.EventType
import cn.itcast.game_project.utils.FilterUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by cbw on 2017/12/22.
  */
object ProvinceInfo {

  def ip2Long(ip:String):Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum<<8L
    }
    ipNum
  }
  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int ={
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  val data2MySQL = (iterator: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps : PreparedStatement = null
    val sql = "INSERT INTO province (name, user) VALUES (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/game", "root", "root")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProvinceInfo").setMaster("local[*]")
    val sc = new SparkContext(conf)
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
    val ipsRDD = sc.textFile("e://GameLog.txt").map(_.split("\\|"))
      .filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER)).map(fields => (fields(2)))
    //println(ipsRDD.collect().toBuffer)
    val result = ipsRDD.map(ip => {
      val ipNum = ip2Long(ip)
      var index = binarySearch(ipRulesBroadcast.value, ipNum)
      val rand = new Random()
      if (index<0) index = Math.abs(rand.nextInt(ipRulesBroadcast.value.length-1))
      val info = ipRulesBroadcast.value(index)
      info
    }).map(t => (t._3,1)).reduceByKey(_+_)
    result.foreachPartition(data2MySQL(_))
    sc.stop()
  }
}
