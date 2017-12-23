package cn.itcast.game_project.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by cbw on 2017/12/22.
  */
object IpUtils {
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
}
