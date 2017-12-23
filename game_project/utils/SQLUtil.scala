package cn.itcast.game_project.utils

import java.sql.DriverManager

/**
  * Created by cbw on 2017/12/20.
  */
object SQLUtil {
  def getConnection()={
    val databaseMessage = "jdbc:mysql://localhost:3306/game?user=root&password=root"
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(databaseMessage)
    conn
  }
}
