package cn.itcast.game_project.main.streaming

import cn.itcast.game_project.tools.JedisConnectionPool
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by cbw on 2017/12/7.
  */
object PlugsScanner {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val Array(zkQuorum, group, topics, numThreads) = Array("shizhan1:2181,shizhan2:2181,shizhan3:2181", "g2", "gamelog", "2")
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("PlugsScanner").setMaster("local[2]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(10000))
    ssc.checkpoint("e://PlugsScanner2")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    val dstream = KafkaUtils.createStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParams,topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.map(_._2)
    val splitedLines = lines.map(_.split("\t"))
    val filterLines = splitedLines.filter(f => {
      val et = f(3)
      val item = f(8)
      et == "11" && item == "强效太阳水"
    })
    //按用户和窗口分组
    val groupedWindow: DStream[(String, Iterable[Long])] = filterLines.map(f => (f(7),dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(30000),Milliseconds(20000))
    //找出窗口内使用次数超过5的((String, Iterable[Long]))
    val filtered = groupedWindow.filter(_._2.size>=5)
    //mapValues只对value进行处理，会返回(key,time)
    val itemAvgTime = filtered.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size
      val first = list(0)
      val last = list(size-1)
      val cha:Double = last - first
      cha/size
    })
    val badUser: DStream[(String, Double)] = itemAvgTime.filter(_._2 < 10000)
    //过滤找出开挂用户
    badUser.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val connection  = JedisConnectionPool.getConnection()
        it.foreach(t => {
          val user = t._1
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          connection.set(user+"_"+currentTime,avgTime.toString)
        })
        connection.close()
      })
    })
    ssc.start
    ssc.awaitTermination
  }
}
