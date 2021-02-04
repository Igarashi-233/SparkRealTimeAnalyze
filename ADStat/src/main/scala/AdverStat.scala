import java.lang
import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("adver")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //    StreamingContext.getActiveOrCreate()
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    val kafka_brokers: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      //    latest: 先去Zookeeper获取offset 如果有直接使用  如果没有则从最新的数据开始消费
      //    earliest: 先去Zookeeper获取offset 如果有直接使用  如果没有则从头的数据开始消费
      //    none: 先去Zookeeper获取offset 如果有直接使用  如果没有则报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    // adRealTimeDStream: DStream[RDD ,RDD ,RDD .....]  RDD[message]
    // message: key value
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出DStream中每一条数据的value
    // adRealTimeValueDStream: DStream[RDD,RDD.....]  RDD[String]
    // String: timestamp  province  city  userid  adid
    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream: DStream[String] = adRealTimeValueDStream.transform {
      logRDD =>
        // blacklistArray: Array[AdBlacklist]
        // AdBlacklist: userId
        val blacklistArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()
        // userIdArray: Array[(Long)userId1, (Long)userId2, (Long)userId3.....]
        val userIdArray: Array[Long] = blacklistArray.map(item => item.userid)

        logRDD.filter {
          // log: timestamp  province  city  userid  adid
          log =>
            val logSplit: Array[String] = log.split(" ")
            val userId: Long = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }

    //    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))


    streamingContext.checkpoint("./spark-streaming")
    adRealTimeFilterDStream.checkpoint(Duration(10000))


    /**
     * 需求一: 实时维护黑名单
     */
    generateBlackList(adRealTimeFilterDStream)

    /**
     * 需求二: 各省城市一天中的广告点击量(累计统计)
     */
    val key2ProvinceCityCountDStream: DStream[(String, Long)] = provinceCityClickStat(adRealTimeFilterDStream)

    /**
     * 需求三: 统计各省Top3热门广告
     */
    provinceTop3Adver(spark, key2ProvinceCityCountDStream)

    /**
     * 需求四: 最近一个小时广告点击量统计
     */
    getRecentHourClickCount(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()

  }


  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]): Unit = {

    val key2TimeMinuteDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map(
      // log: timestamp province city userId adid
      log => {
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        // yyyyMMddHHmm
        val timeMinute: String = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid: Long = logSplit(4).toLong

        val key: String = timeMinute + "_" + adid

        (key, 1L)
      }
    )

    val key2WindowDStream: DStream[(String, Long)] = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) =>
      a + b, Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          // (key, count)
          items =>
            val trendArray = new ArrayBuffer[AdClickTrend]()
            for ((key, count) <- items) {
              val keySplit: Array[String] = key.split("_")
              val timeMinutes: String = keySplit(0)

              // yyyyMMddHHmm
              val date: String = timeMinutes.substring(0, 8)
              val hour: String = timeMinutes.substring(8, 10)
              val minutes: String = timeMinutes.substring(10)

              val adid: Long = keySplit(1).toLong

              trendArray += AdClickTrend(date, hour, minutes, adid, count)
            }
            AdClickTrendDAO.updateBatch(trendArray.toArray)
        }
    }

  }

  def provinceTop3Adver(spark: SparkSession,
                        key2ProvinceCityCountDStream: DStream[(String, Long)]): Unit = {
    import spark.implicits._
    // key2ProvinceCityCountDStream: RDD[(key, count)]
    // key: date_province_city_adid
    // key2ProvinceCountDStream: RDD[(newKey, count)]
    // newKey: date_province_adid
    val key2ProvinceCountDStream: DStream[(String, Long)] = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val province: String = keySplit(1)
        val adid: String = keySplit(3)

        val newKey: String = date + "_" + province + "_" + adid

        (newKey, count)
    }

    val key2ProvinceAggrCountDStream: DStream[(String, Long)] = key2ProvinceCountDStream.reduceByKey(_ + _)

    val top3DStream: DStream[Row] = key2ProvinceAggrCountDStream.transform {
      rdd =>
        // rdd: RDD[(key, value)]
        // key: date_province_adid
        val basicDateRDD: RDD[(String, String, Long, Long)] = rdd.map {
          case (key, count) =>
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val province: String = keySplit(1)
            val adid: Long = keySplit(2).toLong

            (date, province, adid, count)
        }
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql: String = "select t.date, t.province, t.adid, t.count from (" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date, province order by count desc) rank " +
          "from tmp_basic_info ) t " +
          "where t.rank <= 3"

        spark.sql(sql).rdd
    }

    top3DStream.foreachRDD {
      // rdd: RDD[row]
      rdd =>
        rdd.foreachPartition {
          // items: row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items) {
              val date: String = item.getAs[String]("date")
              val province: String = item.getAs[String]("province")
              val adid: Long = item.getAs[Long]("adid")
              val count: Long = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }

  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]): DStream[(String, Long)] = {
    // adRealTimeFilterDStream: DStream[RDD[String]]
    // String -> log: timestamp  province  city  userid  adid
    // key2ProvinceCityDStream: DStream[RDD[(key, 1L)]]
    val key2ProvinceCityDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map(log => {
      val logSplit: Array[String] = log.split(" ")
      val timeStamp: Long = logSplit(0).toLong
      // yy-mm-dd
      val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))
      val province: String = logSplit(1)
      val city: String = logSplit(2)
      val adid: String = logSplit(4)

      val key: String = dateKey + "_" + province + "_" + city + "_" + adid

      (key, 1L)
    })

    // key2StateDStream: 某一天一个省的一个城市中某一个广告的点击次数
    val key2StateDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newVal = 0L
        if (state.isDefined)
          newVal = state.get
        for (value <- values) {
          newVal += value
        }
        Some(newVal)
    }

    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStateArray = new ArrayBuffer[AdStat]()
            // key: date  province  city  adid
            for ((key, count) <- items) {
              val keySplit: Array[String] = key.split("_")
              val date: String = keySplit(0)
              val province: String = keySplit(1)
              val city: String = keySplit(2)
              val adid: Long = keySplit(3).toLong

              adStateArray += AdStat(date, province, city, adid, count)
            }

            AdStatDAO.updateBatch(adStateArray.toArray)
        }
    }

    key2StateDStream
  }

  def generateBlackList(adRealTimeFilterDStream: DStream[String]): Unit = {
    // adRealTimeFilterDStream: DStream[RDD[String]]
    // String -> log: timestamp  province  city  userid  adid
    // key2NumDStream: DStream[RDD[(key, 1L)]]
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        //yy-mm-dd
        val dateKey: String = DateUtils.formatDate(new Date(timeStamp))
        val userId: Long = logSplit(3).toLong
        val adid: Long = logSplit(4).toLong

        val key: String = dateKey + "_" + userId + "_" + adid

        (key, 1L)
    }
    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_ + _)

    //根据每一个RDD中的数据 更新用户点击次数表
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          item =>
            val clickCountArray = new ArrayBuffer[AdUserClickCount]()

            for ((key, count) <- item) {
              val keySplit: Array[String] = key.split("_")
              val date: String = keySplit(0)
              val userId: Long = keySplit(1).toLong
              val adid: Long = keySplit(2).toLong

              clickCountArray += AdUserClickCount(date, userId, adid, count)
            }

            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)

        }
    }

    // key2BlackListDStream: DSream[RDD[(key, count)]]
    val key2BlackListDStream: DStream[(String, Long)] = key2CountDStream.filter {
      case (key, count) =>
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val userId: Long = keySplit(1).toLong
        val adid: Long = keySplit(2).toLong

        val clickCount: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        if (clickCount > 100) {
          true
        } else {
          false
        }
    }

    // userIdDStream: DStream[RDD[UserId]](去重后)
    val userIdDStream: DStream[Long] = key2BlackListDStream.map {
      case (key, count) =>
        key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val userIdArray = new ArrayBuffer[AdBlacklist]()

            for (userId <- items) {
              userIdArray += AdBlacklist(userId)
            }

            AdBlacklistDAO.insertBatch(userIdArray.toArray)

        }
    }


  }
}
