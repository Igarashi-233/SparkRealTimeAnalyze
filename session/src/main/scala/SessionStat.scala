import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionStat {

  def main(args: Array[String]): Unit = {

    // 获取筛选条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取筛选条件对应的JsonObject
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)
    // 创建全局唯一主键
    val taskUUID: String = UUID.randomUUID().toString

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Session")
    // 创建SparkSession (包含Sparkcontext)
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取原始动作表数据
    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(spark, taskParam)

    //    actionRDD.foreach(println(_))


    val sessionID2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => {
      (item.session_id, item)
    })

    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionID2ActionRDD.groupByKey()

    session2GroupActionRDD.cache()
    //    session2GroupActionRDD.foreach(println(_))

    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(spark, session2GroupActionRDD)
    //    sessionId2FullInfoRDD.foreach(println(_))

    val sessionAccumulator = new SessionAccumulator
    spark.sparkContext.register(sessionAccumulator)

    // sessionId2FilterRDD 所有符合过滤条件的数据组成的RDD
    // getSessionFilterRDD 实现根据限制条件对session数据进行过滤 并完成累加器的更新
    val sessionId2FilterRDD: RDD[(String, String)] = getSessionFilterRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)
    sessionId2FilterRDD.foreach(println(_))

    getSessionRatios(spark, taskUUID, sessionAccumulator.value)


    /**
     * 需求二: session随机抽取
     */
    sessionRandomExtract(spark, taskUUID, sessionId2FilterRDD)


    /**
     * 需求三: Top10热门品类统计
     */
    // 获取符合条件的Action数据
    val sessionId2FilterActionRDD: RDD[(String, UserVisitAction)] = sessionID2ActionRDD.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    val top10CategoryArray: Array[(SortKey, String)] = top10PopularCategories(spark, taskUUID, sessionId2FilterActionRDD)


    /**
     * 需求四: Top10热门品类的Top10活跃session统计
     */
    top10ActiveSession(spark, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)

  }

  def top10ActiveSession(spark: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]): Unit = {
    import spark.implicits._
    /**
     * 过滤出所有点击过Top10品类的action
     */
    /*
    // Join 方法
     val cid2CountInfoRDD: RDD[(Long, String)] = spark.sparkContext.makeRDD(top10CategoryArray).map {
         case (sortkey, countInfo) =>
           val cid: Long = StringUtils.getFieldFromConcatString(countInfo, " \\| ", Constants.FIELD_CATEGORY_ID).toLong
           (cid, countInfo)
       }
       val cid2ActionRDD: RDD[(Long, UserVisitAction)] = sessionId2FilterActionRDD.map {
         case (sessionId, action) =>
           val cid: Long = action.click_category_id
           (cid, action)
       }
       val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = cid2CountInfoRDD.join(cid2ActionRDD).map {
         case (cid, (countInfo, action)) =>
           val sessionId: String = action.session_id
           (sessionId, action)
       }
    */
    // Filter方法
    val cidArray: Array[Long] = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, " \\| ", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    // 所有符合过滤条件 并且点击过Top10热门品类的action
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }
    sessionId2ActionRDD.foreach(println(_))
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    val cid2SessionCountRDD: RDD[(Long, String)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()

        for (action <- iterableAction) {
          val cid: Long = action.click_category_id
          if (!categoryCountMap.contains(cid)) {
            categoryCountMap += (cid -> 0)
          }
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)

        }
        // 一个session对于他所有点击过的品类的点击次数
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)

    }
    val cid2GroupRDD: RDD[(Long, Iterable[String])] = cid2SessionCountRDD.groupByKey()

    val top10SessionRDD: RDD[Top10Session] = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        // true: item1在前
        // false: item2在前
        val sortList: List[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)
        val top10Session: List[Top10Session] = sortList.map(item => {
          val sessionId: String = item.split("=")(0)
          val count: Long = item.split("=")(1).toLong
          Top10Session(taskUUID, cid, sessionId, count)
        })
        top10Session

    }
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_1205")
      .mode(SaveMode.Append)
      .save()

  }


  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    val clickFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        action.click_category_id != -1L
    }

    val clickNumRDD: RDD[(Long, Long)] = clickFilterRDD.map {
      case (session, action) =>
        (action.click_category_id, 1L)
    }
    clickNumRDD.reduceByKey(_ + _)

  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    val orderFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionid, action) =>
        action.order_category_ids != null
    }

    val orderNumCountRDD: RDD[(Long, Long)] = orderFilterRDD.flatMap {
      case (sessionid, action) =>
        val array: Array[String] = action.order_category_ids.split(",")
        array.map(item => {
          (item.toLong, 1L)
        })
    }
    orderNumCountRDD.reduceByKey(_ + _)

  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    val payFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionid, action) =>
        action.pay_category_ids != null
    }

    val payNumCountRDD: RDD[(Long, Long)] = payFilterRDD.flatMap {
      case (sessionid, action) =>
        val array: Array[String] = action.pay_category_ids.split(",")
        array.map(item => {
          (item.toLong, 1L)
        })
    }
    payNumCountRDD.reduceByKey(_ + _)
  }

  def getFullCount(categoryId2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {

    val cid2ClickInfoRDD: RDD[(Long, String)] = categoryId2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryID, option)) =>
        val clickCount: Long = if (option.isDefined) option.get else 0

        val aggrCount: String = Constants.FIELD_CATEGORY_ID + "=" + cid + " | " +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCount)
    }

    val cid2OrderInfoRDD: RDD[(Long, String)] = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount: Long = if (option.isDefined) option.get else 0

        val aggrCount: String = clickInfo + " | " + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrCount)
    }

    val cid2PayInfoRDD: RDD[(Long, String)] = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount: Long = if (option.isDefined) option.get else 0

        val aggrInfo: String = orderInfo + " | " + Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }

    cid2PayInfoRDD

  }

  def top10PopularCategories(spark: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]): Array[(SortKey, String)] = {
    import spark.implicits._

    // 获取所有发生过点击 下单 付款的品类
    var categoryId2CidRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (sessionId, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        if (action.click_category_id != -1) { // 点击行为
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) { // 下单行为
          for (orderCid <- action.order_category_ids.split(",")) {
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
          }
        } else if (action.pay_category_ids != null) { // 付款行为
          for (payCid <- action.pay_category_ids.split(",")) {
            categoryBuffer += ((payCid.toLong, payCid.toLong))
          }
        }

        categoryBuffer
    }
    categoryId2CidRDD = categoryId2CidRDD.distinct()

    // 统计品类点击次数
    val cid2ClickCountRDD: RDD[(Long, Long)] = getClickCount(sessionId2FilterActionRDD)
    // 统计品类下单次数
    val cid2OrderCountRDD: RDD[(Long, Long)] = getOrderCount(sessionId2FilterActionRDD)
    // 统计品类付款次数
    val cid2PayCountRDD: RDD[(Long, Long)] = getPayCount(sessionId2FilterActionRDD)

    val cid2FullCountRDD: RDD[(Long, String)] = getFullCount(categoryId2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
    //    cid2FullCountRDD.foreach(println(_))

    // 实现自定义二次排序
    val sortKey2FullCountInfoRDD: RDD[(SortKey, String)] = cid2FullCountRDD.map {
      case (cid, fullCountInfo) =>
        val clickCount: Long = StringUtils.getFieldFromConcatString(fullCountInfo, " \\| ", Constants
          .FIELD_CLICK_COUNT).toLong
        val orderCount: Long = StringUtils.getFieldFromConcatString(fullCountInfo, " \\| ", Constants
          .FIELD_ORDER_COUNT).toLong
        val payCount: Long = StringUtils.getFieldFromConcatString(fullCountInfo, " \\| ", Constants
          .FIELD_PAY_COUNT).toLong

        val sortKey: SortKey = SortKey(clickCount, orderCount, payCount)

        (sortKey, fullCountInfo)
    }

    val top10CategoryArray: Array[(SortKey, String)] = sortKey2FullCountInfoRDD.sortByKey(ascending = false).take(10)
    top10CategoryArray.foreach(println(_))

    val top10CategoryRDD: RDD[Top10Category] = spark.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, " \\| ", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount: Long = sortKey.clickCount
        val orderCount: Long = sortKey.OrderCount
        val payCount: Long = sortKey.payCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)

    }

    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_1205")
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray

  }


  def generateRandomIndexList(extractPerDay: Long,
                              daySessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]): Unit = {

    for ((hour, count) <- hourCountMap) {
      // 获取一个小时要抽取多少条数据
      var hourExtractCount: Int = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      // 避免要抽取的数量超过总数
      if (hourExtractCount > count) {
        hourExtractCount = count.toInt
      }

      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (_ <- 0 until hourExtractCount) {
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }

            hourListMap(hour).append(index)
          }

        case Some(_) =>
          for (_ <- 0 until hourExtractCount) {
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }

            hourListMap(hour).append(index)
          }
      }

    }

  }

  def sessionRandomExtract(spark: SparkSession,
                           taskUUID: String,
                           sessionId2FilterRDD: RDD[(String, String)]): Unit = {
    import spark.implicits._

    val dateHour2FullInfoRDD: RDD[(String, String)] = sessionId2FilterRDD.map {
      case (sessionId, fullInfo) =>
        val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants.FIELD_START_TIME)
        // dateHour(yyyy-MM-dd_HH)
        val dateHour: String = DateUtils.getDateHour(startTime)

        (dateHour, fullInfo)
    }

    // 每个小时的session数量
    val hourCountMap: collection.Map[String, Long] = dateHour2FullInfoRDD.countByKey()

    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(_) => dateHourCountMap(date) += (hour -> count)
      }

    }

    // 获取总共天数
    val size: Int = dateHourCountMap.size
    // 获取每天抽取的session数(总共抽取100条数据)
    val extractPerDay: Int = 100 / size


    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    /**
     * 获取每个小时要抽取session的随机index (generateRandomIndexList)
     */
    for ((date, hourCountMap) <- dateHourCountMap) {
      // 获取每天有多少session
      val dateSessionCount: Long = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))

        case Some(_) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))

      }

      // 设置广播变量
      val dateHourExtractIndexListMapBD: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]]
      = spark.sparkContext.broadcast(dateHourExtractIndexListMap)

      val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullInfoRDD.groupByKey()

      val extractSessionRDD: RDD[SessionRandomExtract] = dateHour2GroupRDD.flatMap {
        case (dateHour, iterableFullInfo) =>
          val date: String = dateHour.split("_")(0)
          val hour: String = dateHour.split("_")(1)

          val extractList: ListBuffer[Int] = dateHourExtractIndexListMapBD.value(date)(hour)
          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for (fullInfo <- iterableFullInfo) {
            if (extractList.contains(index)) {
              val sessionID: String = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants
                .FIELD_SESSION_ID)
              val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants
                .FIELD_START_TIME)
              val searchKeyWords: String = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants
                .FIELD_SEARCH_KEYWORDS)
              val clickCategoryIDs: String = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants
                .FIELD_CLICK_CATEGORY_IDS)

              val extractSession: SessionRandomExtract = SessionRandomExtract(taskUUID, sessionID, startTime,
                searchKeyWords, clickCategoryIDs)

              extractSessionArrayBuffer += extractSession

            }
            index += 1
          }
          extractSessionArrayBuffer
      }

      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract_1205")
        .mode(SaveMode.Append)
        .save()
    }


  }


  def getSessionRatios(spark: SparkSession,
                       taskUUID: String,
                       value: mutable.HashMap[String, Int]): Unit = {

    val session_count: Double = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble
    /**
     * 不同范围访问时长的session个数
     */
    val visitLength_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visitLength_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visitLength_7s_9s: Int = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visitLength_10s_30s: Int = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visitLength_30s_60s: Int = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visitLength_1m_3m: Int = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visitLength_3m_10m: Int = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visitLength_10m_30m: Int = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visitLength_30m: Int = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    /**
     * 不同范围访问步长的session个数
     */
    val stepLength_1_3: Int = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val stepLength_4_6: Int = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val stepLength_7_9: Int = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val stepLength_10_30: Int = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val stepLength_30_60: Int = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val stepLength_60: Int = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    /**
     * 比率计算
     */
    val visitLength_1s_3s_ratio: Double = NumberUtils.formatDouble(visitLength_1s_3s / session_count, 2)
    val visitLength_4s_6s_ratio: Double = NumberUtils.formatDouble(visitLength_4s_6s / session_count, 2)
    val visitLength_7s_9s_ratio: Double = NumberUtils.formatDouble(visitLength_7s_9s / session_count, 2)
    val visitLength_10s_30s_ratio: Double = NumberUtils.formatDouble(visitLength_10s_30s / session_count, 2)
    val visitLength_30s_60s_ratio: Double = NumberUtils.formatDouble(visitLength_30s_60s / session_count, 2)
    val visitLength_1m_3m_ratio: Double = NumberUtils.formatDouble(visitLength_1m_3m / session_count, 2)
    val visitLength_3m_10m_ratio: Double = NumberUtils.formatDouble(visitLength_3m_10m / session_count, 2)
    val visitLength_10m_30m_ratio: Double = NumberUtils.formatDouble(visitLength_10m_30m / session_count, 2)
    val visitLength_30m_ratio: Double = NumberUtils.formatDouble(visitLength_30m / session_count, 2)

    val stepLength_1_3_ratio: Double = NumberUtils.formatDouble(stepLength_1_3 / session_count, 2)
    val stepLength_4_6_ratio: Double = NumberUtils.formatDouble(stepLength_4_6 / session_count, 2)
    val stepLength_7_9_ratio: Double = NumberUtils.formatDouble(stepLength_7_9 / session_count, 2)
    val stepLength_10_30_ratio: Double = NumberUtils.formatDouble(stepLength_10_30 / session_count, 2)
    val stepLength_30_60_ratio: Double = NumberUtils.formatDouble(stepLength_30_60 / session_count, 2)
    val stepLength_60_ratio: Double = NumberUtils.formatDouble(stepLength_60 / session_count, 2)


    val stat: SessionAggrStat = SessionAggrStat(taskUUID, session_count.toInt, visitLength_1s_3s_ratio, visitLength_4s_6s_ratio,
      visitLength_7s_9s_ratio, visitLength_10s_30s_ratio, visitLength_30s_60s_ratio, visitLength_1m_3m_ratio,
      visitLength_3m_10m_ratio, visitLength_10m_30m_ratio, visitLength_30m_ratio, stepLength_1_3_ratio,
      stepLength_4_6_ratio, stepLength_7_9_ratio, stepLength_10_30_ratio, stepLength_30_60_ratio,
      stepLength_60_ratio)

    val sessionRatioRDD: RDD[SessionAggrStat] = spark.sparkContext.makeRDD(Array(stat))

    import spark.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_1205")
      .mode(SaveMode.Append)
      .save()

  }


  def getOriActionRDD(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {

    // 获取Json字段中的起始时间
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    // 获取Json字段中的截止时间
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from user_visit_action where date>='" + startDate + "' and date<= '" + endDate + "' "

    import spark.implicits._
    spark.sql(sql).as[UserVisitAction].rdd

  }

  def getSessionFullInfo(spark: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[(String, String)] = {

    import spark.implicits._

    val userID2AggrInfoRDD: RDD[(Long, String)] = session2GroupActionRDD.map {
      case (sessionID, iterableAction) =>
        var userID: Long = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0
        val searchKeyWords = new StringBuffer("")
        val clickCategories = new StringBuffer("")


        for (action <- iterableAction) {
          if (userID == -1L) {
            userID = action.user_id
          }

          val actionTime: Date = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword: String = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeyWords.toString.contains(searchKeyword)) {
            searchKeyWords.append(searchKeyword + ",")
          }

          val clickCategoryId: Long = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1

        }
        val searchKw: String = StringUtils.trimComma(searchKeyWords.toString)
        val clickCg: String = StringUtils.trimComma(clickCategories.toString)

        val visitLengtgh: Long = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo: String = Constants.FIELD_SESSION_ID + "=" + sessionID + " | " +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + " | " +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + " | " +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLengtgh + " | " +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + " | " +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userID, aggrInfo)
    }
    //    userID2AggrInfoRDD
    val sql = "select * from user_info"
    val userID2InfoRDD: RDD[(Long, UserInfo)] = spark.sql(sql).as[UserInfo].rdd.map(item => {
      (item.user_id, item)
    })

    val sessionId2FullInfoRDD: RDD[(String, String)] = userID2AggrInfoRDD.join(userID2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val age: Int = userInfo.age
        val professional: String = userInfo.professional
        val sex: String = userInfo.sex
        val city: String = userInfo.city

        val fullInfo: String = aggrInfo + " | " +
          Constants.FIELD_AGE + "=" + age + " | " +
          Constants.FIELD_PROFESSIONAL + "=" + professional + " | " +
          Constants.FIELD_SEX + "=" + sex + " | " +
          Constants.FIELD_CITY + "=" + city

        val sessionId: String = StringUtils.getFieldFromConcatString(aggrInfo, " \\| ", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)

    }
    sessionId2FullInfoRDD

  }

  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    /**
     * visitLength过滤分配
     */
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }


  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator): Unit = {
    /**
     * stepLength过滤分配
     */
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilterRDD(taskParam: JSONObject,
                          sessionId2FullInfoRDD: RDD[(String, String)],
                          sessionAccumulator: SessionAccumulator): RDD[(String, String)] = {
    /**
     * task.params.json中限制条件对应的常量字段
     */
    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keyWords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIDs: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + " | " else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + " | " else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + " | " else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + " | " else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + " | " else "") +
      (if (keyWords != null) Constants.PARAM_KEYWORDS + "=" + keyWords + " | " else "") +
      (if (categoryIDs != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIDs else "")

    if (filterInfo.endsWith(" \\| ")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 3)
    }

    sessionId2FullInfoRDD.filter {
      case (sessionID, fullInfo) =>
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants
          .PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)

          val visitLength: Long = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants
            .FIELD_VISIT_LENGTH).toLong

          val stepLength: Long = StringUtils.getFieldFromConcatString(fullInfo, " \\| ", Constants
            .FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)

        }
        success
    }

  }

}
