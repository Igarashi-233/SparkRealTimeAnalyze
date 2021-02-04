import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageConvertStat {

  def main(args: Array[String]): Unit = {

    // 获取任务限制条件
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    // 获取唯一主键
    val taskUUID: String = UUID.randomUUID().toString

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("pageConvert")

    // 创建SparkSession
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAvtion(spark, taskParam)
    // sessionId2ActionRDD.foreach(println(_))

    // pageFlow: "1,2,3,4,5,6,7"
    val pageFlow: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArray: [1,2,3,4,5,6,7]
    val pageFlowArray: Array[String] = pageFlow.split(",")

    // pageFlowArray.slice(0, pageFlowArray.length - 1): [1,2,3,4,5,6]
    // pageFlowArray.tail: [2,3,4,5,6,7]
    // pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail): [(1,2),(2,3),(3,4),(4,5),(5,6),(6,7)]
    // targetPageSplit: [1_2, 2_3, 3_4, 4_5, 5_6, 6_7]
    val targetPageSplit: Array[String] = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) =>
        page1 + "_" + page2
    }

    // sessionId2ActionRDD: RDD[(sessionId, action)]
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    // pageSplitNumRDD: RDD[(String, 1L)]
    val pageSplitNumRDD: RDD[(String, Long)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val sortList: List[UserVisitAction] = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })
        //pageList: List[Long] : [1,2,3,4......]
        val pageList: List[Long] = sortList.map(action => {
          action.page_id
        })

        val pageSplit: List[String] = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) =>
            page1 + "_" + page2
        }

        val pageFilterSplit: List[String] = pageSplit.filter(
          pageSplit =>
            targetPageSplit.contains(pageSplit)
        )

        val pageSplitNum: List[(String, Long)] = pageFilterSplit.map(
          pageSplit =>
            (pageSplit, 1L)
        )

        pageSplitNum
    }

    // pageSplitCountMap: Map[pageSplit, count]
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    val startPage: Long = pageFlowArray(0).toLong
    val startPageCount: Long = sessionId2ActionRDD.filter {
      case (sessionId, action) =>
        action.page_id == startPage
    }.count()

    getPageConvert(spark, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)

  }


  def getPageConvert(spark: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]): Unit = {
    import spark.implicits._

    val pageSplitRatioMap = new mutable.HashMap[String, Double]()
    var lastPageCount: Double = startPageCount.toDouble

    for (pageSplit <- targetPageSplit) {
      // 第一次循环: 1_2count
      val currentPageSplitCount: Double = pageSplitCountMap(pageSplit).toDouble
      val ratio: Double = currentPageSplitCount / lastPageCount

      pageSplitRatioMap.put(pageSplit, ratio)

      // 每次循环将当前pageSplit赋值给lastPageCount
      lastPageCount = currentPageSplitCount
    }

    val convertStr: String = pageSplitRatioMap.map {
      case (pageSplit, ratio) =>
        pageSplit + "=" + ratio
    }.mkString(" | ")

    val pageSplit: PageSplitConvertRate = PageSplitConvertRate(taskUUID, convertStr)

    val pageSplitRatioRDD: RDD[PageSplitConvertRate] = spark.sparkContext.makeRDD(Array(pageSplit))

    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_convert_split_ratio_1205")
      .mode(SaveMode.Append)
      .save()

  }


  def getUserVisitAvtion(spark: SparkSession, taskParam: JSONObject): RDD[(String, UserVisitAction)] = {
    import spark.implicits._

    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"

    spark.sql(sql).as[UserVisitAction].rdd.map(item => {
      (item.session_id, item)
    })

  }


}
