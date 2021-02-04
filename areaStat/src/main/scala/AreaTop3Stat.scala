import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaTop3Stat {

  def main(args: Array[String]): Unit = {

    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    val taskUUID: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("areaTop3")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // RDD[(cityId, Pid)]
    val cityId2PidRDD: RDD[(Long, Long)] = getCityAndProductInfo(spark, taskParam)
    //    cityId2PidRDD.foreach(println(_))

    // RDD[(cityId, CityAreaInfo)]
    val cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)] = getCityAreaInfo(spark)

    // tmp_area_basic_info 表中的一条数据就代表一次点击商品的行为
    getAreaPidBasicInfoTable(spark, cityId2PidRDD, cityId2AreaInfoRDD)
    // 只会显示前20条数据
    spark.sql("select * from tmp_area_basic_info").show()


    spark.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => {
      v1 + split + v2
    })

    spark.udf.register("group_concat_distinct", new GroupConcatDistinct)

    getAreaProductClickCountTable(spark)

    spark.udf.register("get_json_field", (json: String, field: String) => {
      val jSONObject: JSONObject = JSONObject.fromObject(json)
      jSONObject.getString(field)
    })

    getAreaProductClickCountInfo(spark)

    getTop3Product(spark, taskUUID)

  }

  def getTop3Product(spark: SparkSession, taskUUID: String) = {
    import spark.implicits._
    /*
    val sql: String = "select area, city_infos, pid, product_name, product_status, click_count, " +
      " row_number() over(PARTITION BY area ORDER BY click_count DESC) rk" +
      " from tmp_area_count_product_info"

    spark.sql(sql).createOrReplaceTempView("tmp_test")
    */
    val sql: String = "select area, " +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A_Level' " +
      "WHEN area='华中' OR area='华南' THEN 'B_Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C_Level' " +
      "ELSE 'D_Level' " +
      "END area_level, " +
      "city_infos, pid, product_name, product_status, click_count " +
      "from (" + "select area, city_infos, pid, product_name, product_status, click_count, " +
      " row_number() over(PARTITION BY area ORDER BY click_count DESC) rk" +
      " from tmp_area_count_product_info" + ") t where rk<=3"

    val top3ProductRDD: RDD[AreaTop3Product] = spark.sql(sql).rdd.map(row => {
      AreaTop3Product(taskUUID, row.getAs[String]("area"),
        row.getAs[String]("area_level"),
        row.getAs[Long]("pid"),
        row.getAs[String]("city_infos"),
        row.getAs[Long]("click_count"),
        row.getAs[String]("product_name"),
        row.getAs[String]("product_status"))
    })
    top3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product_1205")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def getAreaProductClickCountInfo(spark: SparkSession): Unit = {
    import spark.implicits._

    //tmp_area_click_count: area, city_infos, pid, click_count tacc
    // product_info: product_id, product_name, extend_info pi
    val sql: String = "select tacc.area, tacc.city_infos, tacc.pid, pi.product_name, " +
      " if(get_json_field(pi.extend_info, 'product_status')='0', 'Self', 'Third Party') as product_status, " +
      " tacc.click_count" +
      " from tmp_area_click_count tacc join product_info as pi on tacc.pid=pi.product_id"

    spark.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")

  }


  def getAreaProductClickCountTable(spark: SparkSession): Unit = {
    import spark.implicits._

    val sql: String = "select area, pid, count(*) as click_count," +
      " group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos" +
      " from tmp_area_basic_info group by area, pid"
    spark.sql(sql).createOrReplaceTempView("tmp_area_click_count")

  }


  def getAreaPidBasicInfoTable(spark: SparkSession,
                               cityId2PidRDD: RDD[(Long, Long)],
                               cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]): Unit = {
    import spark.implicits._

    val areaPidInfoRDD: RDD[(Long, String, String, Long)] = cityId2PidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, cityAreaInfo)) =>
        (cityId, cityAreaInfo.city_name, cityAreaInfo.area, pid)
    }

    areaPidInfoRDD.toDF("city_id", "city_name", "area", "pid").createOrReplaceTempView("tmp_area_basic_info")

  }


  def getCityAreaInfo(spark: SparkSession): RDD[(Long, CityAreaInfo)] = {
    import spark.implicits._

    val cityAreaInfoArray: Array[(Long, String, String)] = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    // 返回 RDD[(cityId, CityAreaInfo)]
    spark.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) =>
        (cityId, CityAreaInfo(cityId, cityName, area))
    }

  }

  def getCityAndProductInfo(spark: SparkSession, taskParam: JSONObject): RDD[(Long, Long)] = {

    import spark.implicits._

    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    // 只获取发生过点击的Action数据
    // 获取到的一条Action数据就代表一个点击行为
    val sql: String = "select city_id, click_product_id from user_visit_action where date>='" + startDate + "' and date <='" +
      endDate + "' and click_product_id != -1"

    spark.sql(sql).as[CityClickProduct].rdd.map(cityProdId => {
      (cityProdId.city_id, cityProdId.click_product_id)
    })

  }

}
