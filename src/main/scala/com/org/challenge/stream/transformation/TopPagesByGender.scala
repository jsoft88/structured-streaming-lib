package com.org.challenge.stream.transformation

import com.org.challenge.stream.config.Params
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, col, expr, lit, row_number, sum, window}

object TopPagesByGender {
  val TopicUsers = "users"
  val TopicPageViews = "pageviews"
}

/**
 * Given that transformations are made based on stakeholders requirements, we will have different
 * classes here for providing such functionalities.
 * ------------------------------------------
 * - Joins the messages in these two topics on the user id field
 * - Uses a 1 minute hopping window with 10 second advances to compute the 10 most viewed pages by viewtime for every value of gender
 * - Once per minute produces a message into the top_pages topic that contains the gender, page id, sum of view time in the latest window and distinct count of user ids in the latest window
 * ------------------------------------------
 *
 * So this is a class providing the transformations required in this request. As more requests arrive to the team, we could
 * extend the framework and provide it via Factory.
 *
 * {@note}: Notice that there's no loss of generalization by directly referencing
 * {@code com.org.challenge.stream.jobs.kafka.PageViewsStream.{PageViewsSchema, UsersSchema}, since as mentioned
 * in {@see com.org.challenge.stream.jobs.kafka.PageViewsStream}, I am using the companion object as a schema registry.
 *
 * Given that the transformation is specific to a requirement, it is ok to reference directly, even though the user is prompted
 * to provide topics via CLI params. However, this allows for flexibility in other components, say, readers.
 *
 * This is not different than directly accessing column names in the dataframes.
 * @param spark an active spark session
 * @param params parameters provided to the application
 */
case class TopPagesByGender(spark: SparkSession, params: Params) extends {
  private val DefaultNumberOfTopPages = 10
  private[transformation] var topics: Seq[String] = Seq.empty
  private var eventTimeFieldPerTopic: Map[String, String] = Map.empty
  private var delayPerTopic: Map[String, Long] = Map.empty
  private var windowDuration: Long = 0L
  private var slideInterval: Long = 0L
  private var topPagesLimit = 0
} with BaseTransform(spark, params) {
  import spark.implicits._

  override def setupTransformation(): Unit = {
    this.params.topics match {
      case None => throw new IllegalArgumentException("Required topics to be defined but None found")
      case Some(ts) => this.topics = ts
    }

    this.eventTimeFieldPerTopic = this.params.eventTimeFieldPerTopic.head
    this.params.delayPerTopic match {
      case None => this.delayPerTopic = this.topics.map(t => (t -> 60L)).toMap
      case Some(delay) => this.delayPerTopic = this.topics.map(t => (t -> delay.get(t).getOrElse(60L))).toMap
    }

    this.windowDuration = this.params.windowDuration
    this.slideInterval = this.params.slidingInterval

    this.topPagesLimit = this.params.topPagesNumber.getOrElse(DefaultNumberOfTopPages)
  }

  override def transformStream(dataframes: Option[Map[String, DataFrame]]): Option[DataFrame] = {
    dataframes match {
      case None => throw new IllegalArgumentException("Dataframe to transform is None")
      case Some(dfList) => {
        this.log.info(s"---- List of DFS: ${dfList.keys.map(k => k).mkString(",")}")

        assert(dfList.keys.size == 2)
        assert(dfList.keySet.contains(TopPagesByGender.TopicUsers))
        assert(dfList.keySet.contains(TopPagesByGender.TopicPageViews))

        val usersDF = dfList.get(TopPagesByGender.TopicUsers).head.alias("u")
        val pageviewsDF = dfList.get(TopPagesByGender.TopicPageViews).head.alias("pv")

        val usersDFDelay = this.delayPerTopic.get(TopPagesByGender.TopicUsers).head
        val eventTimePV = this.eventTimeFieldPerTopic.get(TopPagesByGender.TopicPageViews).head
        val eventTimeU = this.eventTimeFieldPerTopic.get(TopPagesByGender.TopicUsers).head

        Some(
          pageviewsDF
            .join(usersDF,
              expr(
                s"""
                  |pv.userid = u.userid AND
                  |pv.${eventTimePV} >= u.${eventTimeU} AND
                  |pv.${eventTimePV} <= u.${eventTimeU} + interval ${usersDFDelay} seconds
                  |""".stripMargin)
            )
            .select(
              col("pv.userId").as("userid"),
              col("u.gender").as("gender"),
              col("pv.viewtime").as("viewtime"),
              col("pv.pageid").as("pageid"),
              col(s"pv.${eventTimePV}").as(eventTimePV)
            )
        )
      }
    }
  }

  override def transformBatch(dataFrame: Option[DataFrame], otherDFs: DataFrame*): Option[DataFrame] = {
    dataFrame match {
      case None => this.log.error(s"Batch transform found an error in ${classOf[TopPagesByGender].getCanonicalName}"); throw new IllegalArgumentException("Expected dataframe to transform in microbatch, but None found")
      case Some(preparedDF) => {
        this.log.info(s"Transforming batch in ${classOf[TopPagesByGender].getCanonicalName}")
        val windowSpecPageId = Window.orderBy(col("total_viewtime").desc)
        val distinctUserIds = preparedDF.agg(approx_count_distinct("userid").as("distinct_user_ids"))

        Some(
          preparedDF
            .groupBy(
              window(
                col("timestamp"),
                s"${this.windowDuration} seconds",
                s"${this.slideInterval} seconds"
              ),
              col("gender"),
              col("pageid")
            )
            .agg(sum(col("viewtime")).as("total_viewtime"))
            .withColumn("page_pos", row_number().over(windowSpecPageId))
            .where(col("page_pos") <= lit(this.topPagesLimit))
            .select(
              col("gender"),
              col("pageid"),
              col("total_viewtime")
            ).crossJoin(distinctUserIds)
        )
      }
    }
  }
}
