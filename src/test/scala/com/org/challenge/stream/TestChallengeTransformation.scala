package com.org.challenge.stream

import java.io.InputStream
import java.util.concurrent.TimeUnit

import com.org.challenge.stream.config.{CLIParams, ParamsBuilder}
import com.org.challenge.stream.factory.TransformationFactory
import com.org.challenge.stream.jobs.kafka
import com.org.challenge.stream.jobs.kafka.PageViewsStream
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

case class MockKafkaDataFrame(key: Array[Byte], value: Array[Byte])

class TestChallengeTransformation extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().appName("TestTopPages").master("local[4]").getOrCreate()
  import spark.implicits._

  implicit val sqlCtx = spark.sqlContext
  val topics = Seq("pageviews.json", "users")

  var eventsUsers: MemoryStream[String] = _
  var eventsPageViews: MemoryStream[String] = _
  var expectedDataframe: DataFrame = _
  var offsetUsers: Offset = _
  var offsetPageviews: Offset = _
  var expectedSumViewtime = 0L

//  test("Output is correct") {
//    // we will not use the reader factory but rather directly create a reader with MemoryStream
//    val pageViewsSchema = PageViewsStream.SchemaByType.get(kafka.PageViewsStream.PageViewsSchema)
//    val usersSchema = kafka.PageViewsStream.SchemaByType.get(kafka.PageViewsStream.UsersSchema)
//
//    val dataframes = Map(
//      topics(0) -> eventsPageViews.toDF().select(from_json(col("value").cast(StringType), pageViewsSchema.get).as("cols")).select("cols.*").withColumn("viewtime_watermark", col("viewtime").cast(TimestampType)).withWatermark("viewtime_watermark", "1 minutes"),
//      topics(1) -> eventsUsers.toDF().select(from_json(col("value").cast(StringType), usersSchema.get).as("cols")).select("cols.*").withColumn("registertime_watermark", col("registertime").cast(TimestampType)).withWatermark("registertime_watermark", "1 minutes")
//    )
//    val paramsForTransformation = new ParamsBuilder()
//      .withTopPagesNumber(2)
//      .withDelayPerTopic(topics(0), 10) //nothing discarded by engine
//      .withDelayPerTopic(topics(1), 10) //nothing discarded by engine
//      .withWindowDuration(10)
//      .withEventTimeFieldPerTopic(topics(0), "viewtime_watermark")
//      .withEventTimeFieldPerTopic(topics(1), "registertime_watermark")
//      .withSchemaTypePerTopic(topics(0), "pageviews.json")
//      .withSchemaTypePerTopic(topics(1), "users")
//      .withSlidingInterval(10)
//      .build()
//
//    dataframes.get(topics(1)).get.printSchema()
//    TransformationFactory.getTransformation(TransformationFactory.Top10ByGender, spark, paramsForTransformation)
//      .transformStream(Some(dataframes)) match {
//      case None => throw new Exception("Test stream transformation returned None")
//      case Some(finalDF) => {
//        val streamingTest = finalDF
//          .writeStream
//          //.format("memory")
//          //.queryName("testChallenge")
//          .foreachBatch((dataframe: DataFrame, batchId: Long) => {
//            TransformationFactory.getTransformation(TransformationFactory.Top10ByGender, spark, paramsForTransformation)
//              .transformBatch(Some(dataframe)) match {
//              case Some(df) => {
//                df.show(10)
//                assert(
//                  df.select(col("pageid")).distinct().collect.map(_.getAs[String](0)).length == 2
//                )
//                df.write.save()
//              }
//            }
//          })
//          //.outputMode(OutputMode.Append)
//          .trigger(Trigger.ProcessingTime(5L, TimeUnit.SECONDS))
//          .start()
//
//        streamingTest.processAllAvailable()
//        eventsUsers.commit(offsetUsers.asInstanceOf[LongOffset])
//        eventsPageViews.commit(offsetPageviews.asInstanceOf[LongOffset])
//
//        streamingTest.awaitTermination(12000)
////        assert(
////          spark.sql("SELECT SUM(sum_viewtime) FROM testChallenge").collect().map(_.getAs[Long](0)).head == expectedSumViewtime
////        )
//
//      }
//    }
//
//  }

  test("CLI command parsing") {
    val inputArgs = Seq(
      "--kafka-brokers", "localhost:9092",
      "--input-topics", "test1,test2",
      "--topic-watermark-pair", "test1=time1,test2=time2",
      "--topic-delay-pair", "test1=10,test2=5",
      "--output-topic", "output1",
      "--window-duration-seconds", "10",
      "--sliding-window-seconds", "5",
      "--write-interval-seconds", "10",
      "--reader-type", "kafka",
      "--writer-type", "kafka",
      "--transform-type", "top10ByGender",
      "--top-pages", "10",
      "--topic-schematype-pair", "test1=schema1,test2=schema2",
      "--application", AppLibrary.ChallengeApp.toString
    )

    val params = new CLIParams().buildCLIParams(inputArgs)
    assert(params.topPagesNumber.get == 10)
    assert(params.launchApp.get.equals(AppLibrary.ChallengeApp.toString))
  }

  override def beforeAll() {
    eventsUsers = MemoryStream[String]
    eventsPageViews = MemoryStream[String]

    val usersFile: InputStream = getClass.getResourceAsStream("/kafka/users/users.json")
    val pageviewsFile: InputStream = getClass().getResourceAsStream("/kafka/pageviews/pageviews.json")
    val usersLines: Iterator[String] = scala.io.Source.fromInputStream( usersFile ).getLines.filter(_.length > 10).map(_.trim)
    val pageviewsLines: Iterator[String] = scala.io.Source.fromInputStream( pageviewsFile ).getLines.filter(_.length > 10).map(_.trim)
    expectedDataframe = spark.read.json(s"file://${getClass.getResource("/kafka/expected_output.json").getPath}")
    expectedSumViewtime = expectedDataframe.agg(functions.sum(col("sum_viewtime"))).collect().map(_.getAs[Long](0)).head
    //println(usersLines.map(s => s).mkString("\n"))
    offsetUsers = eventsUsers.addData(usersLines)
    offsetPageviews = eventsPageViews.addData(pageviewsLines)
  }

  override def afterAll() {
    this.spark.stop()
  }
}
