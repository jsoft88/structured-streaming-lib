package com.org.challenge.stream.transformation

import com.org.challenge.stream.config.ParamsBuilder
import com.org.challenge.stream.factory.ReaderFactory
import com.org.challenge.stream.helpers.{FileReader, SparkUtils, TestUtils}
import com.org.challenge.stream.jobs.kafka.{InputDataframeScaffolding, KafkaSerialization}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll

class TopPagesByGenderTest extends AnyFunSuite with BeforeAndAfterAll {
  val sparkSession = SparkSession.builder().master("local[*]").appName("testtoppages").getOrCreate()

  test("config is parsed correctly") {
    val appParams = new ParamsBuilder()
      .withReaderType(ReaderFactory.KafkaReader.toString)
      .withTopics(Some(Seq("pageviews", "users")))
      .withDelayPerTopic("users", 10L)
      .withDelayPerTopic("pageviews", 10L)
      .withEventTimeFieldPerTopic("users", "timestamp")
      .withEventTimeFieldPerTopic("pageviews", "timestamp")
      .withSchemaTypePerTopic("users", "users")
      .withSchemaTypePerTopic("pageviews", "pageviews")
      .withKafkaBrokers("localhost:9092")
      .withWindowDuration(10L)
      .withSlidingInterval(5L)
      .withKafaInputSerialization(Some(KafkaSerialization.JsonSerialization.toString))
      .build()

    val transformation = TopPagesByGender(this.sparkSession, appParams)
    val transformationSpy = Mockito.spy[TopPagesByGender](transformation)

    val fileReader = FileReader(this.sparkSession, appParams)

    assert(transformationSpy.topics.length == 2)
    assert(transformationSpy.topics.filter(_.equals("pageviews")).length == 1)
    assert(transformationSpy.topics.filter(_.equals("users")).length == 1)
  }

  test("When no input dataframes are provided to transformation, an exception is thrown") {
    val appParams = new ParamsBuilder()
      .withReaderType(ReaderFactory.KafkaReader.toString)
      .withTopics(Some(Seq("pageviews", "users")))
      .withDelayPerTopic("users", 10L)
      .withDelayPerTopic("pageviews", 10L)
      .withEventTimeFieldPerTopic("users", "timestamp")
      .withEventTimeFieldPerTopic("pageviews", "timestamp")
      .withSchemaTypePerTopic("users", "users")
      .withSchemaTypePerTopic("pageviews", "pageviews")
      .withKafkaBrokers("localhost:9092")
      .withWindowDuration(10L)
      .withSlidingInterval(5L)
      .build()

    val transformation = TopPagesByGender(this.sparkSession, appParams)
    val transformationSpy = Mockito.spy[TopPagesByGender](transformation)

    assertThrows[IllegalArgumentException]{
      transformationSpy.transformStream(None)
    }
  }

  test("Transformation produces the desired dataframe for a micro batch") {
    val appParams = new ParamsBuilder()
      .withReaderType(ReaderFactory.KafkaReader.toString)
      .withTopics(Some(Seq("pageviews", "users")))
      .withDelayPerTopic("users", 20L)
      .withDelayPerTopic("pageviews", 20L)
      .withEventTimeFieldPerTopic("users", "timestamp")
      .withEventTimeFieldPerTopic("pageviews", "timestamp")
      .withSchemaTypePerTopic("users", "users")
      .withSchemaTypePerTopic("pageviews", "pageviews")
      .withKafkaBrokers("localhost:9092")
      .withWindowDuration(10L)
      .withSlidingInterval(10L)
      .withTopPagesNumber(4)
      .withSchemaManager("dummy")//required for input stream scaffolding, not for transformation itself
      .withKafaInputSerialization(Some(KafkaSerialization.JsonSerialization.toString))
      .build()

    // update timestamps for watermarks to be valid
    TestUtils.modifyTimestampInResource(
      getClass.getResource("/kafka/users/users.json").getPath,
      getClass.getResource("/kafka/streaming/users/users.json").getPath
    )

    TestUtils.modifyTimestampInResource(
      getClass.getResource("/kafka/pageviews/pageviews.json").getPath,
      getClass.getResource("/kafka/streaming/pageviews/pageviews.json").getPath
    )

    val inputDFs = InputDataframeScaffolding.generateInputStreamThroughSpies(this.sparkSession, appParams, true)
    val transformation = TopPagesByGender(this.sparkSession, appParams)
    val transformationSpy = Mockito.spy[TopPagesByGender](transformation)

    val transformedDF = transformationSpy.transformStream(inputDFs)

    assert(transformedDF != None)

    val expectedOutput = this.sparkSession
      .read
      .json(s"""file://${getClass.getResource("/kafka/expected_output.json").getPath}""")

    transformedDF.get
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        transformationSpy.transformBatch(Some(batchDF)).get
          .createOrReplaceGlobalTempView("top_pages_gender_final")
      })
      .start()

    this.sparkSession.streams.awaitAnyTermination()
    this.sparkSession.streams.resetTerminated()
    val finalDF = this.sparkSession.sql("SELECT * FROM global_temp.top_pages_gender_final")
    finalDF.show()

    assert(finalDF.count() == expectedOutput.count())
    assert(
      finalDF
        .agg(sum(col("total_viewtime")).as("total_viewtime"))
        .collect().head.getAs[Long]("total_viewtime") ==
        expectedOutput.agg(sum(col("total_viewtime")).as("total_viewtime"))
          .collect().head.getAs[Long]("total_viewtime")
    )
  }

  override def afterAll(): Unit = this.sparkSession.sparkContext.stop(); super.afterAll()
}
