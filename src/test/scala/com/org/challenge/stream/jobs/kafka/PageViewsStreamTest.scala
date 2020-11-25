package com.org.challenge.stream.jobs.kafka

import com.org.challenge.stream.config.{ParamsBuilder}
import com.org.challenge.stream.factory.{ReaderFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PageViewsStreamTest extends AnyFunSuite with BeforeAndAfterAll {
  var sparkSession = SparkSession.builder().master("local[*]").appName("testpageviewsstream").getOrCreate()

  test("Reader from factory produces two dataframes hashed by topics") {
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
      .withSchemaManager("dummyManager")
      .build()
    val readDataframes = InputDataframeScaffolding.generateInputStreamThroughSpies(this.sparkSession, appParams)
    assert(readDataframes != None)

    assert(readDataframes.get.keys.size == 2)

    var pageViewsForAsserts = this.sparkSession.emptyDataFrame
    readDataframes
      .get
      .get("pageviews")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => pageViewsForAsserts = batchDF.as("df1"))
      .start()

    var usersForAsserts = this.sparkSession.emptyDataFrame
    readDataframes
      .get
      .get("users")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => usersForAsserts = batchDF.as("df2"))
      .start()

    this.sparkSession.streams.awaitAnyTermination()

    pageViewsForAsserts.show(20, false)
    usersForAsserts.show(20, false)
    assert(pageViewsForAsserts.count() == 18)
    assert(usersForAsserts.count() == 5)
  }

  test("when watermark column is part of data, timestamp is excluded") {
    val appParams = new ParamsBuilder()
      .withReaderType(ReaderFactory.KafkaReader.toString)
      .withTopics(Some(Seq("pageviews", "users")))
      .withDelayPerTopic("users", 10L)
      .withDelayPerTopic("pageviews", 10L)
      .withEventTimeFieldPerTopic("users", "timestamp")
      .withEventTimeFieldPerTopic("pageviews", "viewtime")
      .withSchemaTypePerTopic("users", "users")
      .withSchemaTypePerTopic("pageviews", "pageviews")
      .withKafkaBrokers("localhost:9092")
      .withSchemaManager("dummyManager")
      .build()

    val readDataframes = InputDataframeScaffolding.generateInputStreamThroughSpies(this.sparkSession, appParams)
    assert(readDataframes != None)

    assert(readDataframes.get.keys.size == 2)

    var pageViewsForAsserts = this.sparkSession.emptyDataFrame
    readDataframes
      .get
      .get("pageviews")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => pageViewsForAsserts = batchDF.as("df1"))
      .start()

    var usersForAsserts = this.sparkSession.emptyDataFrame
    readDataframes
      .get
      .get("users")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => usersForAsserts = batchDF.as("df2"))
      .start()

    this.sparkSession.streams.awaitAnyTermination()
    assert(pageViewsForAsserts.schema.fields.filter(_.name.equals("timestamp")).isEmpty)
  }

  override protected def beforeAll(): Unit = super.beforeAll()

  override protected def afterAll(): Unit = this.sparkSession.sparkContext.stop(); super.afterAll()
}
