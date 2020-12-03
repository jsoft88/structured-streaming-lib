package com.org.challenge.stream.jobs.kafka

import com.org.challenge.stream.config.{ParamsBuilder}
import com.org.challenge.stream.factory.{ReaderFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PageViewsStreamTest extends AnyFunSuite with BeforeAndAfterAll {
  var sparkSession:SparkSession = _

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

    readDataframes
      .get
      .get("pageviews")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => batchDF.createOrReplaceGlobalTempView("pageviews_assert"))
      .start()

    readDataframes
      .get
      .get("users")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => batchDF.createOrReplaceGlobalTempView("users_assert"))
      .start()

    this.sparkSession.streams.awaitAnyTermination()

    this.sparkSession.sql("SELECT * FROM global_temp.pageviews_assert").show(20, false)
    this.sparkSession.sql("SELECT * FROM global_temp.users_assert").show(20, false)
    assert(this.sparkSession.sql("SELECT * FROM global_temp.pageviews_assert").count() == 18)
    assert(this.sparkSession.sql("SELECT * FROM global_temp.users_assert").count() == 5)

    this.sparkSession.streams.resetTerminated()
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

    readDataframes
      .get
      .get("pageviews")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) =>{
        batchDF.createOrReplaceGlobalTempView("pageviews_assert2")
      })
      .start()

    this.sparkSession.streams.awaitAnyTermination()
    assert(this.sparkSession.sql("SELECT * FROM global_temp.pageviews_assert2").schema.fields.filter(_.name.equals("timestamp")).isEmpty)
    this.sparkSession.streams.resetTerminated()
  }

  override protected def beforeAll(): Unit = {
    this.sparkSession = SparkSession.builder().master("local[*]").appName("testpageviewsstream").getOrCreate()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = this.sparkSession.stop(); super.afterAll()
}
