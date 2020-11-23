package com.org.challenge.stream.writers

import com.org.challenge.stream.config.ParamsBuilder
import com.org.challenge.stream.factory.ReaderFactory
import com.org.challenge.stream.helpers.TestUtils
import com.org.challenge.stream.jobs.kafka.InputDataframeScaffolding
import com.org.challenge.stream.transformation.TopPagesByGender
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class KafkaTopicWriterTest extends AnyFunSuite with BeforeAndAfterAll {
  private var sparkSession = SparkSession.builder().master("local[*]").appName("testOuput").getOrCreate()
  test("final dataframe contains only key and value as column") {
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
      .withOutputTopic("output_topic")
      .withSchemaManager("dummy")//required for input stream scaffolding, not for transformation itself
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

    val kafkaTopicWriter = KafkaTopicWriter(this.sparkSession, appParams)
    val kafkaSpy = Mockito.spy[KafkaTopicWriter](kafkaTopicWriter)

    Mockito.doAnswer(new Answer[DataStreamWriter[Row]] {
      override def answer(invocation: InvocationOnMock): DataStreamWriter[Row] = {
        invocation.getArgument[Option[DataFrame]](0)
          .get
          .writeStream
          .trigger(Trigger.Once())
          .outputMode(OutputMode.Append())
          .foreachBatch((batchDf: DataFrame, batchId: Long) => {
            kafkaSpy.getFinalDF(transformationSpy.transformBatch(Some(batchDf)).get)
              .createOrReplaceGlobalTempView("kafka_ready")
          })
      }
    }).when(kafkaSpy).writer(transformedDF, transformation)

    kafkaSpy.writer(transformedDF, transformation).start()

    this.sparkSession.streams.awaitAnyTermination()

    this.sparkSession.sql("SELECT * FROM global_temp.kafka_ready").show(20, false)
    Thread.sleep(10000L)
    assert(this.sparkSession.sql("SELECT * FROM global_temp.kafka_ready").schema.fields.map(_.name).length == 2)
    assert(this.sparkSession.sql("SELECT * FROM global_temp.kafka_ready").count() == 4)
  }

  override protected def beforeAll(): Unit = super.beforeAll()

  override protected def afterAll(): Unit = this.sparkSession.sparkContext.stop(); super.afterAll()
}
