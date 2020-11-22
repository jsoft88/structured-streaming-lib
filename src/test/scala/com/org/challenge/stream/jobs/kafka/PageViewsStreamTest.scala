package com.org.challenge.stream.jobs.kafka

import com.org.challenge.stream.config.{Params, ParamsBuilder}
import com.org.challenge.stream.factory.{ReaderFactory, ReaderType, SchemaManagementFactory}
import com.org.challenge.stream.helpers.{FileReader, SparkUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.funsuite.AnyFunSuite

class PageViewsStreamTest extends AnyFunSuite {

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

//    val fileReader = FileReader(SparkUtils.getGlobalTestSparkSession(), appParams)
//    val applicationSpy = Mockito.spy[PageViewsStream](new PageViewsStream(appParams))
//
//    Mockito.doAnswer(new Answer[StructType]() {
//      override def answer(invocation: InvocationOnMock): StructType = {
//        fileReader.schemaPerFile.get(invocation.getArgument[String](0)).head
//      }
//    }).when(applicationSpy).getSchemaByType(ArgumentMatchers.anyString())
//
//    Mockito.doAnswer(new Answer[FileReader]() {
//      override def answer(invocation: InvocationOnMock): FileReader = {
//        FileReader(SparkUtils.getGlobalTestSparkSession(), invocation.getArgument[Params](1))
//      }
//    }).when(applicationSpy).getReaderFromFactory(ArgumentMatchers.any[ReaderType](), ArgumentMatchers.any[Params]())
//
//    Mockito.doReturn(SchemaManagementFactory.getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, None), Nil: _*)
//      .when(applicationSpy).getSchemaManagementInstance(ArgumentMatchers.anyString())
//
//    applicationSpy.setupJob()
    val readDataframes = InputDataframeScaffolding.generateInputStreamThroughSpies(appParams)
    assert(readDataframes != None)

    assert(readDataframes.get.keys.size == 2)

    var pageViewsForAsserts = SparkUtils.getGlobalTestSparkSession().emptyDataFrame
    readDataframes
      .get
      .get("pageviews")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => pageViewsForAsserts = batchDF.as("df1"))
      .start()

    var usersForAsserts = SparkUtils.getGlobalTestSparkSession().emptyDataFrame
    readDataframes
      .get
      .get("users")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => usersForAsserts = batchDF.as("df2"))
      .start()

    SparkUtils.getGlobalTestSparkSession().streams.awaitAnyTermination()

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

//    val fileReader = FileReader(SparkUtils.getGlobalTestSparkSession(), appParams)
//    val applicationSpy = Mockito.spy[PageViewsStream](new PageViewsStream(appParams))
//
//    Mockito.doAnswer(new Answer[StructType]() {
//      override def answer(invocation: InvocationOnMock): StructType = {
//        fileReader.schemaPerFile.get(invocation.getArgument[String](0)).head
//      }
//    }).when(applicationSpy).getSchemaByType(ArgumentMatchers.anyString())
//
//    Mockito.doAnswer(new Answer[FileReader]() {
//      override def answer(invocation: InvocationOnMock): FileReader = {
//        FileReader(SparkUtils.getGlobalTestSparkSession(), invocation.getArgument[Params](1))
//      }
//    }).when(applicationSpy).getReaderFromFactory(ArgumentMatchers.any[ReaderType](), ArgumentMatchers.any[Params]())
//
//    Mockito.doReturn(SchemaManagementFactory.getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, None), Nil: _*)
//      .when(applicationSpy).getSchemaManagementInstance(ArgumentMatchers.anyString())
//
//    applicationSpy.setupJob()
    val readDataframes = InputDataframeScaffolding.generateInputStreamThroughSpies(appParams)
    assert(readDataframes != None)

    assert(readDataframes.get.keys.size == 2)

    var pageViewsForAsserts = SparkUtils.getGlobalTestSparkSession().emptyDataFrame
    readDataframes
      .get
      .get("pageviews")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => pageViewsForAsserts = batchDF.as("df1"))
      .start()

    var usersForAsserts = SparkUtils.getGlobalTestSparkSession().emptyDataFrame
    readDataframes
      .get
      .get("users")
      .head
      .writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .foreachBatch((batchDF: DataFrame, batchId: Long) => usersForAsserts = batchDF.as("df2"))
      .start()

    SparkUtils.getGlobalTestSparkSession().streams.awaitAnyTermination()
    assert(pageViewsForAsserts.schema.fields.filter(_.name.equals("timestamp")).isEmpty)
  }
}
