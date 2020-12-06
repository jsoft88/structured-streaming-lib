package com.org.challenge.stream.jobs.kafka

import com.org.challenge.stream.config.{Params, ParamsBuilder}
import com.org.challenge.stream.factory.{ReaderFactory, ReaderType, SchemaManagementFactory}
import com.org.challenge.stream.helpers.{FileReader, InMemoryReader}
import com.org.challenge.stream.schemas.augmenters.SchemaToAvro
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}
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
      .withKafaInputSerialization(Some(KafkaSerialization.JsonSerialization.toString))
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
      .withKafaInputSerialization(Some(KafkaSerialization.JsonSerialization.toString))
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

  test("Reader from factory produces two dataframes hashed by topics, with avro serialization. Event time is part of data") {
    val appParams = new ParamsBuilder()
      .withReaderType(ReaderFactory.KafkaReader.toString)
      .withTopics(Some(Seq("pageviews", "users")))
      .withDelayPerTopic("users", 10L)
      .withDelayPerTopic("pageviews", 10L)
      .withEventTimeFieldPerTopic("users", "registertime")
      .withEventTimeFieldPerTopic("pageviews", "viewtime")
      .withSchemaTypePerTopic("users", "users")
      .withSchemaTypePerTopic("pageviews", "pageviews")
      .withKafkaBrokers("localhost:9092")
      .withSchemaManager("dummyManager")
      .withKafaInputSerialization(Some(KafkaSerialization.AvroSerialization.toString))
      .build()

    val applicationSpy = Mockito.spy[PageViewsStream](new PageViewsStream(sparkSession, appParams))
    val fileReader = FileReader(sparkSession, appParams, true)

    Mockito.doAnswer(new Answer[StructType]() {
      override def answer(invocation: InvocationOnMock): StructType = {
        fileReader.schemaPerFile.get(invocation.getArgument[String](0)).head
      }
    }).when(applicationSpy).getSchemaByType(ArgumentMatchers.anyString())

    Mockito.doReturn(SchemaManagementFactory.getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, None), Nil: _*)
      .when(applicationSpy).getSchemaManagementInstance(ArgumentMatchers.anyString())

    Mockito.doAnswer(new Answer[InMemoryReader]() {
      override def answer(invocation: InvocationOnMock): InMemoryReader = {
        val avroSchemas = invocation
          .getArgument[Params](1)
          .topics
          .get
          .map(t => t -> SchemaToAvro().fromModelToB(t, applicationSpy.getSchemaByType(t)))
          .toMap
        InMemoryReader(sparkSession, invocation.getArgument[Params](1), avroSchemas)
      }
    }).when(applicationSpy).getReaderFromFactory(ArgumentMatchers.any[ReaderType](), ArgumentMatchers.any[Params]())

    applicationSpy.setupJob()
    val dataframes = applicationSpy.setupInputStream()

    assert(dataframes != None)
    assert(dataframes.get.keys.size == 2)

    dataframes.get.get("users").get.show()
    dataframes.get.get("pageviews").get.show()

    dataframes.get.get("users").get.schema.fields
      .foreach(f => assert(fileReader.schemaPerFile.get("users").get.fields.map(_.name).contains(f.name)))

    dataframes.get.get("pageviews").get.schema.fields
      .foreach(f => assert(fileReader.schemaPerFile.get("pageviews").get.fields.map(_.name).contains(f.name)))
  }

  test("Reader from factory produces two dataframes hashed by topics, with avro serialization. Event time is NOT part of data") {
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
      .withKafaInputSerialization(Some(KafkaSerialization.AvroSerialization.toString))
      .build()

    val applicationSpy = Mockito.spy[PageViewsStream](new PageViewsStream(sparkSession, appParams))
    val fileReader = FileReader(sparkSession, appParams, true)

    Mockito.doAnswer(new Answer[StructType]() {
      override def answer(invocation: InvocationOnMock): StructType = {
        fileReader.schemaPerFile.get(invocation.getArgument[String](0)).head
      }
    }).when(applicationSpy).getSchemaByType(ArgumentMatchers.anyString())

    Mockito.doReturn(SchemaManagementFactory.getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, None), Nil: _*)
      .when(applicationSpy).getSchemaManagementInstance(ArgumentMatchers.anyString())

    Mockito.doAnswer(new Answer[InMemoryReader]() {
      override def answer(invocation: InvocationOnMock): InMemoryReader = {
        val avroSchemas = invocation
          .getArgument[Params](1)
          .topics
          .get
          .map(t => t -> SchemaToAvro().fromModelToB(t, applicationSpy.getSchemaByType(t)))
          .toMap
        InMemoryReader(sparkSession, invocation.getArgument[Params](1), avroSchemas)
      }
    }).when(applicationSpy).getReaderFromFactory(ArgumentMatchers.any[ReaderType](), ArgumentMatchers.any[Params]())

    applicationSpy.setupJob()
    val dataframes = applicationSpy.setupInputStream()

    assert(dataframes != None)
    assert(dataframes.get.keys.size == 2)

    dataframes.get.get("users").get.show()
    dataframes.get.get("pageviews").get.show()

    // timestamp column should also be part of the produced dataframes because event time is not part of the data
    dataframes.get.get("users").get.schema.fields
      .foreach(f => assert((fileReader.schemaPerFile.get("users").get.fields.map(_.name) :+ "timestamp").contains(f.name)))

    dataframes.get.get("pageviews").get.schema.fields
      .foreach(f => assert((fileReader.schemaPerFile.get("pageviews").get.fields.map(_.name) :+ "timestamp").contains(f.name)))
  }

  override protected def beforeAll(): Unit = {
    this.sparkSession = SparkSession.builder().master("local[*]").appName("testpageviewsstream").getOrCreate()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = this.sparkSession.stop(); super.afterAll()
}
