package com.org.challenge.stream.jobs.kafka

import com.org.challenge.stream.config.{Params, ParamsBuilder}
import com.org.challenge.stream.factory.{ReaderFactory, ReaderType, SchemaManagementFactory, TransformationType, WriterFactory, WriterType}
import com.org.challenge.stream.helpers.{ConsoleWriter, FileReader, TestUtils}
import com.org.challenge.stream.writers.BaseWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PageViewsStreamE2ETest extends AnyFunSuite with BeforeAndAfterAll {
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    this.sparkSession = SparkSession.builder().master("local[*]").appName("testOuput").getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = this.sparkSession.stop(); super.afterAll()

  test("When all parameters are provided, complete and correct execution is achieved") {
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

    val pageViewsStreamApp = new PageViewsStream(this.sparkSession, appParams)
    val pageViewsStreamSpy = Mockito.spy[PageViewsStream](pageViewsStreamApp)

    val fileReader = FileReader(sparkSession, appParams, true)

    Mockito.doAnswer(new Answer[StructType]() {
      override def answer(invocation: InvocationOnMock): StructType = {
        fileReader.schemaPerFile.get(invocation.getArgument[String](0)).head
      }
    }).when(pageViewsStreamSpy).getSchemaByType(ArgumentMatchers.anyString())

    Mockito.doAnswer(new Answer[FileReader]() {
      override def answer(invocation: InvocationOnMock): FileReader = {
        val readerType = invocation.getArgument[ReaderType](0)
        val readerParams = invocation.getArgument[Params](1)

        assert(readerType == ReaderFactory.KafkaReader)
        assert(readerParams.kafkaBrokers != None)
        assert(readerParams.topics != None)
        FileReader(sparkSession, invocation.getArgument[Params](1), true)
      }
    }).when(pageViewsStreamSpy).getReaderFromFactory(ArgumentMatchers.any[ReaderType](), ArgumentMatchers.any[Params]())

    Mockito.doReturn(SchemaManagementFactory.getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, None), Nil: _*)
      .when(pageViewsStreamSpy).getSchemaManagementInstance(ArgumentMatchers.anyString())

    Mockito.doCallRealMethod()
      .when(pageViewsStreamSpy).getTransformerFromFactory(ArgumentMatchers.any[TransformationType](), ArgumentMatchers.eq[Params](appParams))

    Mockito.doAnswer(new Answer[BaseWriter] {
      override def answer(invocation: InvocationOnMock): BaseWriter = {
        val writerType = invocation.getArgument[WriterType](0)
        val writerParams = invocation.getArgument[Params](1)

        assert(writerType == WriterFactory.KafkaWriter)
        assert(writerParams.outputTopic != None)
        assert(writerParams.kafkaBrokers != None)

        new ConsoleWriter(sparkSession, appParams)
      }
    }).when(pageViewsStreamSpy).getWriterFromFactory(ArgumentMatchers.any[WriterType](), ArgumentMatchers.eq[Params](appParams))

    pageViewsStreamSpy.runStreamJob()

    Mockito.verify[PageViewsStream](pageViewsStreamSpy, Mockito.times(1)).setupJob()
    Mockito.verify[PageViewsStream](pageViewsStreamSpy, Mockito.times(2))
      .getTransformerFromFactory(ArgumentMatchers.any[TransformationType](), ArgumentMatchers.any[Params]())
    Mockito.verify[PageViewsStream](pageViewsStreamSpy, Mockito.times(1)).setupInputStream()
    Mockito.verify[PageViewsStream](pageViewsStreamSpy, Mockito.times(1))
      .transform(ArgumentMatchers.any[Option[Map[String,DataFrame]]]())
    Mockito.verify[PageViewsStream](pageViewsStreamSpy, Mockito.times(1))
      .writeStream(ArgumentMatchers.any[Option[DataFrame]]())
    Mockito.verify[PageViewsStream](pageViewsStreamSpy, Mockito.times(1))
      .invokeWait()

    assert(this.sparkSession.sql("SELECT * FROM global_temp.e2e_top_pages").schema.fields.map(_.name).length == 4)
  }
}
