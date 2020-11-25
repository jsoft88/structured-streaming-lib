package com.org.challenge.stream.jobs.kafka

import com.org.challenge.stream.config.{Params, ParamsBuilder}
import com.org.challenge.stream.factory.{ReaderFactory, ReaderType, SchemaManagementFactory}
import com.org.challenge.stream.helpers.{FileReader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

object InputDataframeScaffolding {
  def generateInputStreamThroughSpies(sparkSession: SparkSession, appParams: Params, patchedTimestamp: Boolean = false): Option[Map[String, DataFrame]] = {

    val fileReader = FileReader(sparkSession, appParams, patchedTimestamp)
    val applicationSpy = Mockito.spy[PageViewsStream](new PageViewsStream(sparkSession, appParams))

    Mockito.doAnswer(new Answer[StructType]() {
      override def answer(invocation: InvocationOnMock): StructType = {
        fileReader.schemaPerFile.get(invocation.getArgument[String](0)).head
      }
    }).when(applicationSpy).getSchemaByType(ArgumentMatchers.anyString())

    Mockito.doAnswer(new Answer[FileReader]() {
      override def answer(invocation: InvocationOnMock): FileReader = {
        FileReader(sparkSession, invocation.getArgument[Params](1), patchedTimestamp)
      }
    }).when(applicationSpy).getReaderFromFactory(ArgumentMatchers.any[ReaderType](), ArgumentMatchers.any[Params]())

    Mockito.doReturn(SchemaManagementFactory.getSchemaManagementInstance(SchemaManagementFactory.SchemaFromFileManagement, None), Nil: _*)
      .when(applicationSpy).getSchemaManagementInstance(ArgumentMatchers.anyString())

    applicationSpy.setupJob()
    applicationSpy.setupInputStream()
  }
}
