package com.org.challenge.stream.factory

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.writers.{BaseWriter, KafkaTopicWriter}
import org.apache.spark.sql.{DataFrameWriter, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

sealed trait WriterType

object WriterFactory {
  case object KafkaWriter extends WriterType {
    override def toString: String = "kafka"
  }

  val AllWriterTypes = Seq(
    KafkaWriter
  )

  def getWriter(writerType: WriterType, spark: SparkSession, params: Params): BaseWriter = {
    writerType match {
      case KafkaWriter => KafkaTopicWriter(spark, params)
    }
  }

  def getWriterType(writerType: String): WriterType = {
    AllWriterTypes.filter(_.toString.toLowerCase.equals(writerType)) headOption match {
      case None => throw new IllegalArgumentException(s"Invalid reader type: ${writerType} passed.")
      case Some(t) => t
    }
  }
}
