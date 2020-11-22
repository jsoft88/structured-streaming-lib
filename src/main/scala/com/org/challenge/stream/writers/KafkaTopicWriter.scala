package com.org.challenge.stream.writers

import java.util.concurrent.TimeUnit

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.transformation.BaseTransform
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

case class KafkaTopicWriter(spark: SparkSession, params: Params) extends {
  private var writeInterval: Long = 0
  private var kafkaBrokers: String = ""
  private var outputTopic: String = ""
} with BaseWriter(spark, params) {
  override def writer(dataframe: Option[DataFrame], transformInstance: BaseTransform): DataStreamWriter[Row] = {
    dataframe match {
      case None => throw new IllegalArgumentException("Cannot obtain writer for dataframe None")
      case Some(df) => {
        df
          .writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", this.kafkaBrokers)
          .option("topic", this.outputTopic)
          .outputMode(OutputMode.Append)
          .trigger(Trigger.ProcessingTime(this.writeInterval, TimeUnit.SECONDS))
      }
    }
  }

  override def setupWriter(): Unit = {
    this.params.outputTopic match {
      case None => throw new IllegalArgumentException("Expected a value for output topic, but None found")
      case Some(ot) => this.outputTopic = ot
    }

    this.params.kafkaBrokers match {
      case None => throw new IllegalArgumentException("Expected value for kafka brokers but None found")
      case Some(kb) => this.kafkaBrokers = kb
    }

    this.writeInterval = writeInterval
  }
}
