package com.org.challenge.stream.readers

import com.org.challenge.stream.config.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.org.challenge.stream.utils.Logger

case class KafkaTopicReader(spark: SparkSession, params: Params) extends BaseReader(spark, params) with Logger {
  override def read(): Option[DataFrame] = {
    try {
      Some(spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", params.kafkaBrokers.head)
        .option("subscribe", params.topics.get.head)
        .load())
    } catch {
      case ex: Exception => this.log.error(s"Kafka reader: ${ex.getMessage()}"); None
    }
  }
}
