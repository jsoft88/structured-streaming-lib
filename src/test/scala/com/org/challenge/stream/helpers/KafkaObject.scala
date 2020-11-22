package com.org.challenge.stream.helpers

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class KafkaObject(key: String, value: String, topic: String, partition: Int, offset: Long, timestamp: Timestamp)
