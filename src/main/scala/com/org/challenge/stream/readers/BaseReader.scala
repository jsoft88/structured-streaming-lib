package com.org.challenge.stream.readers

import com.org.challenge.stream.config.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.DataFrame

abstract class BaseReader(spark: SparkSession, params: Params) {

  this.setupReader()

  def setupReader(): Unit = {}

  def read(): Option[DataFrame]
}
