package com.org.challenge.stream.writers

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.transformation.BaseTransform
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

abstract class BaseWriter(spark: SparkSession, params: Params) {
  def setupWriter(): Unit

  def writer(dataframe: Option[DataFrame], transformInstance: BaseTransform): DataStreamWriter[Row]
}
