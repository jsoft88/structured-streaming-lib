package com.org.challenge.stream.transformation

import com.org.challenge.stream.config.Params
import org.apache.spark.sql.{DataFrame, SparkSession}

case class NoOpTransform(spark: SparkSession, params: Params) extends BaseTransform(spark, params) {
  override def transformStream(dataframes: Option[Map[String, DataFrame]]): Option[DataFrame] = None

  override def transformBatch(dataFrame: Option[DataFrame], otherDFs: DataFrame*): Option[DataFrame] = None
}
