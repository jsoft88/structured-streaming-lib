package com.org.challenge.stream.helpers

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.readers.BaseReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.DataFrame

case class FileReader(spark: SparkSession, params: Params, patchedTimestamps: Boolean = false) extends {
  var fileToRead = ""
} with BaseReader(spark, params) {
  val schemaPerFile = Map(
    "pageviews" -> StructType(Seq(
      StructField("userid", StringType, false),
      StructField("viewtime", LongType, false),
      StructField("pageid", StringType, false)
    )),
    "users" -> StructType(Seq(
      StructField("userid", StringType, false),
      StructField("gender", StringType, false),
      StructField("registertime", LongType, false),
      StructField("regionid", StringType, false)
    )),
    "kafka" -> StructType(Seq(
      StructField("topic", StringType, false),
      StructField("value", StringType, false),
      StructField("timestamp", LongType, false)
    ))
  )

  override def read(): Option[DataFrame] = {
    Some(spark.readStream.format("file").schema(this.schemaPerFile.get("kafka").head).json(this.fileToRead))
  }

  override def setupReader(): Unit = {
    val pathInResources = if (!this.patchedTimestamps) s"/kafka/${this.params.topics.get.head}/" else s"/kafka/streaming/${this.params.topics.get.head}/"
    this.fileToRead = s"file://${getClass.getResource(pathInResources).getPath}"
  }
}
