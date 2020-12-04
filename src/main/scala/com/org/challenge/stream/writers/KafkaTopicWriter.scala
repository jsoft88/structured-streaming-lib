package com.org.challenge.stream.writers

import java.util.concurrent.TimeUnit

import com.google.common.annotations.VisibleForTesting
import com.org.challenge.stream.config.Params
import com.org.challenge.stream.transformation.BaseTransform
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.types.StringType

case class KafkaTopicWriter(spark: SparkSession, params: Params) extends {
  private var writeInterval: Long = 0
  private var kafkaBrokers: String = ""
  private var outputTopic: String = ""
} with BaseWriter(spark, params) {

  @VisibleForTesting
  private[writers] def getFinalDF(dataframe: DataFrame): DataFrame = {
    val columns = dataframe.schema.fields.map(f => functions.col(f.name))
    dataframe
      .withColumn("key", functions.lit(System.currentTimeMillis()).cast(StringType))
      .withColumn("value", functions.to_json(functions.struct(columns: _*)))
      .select(functions.col("key"),functions.col("value"))
  }

  override def writer(dataframe: Option[DataFrame], transformInstance: BaseTransform): DataStreamWriter[Row] = {
    dataframe match {
      case None => throw new IllegalArgumentException("Cannot obtain writer for dataframe None")
      case Some(df) => {
        df
          .writeStream
          .outputMode(OutputMode.Append)
          .foreachBatch((batchDF: DataFrame, batchId: Long) => {
            transformInstance.transformBatch(Some(batchDF)) match {
              case None => throw new Exception("An exception occurred while transforming batch dataframe")
              case Some(btdf) => {
                btdf.alias("bfdf").show(10, false)
                this.log.info(s"Writer will proceed to apply final batch transformation in microbatch ID ${batchId}")
                this.log.info(s"Ouput topic --> ${this.outputTopic} in broker ${this.kafkaBrokers}")
                val fdf = this.getFinalDF(btdf)
                fdf.alias("to_show").show(10, false)
                fdf
                  .write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", this.kafkaBrokers)
                  .option("topic", this.outputTopic)
                  .save()
              }
            }
          })
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

    this.writeInterval = this.params.writeInterval
  }
}
