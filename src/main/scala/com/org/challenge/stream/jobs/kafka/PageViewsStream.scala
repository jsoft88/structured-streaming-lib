package com.org.challenge.stream.jobs.kafka

import com.google.common.annotations.VisibleForTesting
import com.org.challenge.stream.factory.{ReaderFactory, ReaderType, SchemaManagementFactory, TransformationFactory, WriterFactory}
import com.org.challenge.stream.config.{Params, ParamsBuilder}
import com.org.challenge.stream.core.StreamJob
import com.org.challenge.stream.readers.BaseReader
import com.org.challenge.stream.schemas.SchemaManagement
import com.org.challenge.stream.utils.Utils
import org.apache.spark.sql.functions.{col, from_json, from_unixtime}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait SchemaType {
  def getDefaultWatermarkColName(): String
}

/**
 * Consider this companion object to be a sort of Schema registry.
 * even though the whole idea is having a flexible framework open for extensibility,
 * e.g. take topics names as CLI parameters instead of hardcoding it, it is still
 * valid to map {@code SchemaType} child classes {@code toString} method to the different
 * topics the organisation has, because this is our schema registry.
 */
object PageViewsStream {
  val DefaultDelaySeconds: Long = 0L
  val DefaultWindowDuration: Long = 0L
  val DefaultSlidingWindowInterval: Long = 0L
  val DefaultWatermarkField = "timestamp"
}

class PageViewsStream(spark: SparkSession, params: Params) extends StreamJob[Params](spark, params) {
  var usersDelaySeconds: Long = 0L
  var pageViewsDelaySeconds: Long = 0L
  var topicsDelayPair: Map[String, Long] = Map.empty
  var writeInterval: Long = 0L
  var kafkaBrokers: String = _
  var topicEventField: Map[String, String] = Map.empty
  var schemaByTopic: Map[String, String] = Map.empty
  var topics: Seq[String] = Seq.empty
  var schemaManagement: SchemaManagement = _
  val utils: Utils = new Utils()


  @VisibleForTesting
  private[kafka] def getSchemaByType(topic: String): StructType = {
    this.schemaByTopic.get(topic) match {
      case None => throw new Exception(s"No schema identifier is associated to topic '${topic}'")
      case Some(schemaIdentifier) => this.utils.getSchemaWith(schemaIdentifier, this.schemaManagement)
    }
  }

  @VisibleForTesting
  private[kafka] def getSchemaManagementInstance(schemaManagementType: String): SchemaManagement = {
    SchemaManagementFactory.getSchemaManagementInstance(schemaManagementType)
  }

  @VisibleForTesting
  override protected[kafka] def setupJob(): Unit = {
    params.delayPerTopic match {
      case None => throw new IllegalArgumentException("Streaming from kafka requires topics to be present, but None found")
      case Some(topics) => this.topicsDelayPair = topics
    }

    this.writeInterval = params.writeInterval
    params.kafkaBrokers match {
      case None => throw new IllegalArgumentException("Streaming from kafka requires bootstrap servers, but None found")
      case Some(kb) => this.kafkaBrokers = kb
    }

    params.topics match {
      case None => throw new IllegalArgumentException("Expected topics to be present, but None found")
      case Some(t) => this.topics = t
    }

    params.eventTimeFieldPerTopic match {
      case None => {
        // When None, even when the stream does not have such column, we will assume the event time is kafka timestamp.
        this.topicEventField = this.topics.map(t => t -> PageViewsStream.DefaultWatermarkField).toMap

      }
      case Some(evt) => this.topicEventField = this.
        topics
        .foldRight(Map.empty[String, String])((ct, mp) => mp ++ Map(ct -> evt.getOrElse(ct, PageViewsStream.DefaultWatermarkField)))
    }

    params.schemaTypeByTopic match {
      case None => throw new IllegalArgumentException("Expected schema types of topics to be defined, but None found")
      case Some(stbt) => this.schemaByTopic = stbt
    }


    params.schemaManager match {
      case None => throw new IllegalArgumentException("Expected schema manager type to be present but None provided")
      case Some(sm) => this.schemaManagement = this.getSchemaManagementInstance(sm)
    }
  }

  @VisibleForTesting
  private[kafka] def getReaderFromFactory(readerType: ReaderType, kafkaParams: Params): BaseReader = {
    ReaderFactory
      .getReader(readerType, this.spark, kafkaParams)
  }

  @VisibleForTesting
  override protected[kafka] def setupInputStream(): Option[Map[String, DataFrame]] = {
    // Iterate the different topics we obtained via CLI to load each dataframe.
    Some(this.topics.map(t => {
      val schemaForTopic = this.getSchemaByType(t)
      val watermarkCol = schemaForTopic.fields.filter(_.name.equals(this.topicEventField.get(t).head)) headOption match {
        case None => PageViewsStream.DefaultWatermarkField
        case Some(f) => f.name
      }

      val kafkaParams = new ParamsBuilder().withKafkaBrokers(this.kafkaBrokers).withTopics(Some(Seq(t))).build()

      val topicDF = this.getReaderFromFactory(ReaderFactory.KafkaReader, kafkaParams).read() match {
        case None => this.log.error(s"An error occurred while reading topic: ${t}"); throw new Exception(s"Failed to read from topic ${t}")
        case Some(df) if watermarkCol.equals(PageViewsStream.DefaultWatermarkField)=> {
          df
            .select(from_json(col("value").cast(StringType), schemaForTopic).as("data"), col(watermarkCol))
            .select(col("data.*"), from_unixtime(col(watermarkCol)).cast(TimestampType).as(watermarkCol))
            .withWatermark(watermarkCol, s"${this.topicsDelayPair.get(t).get} seconds")
        }
        case Some(df) => {
          df
            .select(from_json(col("value").cast(StringType), schemaForTopic).as("data"))
            .select(
              schemaForTopic
                .filterNot(_.name.equals(watermarkCol))
                .map(f => col(s"data.${f.name}"))
                .foldRight(Seq(from_unixtime(col(s"data.${watermarkCol}")).cast(TimestampType).as(watermarkCol)))((cc, cs) => cs :+ cc): _*)
            .withWatermark(watermarkCol, s"${this.topicsDelayPair.get(t).get} seconds")
        }
      }
      t -> topicDF
    }) toMap)
  }

  @VisibleForTesting
  override protected[kafka] def transform(dataframes: Option[Map[String, DataFrame]]): DataFrame = {
    TransformationFactory.getTransformation(TransformationFactory.Top10ByGender, this.spark, params)
      .transformStream(dataframes) match {
      case None => throw new Exception("Error occurred while transforming stream, expected dataframe but None was found.")
      case Some(df) => df
    }
  }

  @VisibleForTesting
  override protected[kafka] def writeStream(dataFrame: Option[DataFrame]): Unit = {
    WriterFactory.getWriter(WriterFactory.KafkaWriter, this.spark, params).writer(
      dataFrame,
      TransformationFactory.getTransformation(TransformationFactory.NoOp, spark, params)
    ).start()
  }

  @VisibleForTesting
  override protected[kafka] def finalizeJob(): Unit = {
    this.spark.streams.awaitAnyTermination()
    this.spark.stop()
  }
}
