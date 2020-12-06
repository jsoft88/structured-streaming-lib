package com.org.challenge.stream.config

import com.org.challenge.stream.factory.{ReaderFactory, SchemaManagementFactory, TransformationFactory, WriterFactory}
import com.org.challenge.stream.AppLibrary
import com.org.challenge.stream.factory.ReaderFactory.KafkaReader
import com.org.challenge.stream.factory.TransformationFactory.Top10ByGender
import com.org.challenge.stream.factory.WriterFactory.KafkaWriter
import com.org.challenge.stream.jobs.kafka.KafkaSerialization

class ParamsBuilder {
  var kafkaBrokers: Option[String] = None
  var delayPerTopic: Option[Map[String, Long]] = None
  var windowDuration: Long = 0L
  var slidingInterval: Long = 0L
  var writeInterval: Long = 0L
  var readerType: String = _
  var writerType: String = _
  var outputTopic: Option[String] = None
  var transformationType: String = _
  var eventTimeFieldPerTopic: Option[Map[String, String]] = None
  var schemaTypeByTopic: Option[Map[String, String]] = None
  var topics: Option[Seq[String]] = None
  var topPagesNumber: Option[Int] = None
  var launchApp: Option[String] = None
  var schemaManager: Option[String] = None
  var transformationConfigPath: Option[String] = None
  var kafkaInputSerialization: Option[String] = None
  var kafkaWriterSerialization: Option[String] = None

  def withKafaInputSerialization(kafkaSerialization: Option[String]): ParamsBuilder = {
    this.kafkaInputSerialization = kafkaSerialization
    this
  }

  def withKafaWriterSerialization(kafkaSerialization: Option[String]): ParamsBuilder = {
    this.kafkaWriterSerialization = kafkaSerialization
    this
  }

  def withTransformationConfigPath(transformationConfigPath: String): ParamsBuilder = {
    this.transformationConfigPath = Some(transformationConfigPath)
    this
  }

  def withSchemaManager(schemaManagerType: String): ParamsBuilder = {
    this.schemaManager = Some(schemaManagerType)
    this
  }

  def withKafkaBrokers(kafkaBrokers: String): ParamsBuilder = {
    this.kafkaBrokers = Some(kafkaBrokers)
    this
  }

  def withDelayPerTopic(topic: String, delay: Long): ParamsBuilder = {
    this.delayPerTopic match {
      case None => this.delayPerTopic = Some(Map(topic -> delay))
      case Some(m) => this.delayPerTopic = Some(m ++ Map(topic -> delay))
    }
    this
  }

  def withWindowDuration(duration: Long): ParamsBuilder = {
    this.windowDuration = duration
    this
  }

  def withSlidingInterval(slidingInterval: Long): ParamsBuilder = {
    this.slidingInterval = slidingInterval
    this
  }

  def withWriteInterval(writeInterval: Long): ParamsBuilder = {
    this.writeInterval = writeInterval
    this
  }

  def withReaderType(readerType: String): ParamsBuilder = {
    this.readerType = readerType
    this
  }

  def withWriterType(writerType: String): ParamsBuilder = {
    this.writerType = writerType
    this
  }

  def withOutputTopic(outputTopic: String): ParamsBuilder = {
    this.outputTopic = Some(outputTopic)
    this
  }

  def withTransformationType(transformationType: String): ParamsBuilder = {
    this.transformationType = transformationType
    this
  }

  def withEventTimeFieldPerTopic(topic: String, eventTimeField: String): ParamsBuilder = {
    this.eventTimeFieldPerTopic match {
      case None => this.eventTimeFieldPerTopic = Some(Map(topic -> eventTimeField))
      case Some(m) => this.eventTimeFieldPerTopic = Some(m ++ Map(topic -> eventTimeField))
    }
    this
  }

  def withSchemaTypePerTopic(topic: String, schemaType: String): ParamsBuilder = {
    this.schemaTypeByTopic match {
      case None => this.schemaTypeByTopic = Some(Map(topic -> schemaType))
      case Some(m) => this.schemaTypeByTopic = Some(m ++ Map(topic -> schemaType))
    }
    this
  }

  def withTopics(topics: Option[Seq[String]]): ParamsBuilder = {
    this.topics = topics
    this
  }

  def withTopPagesNumber(number: Int): ParamsBuilder = {
    this.topPagesNumber = Some(number)
    this
  }

  def withLaunchApp(app: String): ParamsBuilder = {
    this.launchApp = Some(app)
    this
  }

  def build(): Params = {
    val instance = new Params()
    instance.kafkaBrokers = this.kafkaBrokers
    instance.delayPerTopic = this.delayPerTopic
    instance.windowDuration = this.windowDuration
    instance.slidingInterval = this.slidingInterval
    instance.writerType = this.writerType
    instance.readerType = this.readerType
    instance.writeInterval = this.writeInterval
    instance.outputTopic = this.outputTopic
    instance.transformationType = this.transformationType
    instance.eventTimeFieldPerTopic = this.eventTimeFieldPerTopic
    instance.schemaTypeByTopic = this.schemaTypeByTopic
    instance.topics = this.topics
    instance.topPagesNumber = this.topPagesNumber
    instance.launchApp = this.launchApp
    instance.schemaManager = this.schemaManager
    instance.transformationConfigPath = this.transformationConfigPath
    instance.kafkaInputSerialization = this.kafkaInputSerialization
    instance.kafkaWriterSerialization = this.kafkaWriterSerialization

    instance
  }
}

final class Params {
  var kafkaBrokers: Option[String] = None
  var delayPerTopic: Option[Map[String, Long]] = None
  var windowDuration: Long = 0L
  var slidingInterval: Long = 0L
  var writeInterval: Long = 0L
  var readerType: String = _
  var writerType: String = _
  var outputTopic: Option[String] = None
  var transformationType: String = _
  var eventTimeFieldPerTopic: Option[Map[String, String]] = None
  var schemaTypeByTopic: Option[Map[String, String]] = None
  var topics: Option[Seq[String]] = None
  var topPagesNumber: Option[Int] = None
  var launchApp: Option[String] = None
  var schemaManager: Option[String] = None
  var transformationConfigPath: Option[String] = None
  var kafkaInputSerialization: Option[String] = None
  var kafkaWriterSerialization: Option[String] = None
}

class CLIParams {
  def buildCLIParams(args: Seq[String]): Params = {
    val parser = new scopt.OptionParser[ParamsBuilder]("Streaming lib") {
      opt[Seq[String]](name = "kafka-brokers")
        .action((value, c) => c.withKafkaBrokers(value.mkString(",")))
        .text("list of kafka brokers, separated by ','")

      opt[Seq[String]](name = "input-topics")
        .action((value, c) => c.withTopics(Some(value)))
        .text("list of topics to read data from, separated by ','")

      opt[Map[String, String]](name = "topic-watermark-pair")
        .action((value, c) => {
          value.foreach(v => c.withEventTimeFieldPerTopic(v._1, v._2))
          c
        })
        .text("list of <topic,event_time_column_name> pairs. If input data has no event time, specify the column names to be generated per topic by engine")

      opt[Map[String, Long]](name = "topic-delay-pair")
        .action((value, c) => {
          value.foreach(v => c.withDelayPerTopic(v._1, v._2))
          c
        })
        .text("List of expected delays in each topic. Pairs <topic=delay> separated by ','")

      opt[String]("output-topic")
        .action((value, c) => c.withOutputTopic(value))
        .text("Topic name to write output to")

      opt[Long]("window-duration-seconds")
        .action((value, c) => c.withWindowDuration(value))
        .text("Duration of window in seconds")

      opt[Long]("sliding-window-seconds")
        .action((value, c) => c.withSlidingInterval(value))
        .text("Sliding window interval in seconds")

      opt[Long]("write-interval-seconds")
        .action((value, c) => c.withWriteInterval(value))
        .text("Interval in seconds for outputting computation to output topic")
      opt[String]("reader-type")
        .action((value, c) => c.withReaderType(value))
        .withFallback(() => KafkaReader.toString)
        .text(s"One of: ${ReaderFactory.AllReaderTypes.map(_.toString).mkString(",")}. Default is ${ReaderFactory.KafkaReader.toString}")

      opt[String]("writer-type")
        .action((value, c) => c.withWriterType(value))
        .withFallback(() => KafkaWriter.toString)
        .text(s"One of: ${WriterFactory.AllWriterTypes.map(_.toString).mkString(",")}. Default is ${WriterFactory.KafkaWriter.toString}")

      opt[String]("transform-type")
        .action((value, c) => c.withTransformationType(value))
        .withFallback(() => Top10ByGender.toString)
        .text(s"One of: ${TransformationFactory.AllTransformationTypes.map(_.toString).mkString(",")}. Default is ${TransformationFactory.Top10ByGender.toString}")

      opt[Int]("top-pages")
        .action((value, c) => c.withTopPagesNumber(value))
        .withFallback(() => 10)
        .text(s"Number of pages to return when transformation is ${TransformationFactory.Top10ByGender.toString}")

      opt[Map[String, String]]("topic-schematype-pair")
        .action((value, c) => {
          value.foreach(v => c.withSchemaTypePerTopic(v._1, v._2))
          c
        })
        .text("Pairs of <topic, schema_type>")

      opt[String]("application")
        .action((value, c) => c.withLaunchApp(value))
        .withFallback(() => AppLibrary.QuickJob.toString)
        .text(s"Enter the name of the application to run. For the challenge: ${AppLibrary.ChallengeApp.toString}")

      opt[String](name = "schema-manager")
        .action((value, c) => c.withSchemaManager(value))
        .text(s"Enter one of the following: ${SchemaManagementFactory.AllManagementTypes.map(t => t).mkString(",")}")

      opt[String](name = "kafka-input-serialization")
        .action((value, c) => c.withKafaInputSerialization(Some(value)))
        .text(s"""Enter one of the following: ${KafkaSerialization.AllSerializations.map(_.toString).mkString(",")}""")
        .withFallback(KafkaSerialization.JsonSerialization.toString)

      opt[String](name = "kafka-writer-serialization")
        .action((value, c) => c.withKafaInputSerialization(Some(value)))
        .text(s"""Enter one of the following: ${KafkaSerialization.AllSerializations.map(_.toString).mkString(",")}""")
        .withFallback(KafkaSerialization.JsonSerialization.toString)
    }

    parser.parse(args, new ParamsBuilder()) match {
      case Some(builder) => builder.build()
      case _ => throw new Exception("Error while parsing CLI args")
    }
  }
}
