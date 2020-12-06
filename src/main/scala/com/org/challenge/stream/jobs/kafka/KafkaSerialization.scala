package com.org.challenge.stream.jobs.kafka

trait KafkaSerialization

object KafkaSerialization {
  case object AvroSerialization extends KafkaSerialization {
    override def toString: String = "avro"
  }
  case object JsonSerialization extends KafkaSerialization {
    override def toString: String = "json"
  }

  val AllSerializations = Seq(
    KafkaSerialization.AvroSerialization,
    KafkaSerialization.JsonSerialization
  )

  def getKafkaSerialization(serialization: String): KafkaSerialization = {
    KafkaSerialization.AllSerializations.filter(s => s.toString.equals(serialization)).headOption match {
      case Some(ks) => ks
      case None => throw new Exception(s"Invalid Serialization format provided for kafka -> ${serialization}")
    }
  }
}
