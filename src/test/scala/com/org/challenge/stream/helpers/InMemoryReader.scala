package com.org.challenge.stream.helpers

import java.io.ByteArrayOutputStream

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.readers.BaseReader
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

case class InMemoryReader(spark: SparkSession, params: Params, avroSchema: Map[String, Schema]) extends BaseReader(spark, params){
  override def read(): Option[DataFrame] = {
    import spark.implicits._

    this.params.topics.get.head match {
      case "users" => {
        Some(
          Seq(
            ("user1", "M", 500L, "1"),
            ("user2", "F", 8000L, "2"),
            ("user3", "M", 2000L, "3"),
            ("user4", "F", 1500L, "4"),
            ("user5", "F", 5500L, "5")
          ).map(t => {
            val rec = new GenericData.Record(avroSchema.get("users").head)
            rec.put("userid", t._1)
            rec.put("gender", t._2)
            rec.put("registertime", t._3)
            rec.put("regionid", t._4)
            val out = new ByteArrayOutputStream()
            val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
            new GenericDatumWriter[GenericRecord](avroSchema.get("users").head)
              .write(rec, encoder)
            encoder.flush()
            (out.toByteArray, System.currentTimeMillis() / 1000L)
          }).toDF("value", "timestamp")
        )
      }
      case "pageviews" => {
        Some(
          Seq(
            ("user1", 1000L, "1"),
            ("user2", 5000L, "2"),
            ("user3", 3000L, "3"),
            ("user4", 2000L, "4"),
            ("user5", 4500L, "5")
          ).map(t => {
            val rec = new GenericData.Record(avroSchema.get("pageviews").head)
            rec.put("userid", t._1)
            rec.put("viewtime", t._2)
            rec.put("pageid", t._3)
            val out = new ByteArrayOutputStream()
            val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
            new GenericDatumWriter[GenericRecord](avroSchema.get("pageviews").head)
              .write(rec, encoder)
            encoder.flush()
            (out.toByteArray, System.currentTimeMillis() / 1000L)
          }).toDF("value", "timestamp")
        )
      }
    }
  }
}
