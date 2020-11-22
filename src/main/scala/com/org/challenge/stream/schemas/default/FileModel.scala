package com.org.challenge.stream.schemas.default

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, ShortType, StringType}

case class SchemaFieldModel(name: String, `type`: String) {
  def getSQLType(): DataType = {
    this.`type`.toLowerCase match {
      case "string" => StringType
      case "int" => IntegerType
      case "bigint" => LongType
      case "short" => ShortType
    }
  }
}
case class FileBasedSchemaModel(topic: String, fields: Seq[SchemaFieldModel])
