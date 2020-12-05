package com.org.challenge.stream.schemas.default

import com.org.challenge.stream.schemas.SchemaModel
import com.org.challenge.stream.schemas.types.{TypeMapper}

object SchemaFieldModel {
  def map[A, B](typeMapper: TypeMapper[A, B], `type`: B): A = {
    typeMapper.map(`type`)
  }

  def reverseMap[A, B](typeMapper: TypeMapper[A, B], `type`: A): B = {
    typeMapper.reverseMap(`type`)
  }
}
case class SchemaFieldModel(name: String, `type`: String)
case class FileBasedSchemaModel(topic: String, override val fields: Seq[SchemaFieldModel]) extends SchemaModel(topic, fields)
