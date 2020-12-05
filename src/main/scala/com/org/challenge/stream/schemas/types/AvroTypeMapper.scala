package com.org.challenge.stream.schemas.types

import org.apache.avro.{Schema}

trait AvroTypeMapper extends TypeMapper[Schema.Type, String] {
  def map(implicit `type`: String): Schema.Type = {
    `type`.toLowerCase match {
      case PredefinedDomainTypes.String => Schema.Type.STRING
      case PredefinedDomainTypes.Int | PredefinedDomainTypes.Short => Schema.Type.INT
      case PredefinedDomainTypes.Long => Schema.Type.LONG
      case _ => throw new Exception(s"No avro type found for ${`type`}")
    }
  }

  override def reverseMap(implicit `type`: Schema.Type): String = {
    `type` match {
      case Schema.Type.STRING => PredefinedDomainTypes.String
      case Schema.Type.INT => PredefinedDomainTypes.Int
      case Schema.Type.LONG => PredefinedDomainTypes.Long
      case _ => throw new Exception(s"Impossible to revert invalid avro type ${`type`.getName}")
    }
  }
}
