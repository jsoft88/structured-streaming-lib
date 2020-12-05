package com.org.challenge.stream.schemas.types

import org.apache.spark.sql.types.{DataType, IntegerType, LongType, ShortType, StringType}

trait SQLTypeMapper extends TypeMapper[DataType, String] {
  override def map(implicit `type`: String): DataType = {
    `type`.toLowerCase match {
      case PredefinedDomainTypes.String => StringType
      case PredefinedDomainTypes.Int => IntegerType
      case PredefinedDomainTypes.Long => LongType
      case PredefinedDomainTypes.Short => ShortType
      case _ => throw new Exception(s"No sql type found for ${`type`}")
    }
  }

  override def reverseMap(implicit `type`: DataType): String = {
    `type` match {
      case StringType => PredefinedDomainTypes.String
      case IntegerType => PredefinedDomainTypes.Int
      case LongType => PredefinedDomainTypes.Long
      case ShortType => PredefinedDomainTypes.Short
      case _ => throw new Exception(s"Impossible to reverse sql type ${`type`.simpleString}")
    }
  }
}
