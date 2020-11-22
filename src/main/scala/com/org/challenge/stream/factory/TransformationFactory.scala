package com.org.challenge.stream.factory

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.transformation.{BaseTransform, NoOpTransform, TopPagesByGender}
import org.apache.spark.sql.SparkSession

sealed trait TransformationType

object TransformationFactory {
  case object Top10ByGender extends TransformationType {
    override def toString: String = "top10ByGender"
  }

  case object NoOp extends TransformationType {
    override def toString: String = "noOp"
  }

  val AllTransformationTypes = Seq(
    Top10ByGender,
    NoOp
  )

  def getTransformation(transformationType: TransformationType, spark: SparkSession, params: Params): BaseTransform = {
    transformationType match {
      case Top10ByGender => TopPagesByGender(spark, params)
      case NoOp => NoOpTransform(spark, params)
    }
  }
}
