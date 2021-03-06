package com.org.challenge.stream.factory

import com.org.challenge.stream.AppLibrary
import com.org.challenge.stream.config.Params
import com.org.challenge.stream.core.StreamJob
import com.org.challenge.stream.jobs.kafka.PageViewsStream
import org.apache.spark.sql.SparkSession

object ApplicationFactory {
  def getApplicationInstance(appType: String, spark:SparkSession, params: Params): StreamJob[Params] = {
    AppLibrary.AllApps.filter(_.toString.toLowerCase.equals(appType)).headOption match {
      case None => throw new IllegalArgumentException("The requested application does not exist")
      case Some(at) => at match {
        case AppLibrary.ChallengeApp => new PageViewsStream(spark, params)
      }
    }
  }
}
