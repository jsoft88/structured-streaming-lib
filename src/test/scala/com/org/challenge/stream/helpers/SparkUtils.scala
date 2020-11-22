package com.org.challenge.stream.helpers

import org.apache.spark.sql.SparkSession

object SparkUtils {
  private var sparkSession: Option[SparkSession] = None

  def getGlobalTestSparkSession(): SparkSession = {
    this.sparkSession match {
      case None => {
        SparkSession.builder()
          .appName("spark-test-session")
          .master("local[*]")
          .getOrCreate()
      }
      case Some(ss) => ss
    }
  }

  def stopSparkSession(): Unit = {
    this.sparkSession match {
      case None =>
      case Some(ss) => ss.stop()
    }
  }
}
