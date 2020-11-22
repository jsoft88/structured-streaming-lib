package com.org.challenge.stream.helpers

import org.apache.spark.sql.SparkSession

object SparkUtils {
  private var sparkSession: Option[SparkSession] = None

  def getGlobalTestSparkSession(): SparkSession = {
    this.sparkSession match {
      case None => {
        this.sparkSession = Some(SparkSession
          .builder()
          .appName("spark-test-session")
          .master("local[*]")
          .getOrCreate())

        this.sparkSession.get
      }
      case Some(ss) => ss
    }
  }

  def stopSparkSession(): Unit = {
    this.sparkSession match {
      case None =>
      case Some(_) => this.sparkSession.get.stop(); this.sparkSession = None
    }
  }
}
