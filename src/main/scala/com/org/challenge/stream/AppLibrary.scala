package com.org.challenge.stream

import com.org.challenge.stream.config.CLIParams
import com.org.challenge.stream.factory.ApplicationFactory
import org.apache.spark.sql.SparkSession

sealed trait AppLibEntry

object AppLibrary {
  case object ChallengeApp extends AppLibEntry {
    override def toString: String = "challenge"
  }

  case object QuickJob extends AppLibEntry {
    override def toString: String = "nothing"
  }

  val AllApps = Seq(ChallengeApp)

  def main(args: Array[String]): Unit = {
    val cliParams = new CLIParams().buildCLIParams(args)
    val sparkSession = SparkSession.builder().appName(s"stream-job-${System.currentTimeMillis()}").getOrCreate()
    ApplicationFactory.getApplicationInstance(cliParams.launchApp.getOrElse(QuickJob.toString), sparkSession, cliParams)
      .runStreamJob()
  }
}
