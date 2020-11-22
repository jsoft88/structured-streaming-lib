package com.org.challenge.stream

import com.org.challenge.stream.config.CLIParams
import com.org.challenge.stream.factory.ApplicationFactory

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
    ApplicationFactory.getApplicationInstance(cliParams.launchApp.getOrElse(QuickJob.toString), cliParams)
      .runStreamJob()
  }
}
