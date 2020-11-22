package com.org.challenge.stream.helpers

import java.io._
import io.circe._
import io.circe.parser._

object TestUtils {

  def modifyTimestampInResource(path: String, outputPath: String): Unit = {
    try {
      val inputFile = new File(path)
      val inputStream = new FileInputStream(inputFile)
      val outputFile = new File(outputPath)
      val bw = new BufferedWriter(new FileWriter(outputFile))
      scala.io.Source.fromInputStream(inputStream).getLines().foreach { line =>
        parse(line).toOption match {
          case None => throw new Exception(s"Error while parsing: ${line}")
          case Some(js) => {
            js.hcursor.withFocus(jObj => {
              jObj.mapObject(j => j.remove("timestamp").add("timestamp", Json.fromLong(System.currentTimeMillis()/1000L)))
            }).top match {
              case None => throw new Exception("Failed to add timestamp field to json")
              case Some(newJson) => bw.write("\n" + newJson.pretty(Printer.noSpaces))
            }
          }
        }
      }
      bw.flush()
      bw.close()
      inputStream.close()
    } catch {
      case ex => throw ex
    }
  }
}
