package com.org.challenge.stream.core

import com.org.challenge.stream.utils.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Base abstraction for streaming jobs.
 * @param params parameters passed to the streaming job
 * @tparam P type of the parameters passed to the streaming job
 */
abstract class StreamJob[P](spark: SparkSession, params: P) extends Logger {
  /**
   * Perform any initializations required by the job, possibly by accessing the params object.
   */
  protected def setupJob(): Unit

  /**
   * Here one would setup the input stream and acquire a reader. The reader has to be used to initiate the
   * load of dataframes.
   * @return the dataframes obtained by reader. The key can be used for uniquely identifying multiple sources loaded
   *         by the reader(s). Example: Multiple topics, so we can have <t1, df1>,<t2, df2>, ...
   */
  protected def setupInputStream(): Option[Map[String, DataFrame]]

  /**
   * Perform any transformations to the dataframes in this method.
   * @param dataframes the pairs <t1, df1>, <t2, df2>
   * @return the transformed dataframe
   */
  protected def transform(dataframes: Option[Map[String, DataFrame]]): DataFrame

  /**
   * Obtain a writer and write the (possibly, transformed) dataframe to the target system.
   * @param dataFrame to write to target system
   */
  protected def writeStream(dataFrame: Option[DataFrame]): Unit

  /**
   * End any open connections, sessions, etc. at the end of the job.
   */
  protected def finalizeJob(): Unit

  protected def invokeWait(args: Any*): Unit

  /**
   * Orchestration method for the StreamJob
   */
  final def runStreamJob(): Unit = {
    try {
      this.setupJob()
      this.log.info("Job setup ready, now proceeding to read from source")
      val inputDF = setupInputStream()
      inputDF match {
        case None => {
          this.log.error("Input dataframe is None, something failed.")
          throw new RuntimeException("Input stream was None")
        }
        case Some(df) => {
          this.log.info("Now starting transformation of input dataframes...")
          val transformedDF = transform(Some(df))

          this.log.info("Transformation ready, invoking writer...")
          writeStream(Some(transformedDF))
          this.log.info("Now standing by for termination of streaming job...")
          this.invokeWait()
          this.log.info("Job completed successfully, proceeding termination...")
        }
      }
    } catch {
      case ex => this.log.error(s"FATAL ERR: ${ex.getMessage} --> ${ex.getStackTrace.mkString("\n")}")
    } finally {
      finalizeJob()
    }
  }
}
