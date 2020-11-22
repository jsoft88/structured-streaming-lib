package com.org.challenge.stream.transformation

import com.org.challenge.stream.config.Params
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseTransform(spark: SparkSession, params: Params) {
  this.setupTransformation()

  /**
   * Perform any required initializations here based on the params received
   */
  def setupTransformation(): Unit = {}

  /**
   * Write the transformations required to the input dataframes here
   * @param dataframes input dataframes
   * @return transformed dataframe
   */
  def transformStream(dataframes: Option[Map[String, DataFrame]]): Option[DataFrame]

  /**
   * Some limitations in the transformation or custom actions might be required,
   * use this method to implement final transformations required, that will be executed in micro-batches.
   * Normally, invoked by the Writer.
   * @param dataFrame corresponding to a micro-batch
   * @return possibly transformed dataframe
   */
  def transformBatch(dataFrame: Option[DataFrame], otherDFs: DataFrame*): Option[DataFrame]
}
