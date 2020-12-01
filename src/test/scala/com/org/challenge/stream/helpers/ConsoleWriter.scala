package com.org.challenge.stream.helpers

import com.org.challenge.stream.config.Params
import com.org.challenge.stream.transformation.{BaseTransform, TopPagesByGender}
import com.org.challenge.stream.writers.BaseWriter
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ConsoleWriter(sparkSession: SparkSession, params: Params) extends BaseWriter(sparkSession, params) {
  override def setupWriter(): Unit = {}

  override def writer(dataframe: Option[DataFrame], transformInstance: BaseTransform): DataStreamWriter[Row] = {
    dataframe match {
      case None => throw new IllegalArgumentException("Cannot obtain writer for dataframe None")
      case Some(df) => {
        assert(transformInstance.isInstanceOf[TopPagesByGender])
        df
          .writeStream
          .foreachBatch((batchDF: DataFrame, batchId: Long) => {
            val transformedDF = transformInstance.transformBatch(Some(batchDF))
              assert(transformedDF != None)
                transformedDF.get
                  .createOrReplaceGlobalTempView("e2e_top_pages")
          })
          .outputMode(OutputMode.Append())
          .trigger(Trigger.Once())
      }
    }
  }
}
