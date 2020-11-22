# Joyn Challenge
## Problem

1. Download the Confluent Platform Quick Start and set it up
2. Create three topics: users, pageviews and top_pages
3. Use the Kafka Connect DataGenConnector to produce data into these topics, using the users_extended and pageview quickstarts
4. Use a stream processing engine other than KSQL to develop a processor that:
 * Joins the messages in these two topics on the user id field
 * Uses a 1 minute hopping window with 10 second advances to compute the 10 most viewed pages by viewtime for every value of gender
 * Once per minute produces a message into the top_pages topic that contains the gender, page id, sum of view time in the latest window and distinct count of user ids in the latest window
 
 ## Solution Design
 To make it extensible, there is a main base class: `com.joyn.challenge.stream.core.StreamJob`, which is the base of every streaming
 task we might have. The main structure is as follows: 
   ```scala
   package com.joyn.challenge.stream.core
   
   import org.apache.spark.sql.DataFrame
   
   /**
    * Base abstraction for streaming jobs.
    * @param params parameters passed to the streaming job
    * @tparam P type of the parameters passed to the streaming job
    */
   abstract class StreamJob[P](params: P) {
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
   
     /**
      * Orchestration method for the StreamJob
      */
     final def runStreamJob(): Unit = {
       val inputDF = setupInputStream()
       inputDF match {
         case None => {
           finalizeJob()
           throw new RuntimeException("Input stream was None")
         }
         case Some(df) => {
           val transformedDF = transform(Some(df))
           writeStream(Some(transformedDF))
           finalizeJob()
         }
       }
   
     }
   }
```
The idea is to have different stakeholder requirements added to the framework,
where new requirements would be implemented as child classes of StreamJob. Whether the new
requirement is about Streaming or Batch jobs, the base class still allows for lean implementation.

## Good practice
There are `Factories` for main components of the framework, that is,
for the readers, transformers (this is just fancy huh? xD) and writers.
This brings 2 advantages:
1. First of all, modularization. Having components writen in different packages,
and files, makes it easier to `maintain`. Also, it is `extensible` as we can replace
Readers, Transformers, Writers directly from CLI by providing the type and need to introduce
minimal changes, mainly, in implementing the component; the main body (possibly) will work out
of the box.
2. `Testing`: we can test the transformation by replacing the source and target by
simpler components. As such, we can setup a standard reader and writer for the test cases,
and just invoke the transformation from the `Factory` to test that the produced Dataframe is what the stakeholders need.
 
## Challenge: Top pages by Gender
The implementation of this transformation can be found under 
`com.joyn.challenge.stream.transformation.TopPagesByGender`. The
base structure of the transformation is defined in `BaseTransform`.

```scala
abstract class BaseTransform(spark: SparkSession, params: Params) {
  import spark.implicits._

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
  def transformBatch(dataFrame: Option[DataFrame]): Option[DataFrame]
}
```
This is the implementation of the challenge:
```scala
import com.org.challenge.stream.config.Params
import com.org.challenge.stream.jobs.kafka.PageViewsStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum, window}

/**
 * Given that transformations are made based on stakeholders requirements, we will have different
 * classes here for providing such functionalities. In this class, Joyn recruitment team asked the following:
 * ------------------------------------------
 * - Joins the messages in these two topics on the user id field
 * - Uses a 1 minute hopping window with 10 second advances to compute the 10 most viewed pages by viewtime for every value of gender
 * - Once per minute produces a message into the top_pages topic that contains the gender, page id, sum of view time in the latest window and distinct count of user ids in the latest window
 * ------------------------------------------
 *
 * So this is a class providing the transformations required in this request. As more requests arrive to the team, we could
 * extend the framework and provide it via Factory.
 *
 * {@note}: Notice that there's no loss of generalization by directly referencing
 * {@code com.joyn.challenge.stream.jobs.kafka.PageViewsStream.{PageViewsSchema, UsersSchema}, since as mentioned
 * in {@see com.joyn.challenge.stream.jobs.kafka.PageViewsStream}, I am using the companion object as a schema registry.
 *
 * Given that the transformation is specific to a requirement, it is ok to reference directly, even though the user is prompted
 * to provide topics via CLI params. However, this allows for flexibility in other components, say, readers.
 *
 * This is not different than directly accessing column names in the dataframes.
 * @param spark an active spark session
 * @param params parameters provided to the application
 */
case class TopPagesByGender(spark: SparkSession, params: Params) extends BaseTransform(spark, params) {
  private val DefaultNumberOfTopPages = 10
  private var pageViewsWatermark: String = _
  private var usersWatermark: String = _
  import spark.implicits._

  override def transformStream(dataframes: Option[Map[String, DataFrame]]): Option[DataFrame] = {
    dataframes match {
      case None => throw new IllegalArgumentException("Dataframe to transform is None")
      case Some(dfList) => {
        val pageViewsDF = dfList.get(PageViewsStream.PageViewsSchema.toString).get
        val usersDF = dfList.get(PageViewsStream.UsersSchema.toString).get
        val windowSpecPageId = Window.partitionBy("gender").orderBy(col("total_viewtime").desc)
        params.eventTimeFieldPerTopic match {
          case None => throw new IllegalArgumentException("Field for watermark expected in parameters, but None found")
          case Some(fields) => (fields.get(PageViewsStream.PageViewsSchema.toString), fields.get(PageViewsStream.UsersSchema.toString)) match {
            case (None, _) => throw new Exception("Watermark for one required topic was not specified")
            case (_, None) => throw new Exception("Watermark for one required topic was not specified")
            case (Some(pageViewsField), Some(usersField)) => {
              this.pageViewsWatermark = pageViewsField
              this.usersWatermark = usersField
            }
          }
        }

        Some(
          pageViewsDF
          .join(usersDF,
            pageViewsDF("userid") === usersDF("userid") &&
              pageViewsDF(this.pageViewsWatermark) >= usersDF(this.usersWatermark) &&
              pageViewsDF(this.pageViewsWatermark) <= usersDF(this.usersWatermark) + functions.expr(s"interval ${params.delayPerTopic.get.get(PageViewsStream.UsersSchema.toString).head} seconds")
          ).select(
            usersDF("userid").as("userid"),
            pageViewsDF("viewtime").as("viewtime"),
            pageViewsDF(this.pageViewsWatermark),
            usersDF("gender").as("gender"),
            pageViewsDF("pageid").as("pageid")
          ).groupBy(
            col("gender"),
            window(col(this.pageViewsWatermark), s"${params.windowDuration} seconds", s"${params.slidingInterval} seconds"),
            col("pageid")
          ).agg(
            sum(col("viewtime")).as("total_viewtime"), functions.approx_count_distinct(col("userid")).as("distinct_userid_count")
          )
        )

      }
    }
  }

  override def transformBatch(dataFrame: Option[DataFrame]): Option[DataFrame] = {
    dataFrame match {
      case None => throw new IllegalArgumentException("Expected dataframe to transform in microbatch, but None found")
      case Some(preparedDF) => {
        val windowSpecPageId = Window.partitionBy("gender").orderBy(col("total_viewtime").desc)
        Some(
          preparedDF
            .withColumn(
              "page_pos", row_number().over(windowSpecPageId)
            ).where(col("page_pos") <= functions.lit(this.params.topPagesNumber.getOrElse(this.DefaultNumberOfTopPages)))
        )
      }
    }
  }
}
```
Some explanation of the solution. The `watermark` part is for obtaining the fields that will allow the engine
to discard events that arrive really late. Normally, the engine would
use a column in the data being received, rather than the start of processing
by the engine. As I could not find a suitable column for `watermark` (NOTE: this is also provided by the user of the frameword
via CLI args), I decided to use the timestamp field in kafka schema. So the part of the code below, is mainly
checking that the watermark fields for the given topics exist and that a dataframe is present.

```scala
dataframes match {
      case None => throw new IllegalArgumentException("Dataframe to transform is None")
      case Some(dfList) => {
        val pageViewsDF = dfList.get(PageViewsStream.PageViewsSchema.toString).get
        val usersDF = dfList.get(PageViewsStream.UsersSchema.toString).get
        val windowSpecPageId = Window.partitionBy("gender").orderBy(col("total_viewtime").desc)
        params.eventTimeFieldPerTopic match {
          case None => throw new IllegalArgumentException("Field for watermark expected in parameters, but None found")
          case Some(fields) => (fields.get(PageViewsStream.PageViewsSchema.toString), fields.get(PageViewsStream.UsersSchema.toString)) match {
            case (None, _) => throw new Exception("Watermark for one required topic was not specified")
            case (_, None) => throw new Exception("Watermark for one required topic was not specified")
            case (Some(pageViewsField), Some(usersField)) => {
              this.pageViewsWatermark = pageViewsField
              this.usersWatermark = usersField
            }
          }
        }
```
The next part produces the following output:
----------------------------------------------------------------------
| gender | pageid | distinct_userid_by_gender_count | total_viewtime |
----------------------------------------------------------------------

```scala
pageViewsDF
          .join(usersDF,
            pageViewsDF("userid") === usersDF("userid") && //join column and watermarking checks
              pageViewsDF(this.pageViewsWatermark) >= usersDF(this.usersWatermark) &&
              pageViewsDF(this.pageViewsWatermark) <= usersDF(this.usersWatermark) + functions.expr(s"interval ${params.delayPerTopic.get.get(PageViewsStream.UsersSchema.toString).head} seconds")
          ).select(
            usersDF("userid").as("userid"),
            pageViewsDF("viewtime").as("viewtime"),
            pageViewsDF(this.pageViewsWatermark),
            usersDF("gender").as("gender"),
            pageViewsDF("pageid").as("pageid")
          ).groupBy(
            col("gender"),
            window(col(this.pageViewsWatermark), s"${params.windowDuration} seconds", s"${params.slidingInterval} seconds"), //Computation window as requested
            col("pageid")
          ).agg(
            sum(col("viewtime")).as("total_viewtime"), functions.approx_count_distinct(col("userid")).as("distinct_userid_count") //countDistinct not supported yet
          )
```
Finally, the writer will invoke for every microbatch, the calculation of the top 10
pages, by using an analytical function `row_number()` (done here as of spark 2.4)
using a window with partition by gender, produces an error.

```scala
    val windowSpecPageId = Window.partitionBy("gender").orderBy(col("total_viewtime").desc)
    Some(
      preparedDF
        .withColumn(
          "page_pos", row_number().over(windowSpecPageId)
        ).where(col("page_pos") <= functions.lit(this.params.topPagesNumber.getOrElse(this.DefaultNumberOfTopPages)))
        .select(to_avro(functions.struct(preparedDF.columns.map(col _): _*).alias("value")))
    )
```

## Usage and config
It takes the following arguments: 
* --kafka-brokers: list of kafka brokers, separated by ','
* --input-topics: list of topics to read data from, separated by ','
* --topic-watermark-pair: list of <topic,event_time_column_name> pairs. If input data has no event time, specify the column names to be generated per topic by engine
* --topic-delay-pair: List of expected delays in each topic. Pairs <topic=delay> separated by ','
* --output-topic: Topic name to write output to.
* --window-duration-seconds: Duration of window in seconds
* --sliding-window-seconds: Sliding window interval in seconds
* --write-interval-seconds: Interval in seconds for outputting computation to output topic
* --reader-type: One of: kafka. Default is kafka.
* --writer-type: One of: Kafka. Default is kafka.
* --transform-type: One of: top10ByGender, noOp. Default is top10ByGender.
* --top-pages: number of pages to choose for each gender.
* --application: Enter the name of the application to run. For the challenge: challenge

## Build and Execution
Build -> `sbt package`

Execute with challenge specs -> `/bin/spark-submit --class com.joyn.challenge.stream.AppLibrary --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --packages org.apache.spark:spark-avro_2.11:2.4.0 --packages com.github.scopt:scopt_2.11:3.7.0 /tmp/joyn-challenge_2.11-2.4.0_0.1.jar --kafka-brokers localhost:9092 --input-topics pageviews,users --topic-watermark-pair pageviews=timestamp,users=timestamp --topic-delay-pair pageviews=20,users=20 --output-topic top_pages --window-duration-seconds 60 --sliding-window-seconds 30 --write-interval-seconds 60 --reader-type kafka --writer-type kafka --transform-type top10ByGender --top-pages 10 --application challenge --topic-schematype-pair pageviews=pageviews,users=users`

Make sure to check the path to jar

## Testing
Tried an approach with `MemoryStream` but unfortunately it is not working,
the job runs until timeout but no output is obtained. I need more time to
dig deeper and found the problem. To be honest I am using `Structured Streaming` for the
first time, so there might be something very basic wrong. 