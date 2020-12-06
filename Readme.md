# Generic structured streaming library
 ## Design
 To make it extensible, there is a main base class: `com.org.challenge.stream.core.StreamJob`, which is the base of every streaming
 task we might have. The main structure is as follows: 
   ```scala
   package com.org.challenge.stream.core
   
   import com.org.challenge.stream.utils.Logger
   import org.apache.spark.sql.DataFrame
   
   /**
    * Base abstraction for streaming jobs.
    * @param params parameters passed to the streaming job
    * @tparam P type of the parameters passed to the streaming job
    */
   abstract class StreamJob[P](params: P) extends Logger {
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
       try {
         this.setupJob()
         val inputDF = setupInputStream()
         inputDF match {
           case None => {
             this.log.error("Input dataframe is None, something failed.")
             throw new RuntimeException("Input stream was None")
           }
           case Some(df) => {
             val transformedDF = transform(Some(df))
             writeStream(Some(transformedDF))
           }
         }
       } catch {
         case ex => this.log.error(ex.getMessage)
       } finally {
         finalizeJob()
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
 
## Sample out of the box: Top pages by Gender
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
This is the implementation of the use case (`transformStream` method):
```scala
pageviewsDF
    .join(usersDF,
      expr(
        s"""
          |pv.userid = u.userid AND
          |pv.${eventTimePV} >= u.${eventTimeU} AND
          |pv.${eventTimePV} <= u.${eventTimeU} + interval ${usersDFDelay} seconds
          |""".stripMargin)
    )
    .select(
      col("pv.userId").as("userid"),
      col("u.gender").as("gender"),
      col("pv.viewtime").as("viewtime"),
      col("pv.pageid").as("pageid"),
      col(s"pv.${eventTimePV}").as(eventTimePV)
    )
```
Some explanation of the solution. The `watermark` part is for obtaining the fields that will allow the engine
to discard events that arrive really late. Normally, the engine would
use a column in the data being received, rather than the start of processing
by the engine.

And this completes the query (in `transformBatch` method):
```scala
val windowSpecPageId = Window.orderBy(col("total_viewtime").desc)
val distinctUserIds = preparedDF.agg(approx_count_distinct("userid").as("distinct_user_ids"))

Some(
  preparedDF
    .groupBy(
      window(
        col("timestamp"),
        s"${this.windowDuration} seconds",
        s"${this.slideInterval} seconds"
      ),
      col("gender"),
      col("pageid")
    )
    .agg(sum(col("viewtime")).as("total_viewtime"))
    .withColumn("page_pos", row_number().over(windowSpecPageId))
    .where(col("page_pos") <= lit(this.topPagesLimit))
    .select(
      col("gender"),
      col("pageid"),
      col("total_viewtime")
    ).crossJoin(distinctUserIds)
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
* --top-pages: Number of pages to choose for each gender.
* --application: Enter the name of the application to run. For the challenge: challenge
* --schema-manager: Which schema manager will be used to resolve schema from input data. Available `file-based`, which means
expected schemas to exist in json format under `resources/`.
* --kafka-input-serialization: Use one of `json`, `avro` to tell the engine the kind of serialization
is applied at the source.

## Schemas
As it can be seen from the last line in the itemization above, the final parameter
is which schema manager will be used by the library to resolve schemas of input data. There
is currently only one schema manager which is `file-based`. This schema manager uses the 
`resources/` directory as schema registry. In there, you can already find two files which are
relevant for the use case in this library, that is, `pageviews.json` and `users.json`. The format is
a json object, with very simple structure as you can see below:

```json
{
  "topic": "users",
  "fields": [
    {
      "name": "registertime",
      "type": "LONG"
    },
    {
      "name": "userid",
      "type": "STRING"
    },
    {
      "name": "regionid",
      "type": "STRING"
    },
    {
      "name": "gender",
      "type": "STRING"
    }
  ]
}
```

The values for the `type` key, are defined as *Predefined Domain Types*, which can be found
in the class `com.org.challenge.stream.schemas.types.PredefinedDomainTypes`.
It was also introduced the concept of type *Augmenters* which can be seen as a way
of augmenting the capabilities of the standard `StructType` to be used
as avro schemas for example. Actually, currently it is possible to use the class
in `com.org.challenge.stream.schemas.augmenters.SchemaToAvro` to parse data coming in
avro serialization format.

Also the schema management system included, introduced the capability to use
type mappers in `com.org.challenge.stream.schemas.types.TypeMapper`, which is a trait
containing methods to map from/to *Predefined Domain Types* to avro/sql types.

The core functionality of schemas are based on the following three classes:
* `com.org.challenge.stream.schemas.RegistryHandler`: implementations of this trait know
how to interact with the underlying schema registry, it know how to authenticate, query it, etc.
* `com.org.challenge.stream.schemas.SchemaManagement`: base class for abstracting the low level
implementation of the RegistryHandler by returning an schema of `StructType` for a given identifier.
* `com.org.challenge.stream.schemas.SchemaModel`: a class which represents the schema retrieved from
the registry, later converted to `StructType` (which can be augmented) but also
allows to go from `StructType` back to the class extending `SchemaModel`

## Build and Execution
Build -> `sbt package`

Execute with challenge specs -> ~~/bin/spark-submit --class com.org.challenge.stream.AppLibrary --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --packages org.apache.spark:spark-avro_2.11:2.4.0 --packages com.github.scopt:scopt_2.11:3.7.0 /tmp/streaming-lib_2.12-2.4.6_0.1.jar --kafka-brokers localhost:9092 --input-topics pageviews,users --topic-watermark-pair pageviews=timestamp,users=timestamp --topic-delay-pair pageviews=20,users=20 --output-topic top_pages --window-duration-seconds 60 --sliding-window-seconds 30 --write-interval-seconds 60 --reader-type kafka --writer-type kafka --transform-type top10ByGender --top-pages 10 --application challenge --topic-schematype-pair pageviews=pageviews,users=users~~

Now it is not longer required to include dependencies like this, since `assembly` plugin has been included, which generates a fat jar.
Keep in mind the packaging process can take several minutes to complete.

The new execution looks like this: `spark-submit --master local[*] --class com.org.challenge.stream.AppLibrary /path/to/jars/streaming-lib-assembly-0.1.jar --schema-manager file-based --kafka-brokers broker:9092 --input-topics pageviews,users --topic-watermark-pair pageviews=timestamp,users=timestamp --topic-delay-pair pageviews=50,users=10 --output-topic top_pages --window-duration-seconds 10 --sliding-window-seconds 5 --write-interval-seconds 60 --reader-type kafka --writer-type kafka --transform-type top10ByGender --top-pages 10 --application challenge --topic-schematype-pair pageviews=pageviews,users=users --kafka-input-serialization json` 

Make sure to check the path to jar

Note: you can also run the tests via `sbt testOnly *<TestClass>` 