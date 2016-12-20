[![Build Status](https://travis-ci.org/maropu/spark-kinesis-sql-asl.svg?branch=master)](https://travis-ci.org/maropu/spark-kinesis-sql-asl)

Structured Streaming integration for Kinesis and some utility stuffs for AWS

### How to use

For the Kinesis integration, you need to launch a spark-shell with this compiled jar.

    $ git clone https://github.com/maropu/spark-kinesis-sql-asl.git
    $ cd spark-kinesis-sql-asl
    $ ./bin/spark-shell --jars assembly/spark-sql-kinesis-asl_2.11-2.0.1.jar

### Prepare a test stream

    $ aws kinesis create-stream --stream-name LogStream --shard-count 2
    $ aws kinesis put-record --stream-name LogStream --partition-key 1 --data 1,abc,0.3
    $ aws kinesis put-record --stream-name LogStream --partition-key 2 --data 2,defghi,1.1
    $ aws kinesis put-record --stream-name LogStream --partition-key 3 --data 3,jk,0.1

### Creating a Kinesis source stream

    // Subscribe the "LogStream" stream
    scala> :paste
    val kinesis = spark
      .readStream
      .format("kinesis")
      .option("streams", "LogStream")
      .option("endpointUrl", "kinesis.ap-northeast-1.amazonaws.com")
      .option("initialPositionInStream", "earliest")
      .option("format", "csv")
      .option("inferSchema", "true")
      .load

    scala> kinesis.printSchema
    root
     |-- timestamp: timestamp (nullable = false)
     |-- _c0: integer (nullable = true)
     |-- _c1: string (nullable = true)
     |-- _c2: double (nullable = true)

    // Write the stream data into console
    scala> :paste
    kinesis
      .writeStream
      .format("console")
      .start()
      .awaitTermination()

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +--------------------+---+------+---+
    |           timestamp|_c0|   _c1|_c2|
    +--------------------+---+------+---+
    |2016-10-24 09:49:...|  1|   abc|0.3|
    |2016-10-24 09:50:...|  2|defghi|1.1|
    |2016-10-24 09:50:...|  3|    jk|0.1|
    +--------------------+---+------+---+
    ...

The following options must be set for the Kinesis source.

 * `streams`

    A stream list to read. You can specify multiple streams by setting a comma-separated string.

 * `endpointUrl`

    An entry point URL for Kinesis streams.

The following configurations are optional:

 * `initialPositionInStream `: \["earliest", "latest"\] (default: "latest")

    A start point when a query is started, either "earliest" which is from the earliest sequence
    number, or "latest" which is just from the latest sequence number. Note: This only applies
    when a new Streaming query is started, and that resuming will always pick up from
    where the query left off.

 * `format`: \["default", "csv", "json", "libsvm"\] (default: "default")

    A stream format of input stream data. Each format has configuration prameters:
    [csv](./external/kinesis-sql-asl/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVKinesisValueFormat.scala#L34),
    [json](./external/kinesis-sql-asl/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonKinesisValueFormat.scala#L36),
    and [libsvm](./external/kinesis-sql-asl/src/main/scala/org/apache/spark/ml/source/libsvm/LibSVMKinesisValueFormat.scala#L39)

 * `reportIntervalMs`: (default: "1000")

    Report interval time in milliseconds. This source implementation internally uses
    [Kinesis receivers in Spark Streaming](https://github.com/apache/spark/tree/master/external/kinesis-asl)
    and tracks available stream blocks and latest sequence numbers of shards by using the metadata
    that the receivers report at this interval.

 * `softLimitMaxRecordsPerTrigger`: (default: "-1")

    If a positive value is set, limit maximum processing number of records per trigger to prevent
    a job from having many records in a batch. Note that this is a soft limit, so the actual
    processing number of records goes beyond the value.

 * `limitMaxRecordsToInferSchema`: (default: 100000)

    Limit the number of records fetched from a shard to infer a schema. This source reads
    stream data from the earliest offset to a offset at current time. If the number of records
    goes beyond this value, it stops reading subsequent data.

 * `failOnDataLoss`: \["true", "false"\] (default: "false")

    Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or
    offsets are out of range). This may be a false alarm. You can disable it when it doesn't work
    as you expected.

### Output Operation for Spark Streaming

Since a Kinesis output operation for Spark Streaming is not officially supported in the latest Spark release,
this provides the operation like this;

    // Import a class that includes an output function
    scala> import org.apache.spark.streaming.kinesis.KinesisDStreamFunctions._

    // Create a DStream
    scala> val stream: DStream[String] = ...

    // Define a handler to convert the DStream type for output
    scala> val msgHandler = (s: String) => s.getBytes("UTF-8")

    // Define the output operation
    scala> val streamName = "OutputStream"
    scala> val endpointUrl = "kinesis.ap-northeast-1.amazonaws.com"
    scala> kinesisStream.count().saveAsKinesisStream(streamName, endpointUrl, msgHandler)

    // Start processing the stream
    scala> ssc.start()
    scala> ssc.awaitTermination()

## Read/Write data to S3

If you launch a spark-shell with this compiled jar, you can read data from S3 as follows;

     // Settings for S3
    scala> val hadoopConf = sc.hadoopConfiguration
    scala> hadoopConf.set("fs.s3n.awsAccessKeyId", "XXX")
    scala> hadoopConf.set("fs.s3n.awsSecretAccessKey", "YYY")
    scala> sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // Read CSV-fomatted data from S3
    scala> val df = spark.read.format("csv").option("path", "s3n://<bucketname>/<filename>.csv").load

### Points to Remember

Note that the total number of cores in executors must be bigger than the number of shards assigned
in streams because this source depends on the Kinesis integration and their receivers use
as many cores as a total shard number. More details can be found in
[the Spark Streaming documentation](http://spark.apache.org/docs/latest/streaming-programming-guide.html#points-to-remember-1).

### Bug reports

If you hit some bugs and requests, please leave some comments on
[Issues](https://github.com/maropu/spark-kinesis-sql-asl/issues) or
Twitter([@maropu](http://twitter.com/#!/maropu)).
