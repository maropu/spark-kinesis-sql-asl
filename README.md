[![Build Status](https://travis-ci.org/maropu/spark-kinesis-sql-asl.svg?branch=master)](https://travis-ci.org/maropu/spark-kinesis-sql-asl)

Structured Streaming integration for Amazon Kinesis

### How to use
For the Kinesis integration, you need to launch spark-shell with this compiled jar.

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
    [csv](./external/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVKinesisValueFormat.scala#L33),
    [json](./external/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonKinesisValueFormat.scala#L36),
    and [libsvm](./external/src/main/scala/org/apache/spark/ml/source/libsvm/LibSVMKinesisValueFormat.scala#L39)

 * `failOnDataLoss`: \["true", "false"\] (default: "false")

    Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or
    offsets are out of range). This may be a false alarm. You can disable it when it doesn't work
    as you expected.

### Bug reports

If you hit some bugs and requests, please leave some comments on
[Issues](https://github.com/maropu/spark-kinesis-sql-asl/issues) or
Twitter([@maropu](http://twitter.com/#!/maropu)).
