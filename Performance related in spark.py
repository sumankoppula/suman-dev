# Databricks notebook source
df_transformed.unpersist()

# COMMAND ----------

# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Cache the transformed DataFrame
df_transformed.cache()

# COMMAND ----------

# Trigger an action to materialize the cache
df_transformed.count() 

# COMMAND ----------





# Additional actions will benefit from the cached data
df_transformed.filter(df_transformed["Age"] > 30).show()


# COMMAND ----------

# MAGIC %md
# MAGIC Persist allows more control over the storage level, including storing data on disk, memory, or a combination of both.
# MAGIC
# MAGIC Different Storage Levels with persist
# MAGIC
# MAGIC You can use different storage levels with persist to optimize the storage based on your use case:
# MAGIC
# MAGIC MEMORY_ONLY: Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed.
# MAGIC
# MAGIC MEMORY_AND_DISK: Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when needed.
# MAGIC
# MAGIC MEMORY_ONLY_SER: Store RDD as serialized Java objects (one byte array per partition). This is more space-efficient than deserialized objects, especially useful for fast serialization libraries.
# MAGIC
# MAGIC MEMORY_AND_DISK_SER: Similar to MEMORY_AND_DISK, but store partitions as serialized objects.
# MAGIC DISK_ONLY: Store RDD partitions only on disk.

# COMMAND ----------

from pyspark import StorageLevel

# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Persist the transformed DataFrame with MEMORY_AND_DISK_SER storage level
#df_transformed.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Define a custom storage level with serialization enabled
custom_storage_level = StorageLevel(useDisk=True, useMemory=True, useOffHeap=False, deserialized=True)

# Persist the DataFrame with the custom storage level
df_transformed.persist(custom_storage_level)

# Trigger an action to materialize the persisted data
df_transformed.show()

# Additional actions will benefit from the persisted data
df_transformed.filter(df_transformed["Age"] > 30).show()


# COMMAND ----------

# Using MEMORY_ONLY
# Stores RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed.


from pyspark import StorageLevel

# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Persist the transformed DataFrame with MEMORY_ONLY storage level
df_transformed.persist(StorageLevel.MEMORY_ONLY)

# Trigger an action to materialize the persisted data
df_transformed.show()


# COMMAND ----------

# Using MEMORY_AND_DISK
# Stores RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when needed.

from pyspark import StorageLevel


# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Persist the transformed DataFrame with MEMORY_AND_DISK storage level
df_transformed.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger an action to materialize the persisted data
df_transformed.show()


# COMMAND ----------

df_transformed.unpersist()

# COMMAND ----------

# Using MEMORY_ONLY_SER
# Stores RDD as serialized Java objects (one byte array per partition). This is more space-efficient than deserialized objects, especially useful for fast serialization libraries.


from pyspark import StorageLevel

# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Define a custom storage level with serialization enabled
custom_storage_level = StorageLevel(useDisk=True, useMemory=False, useOffHeap=False, deserialized=True)

# Persist the DataFrame with the custom storage level
df_transformed.persist(custom_storage_level)


# Trigger an action to materialize the persisted data
df_transformed.count()


# COMMAND ----------

# Using MEMORY_AND_DISK_SER
# Similar to MEMORY_AND_DISK, but stores partitions as serialized objects.

from pyspark.sql import SparkSession
from pyspark import StorageLevel

# Initialize Spark session
spark = SparkSession.builder.appName("MemoryAndDiskSerExample").getOrCreate()

# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Persist the transformed DataFrame with MEMORY_AND_DISK_SER storage level
#df_transformed.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Define a custom storage level with serialization enabled
custom_storage_level = StorageLevel(useDisk=True, useMemory=True, useOffHeap=False, deserialized=True)

# Persist the DataFrame with the custom storage level
df_transformed.persist(custom_storage_level)

# Trigger an action to materialize the persisted data
df_transformed.show()


# COMMAND ----------

# Using DISK_ONLY
# Stores RDD partitions only on disk.

from pyspark.sql import SparkSession
from pyspark import StorageLevel

# Initialize Spark session
spark = SparkSession.builder.appName("DiskOnlyExample").getOrCreate()

# Create an example DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform a transformation
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Persist the transformed DataFrame with DISK_ONLY storage level
df_transformed.persist(StorageLevel.DISK_ONLY)

# Trigger an action to materialize the persisted data
df_transformed.show()


# COMMAND ----------

What is Spill in Spark?
Spill in Spark occurs when the intermediate data generated during the execution of a Spark job exceeds the available memory. When this happens, Spark writes this data to disk to avoid running out of memory and causing an out-of-memory (OOM) error. This process is known as spilling. Spilling can happen during various stages of a Spark job, such as shuffling, sorting, or aggregating data.

Examples of Spill:

Shuffle Spill: During a shuffle operation, if the data being shuffled cannot fit into memory, Spark will spill some of the data to disk.
Sort Spill: When sorting a large dataset that cannot fit into memory, Spark will spill parts of the dataset to disk to complete the sort operation.
Spilling is an expensive operation because reading from and writing to disk is much slower than accessing data in memory. Therefore, minimizing spills is important for optimizing Spark job performance.

Monitoring Spills:

You can monitor spills in the Spark UI under the "Stages" tab. Look for metrics such as "Spill (Memory)" and "Spill (Disk)" to understand the extent of spilling happening in your job.

# COMMAND ----------

What is Data Skew in Spark?
Data skew in Spark refers to an uneven distribution of data across partitions during a Spark job. When data skew occurs, some partitions may have significantly more data than others, leading to performance bottlenecks. This is because tasks that process larger partitions take longer to complete, causing an imbalance in the workload and inefficient resource utilization.

Causes of Data Skew:

Highly Skewed Keys: When certain keys in the dataset have a disproportionately large number of records, leading to skewed partitions.
Uneven Data Source: When the input data source itself is unevenly distributed.
Complex Joins and Aggregations: Certain join and aggregation operations can exacerbate data skew if they involve skewed keys.
Examples of Data Skew:

Skewed Join: If one side of a join operation has a highly skewed key, it can cause data skew.
Skewed GroupBy: When performing a groupBy operation on a column with a highly skewed distribution, some partitions will end up with much more data than others.
Mitigating Data Skew:

Salting: Adding a random value (salt) to the key to distribute skewed keys across multiple partitions.
Broadcast Joins: Using broadcast joins to handle small skewed tables.
Repartitioning: Repartitioning the data to ensure a more even distribution.
Skew Join Optimization: Using techniques such as skew join optimization available in Spark to handle skewed joins.

# COMMAND ----------

# Example of Handling Skew with Salting:

from pyspark.sql.functions import lit, monotonically_increasing_id

# Create an example DataFrame
data = [("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key3", 5)]
columns = ["key", "value"]
df = spark.createDataFrame(data, columns)

# Add a salt column to distribute skewed keys
df_salted = df.withColumn("salt", monotonically_increasing_id() % 10)
df_salted = df_salted.withColumn("salted_key", lit(df_salted["key"]) + lit("_") + lit(df_salted["salt"]))

# Perform a groupBy on the salted key
df_grouped = df_salted.groupBy("salted_key").sum("value")

df_grouped.show()


# COMMAND ----------

Summary

Spill: Occurs when intermediate data exceeds available memory and is written to disk, affecting performance.

Data Skew: Refers to uneven data distribution across partitions, leading to performance bottlenecks. Mitigation strategies include salting, broadcast joins, repartitioning, and using skew join optimization.

# COMMAND ----------

# Broadcast Variables
# In Spark, broadcast variables allow you to efficiently distribute large read-only data across all worker nodes. They are particularly useful when you need to send a large lookup table to all nodes.

# Example of Broadcast Variable
# Let's consider a scenario where we have a large lookup table and we need to use it in our transformations.


# Example lookup table
lookup_table = {
    "Alice": "Engineering",
    "Bob": "HR",
    "Catherine": "Finance",
    "David": "Engineering"
}

# Broadcast the lookup table
broadcast_lookup = sc.broadcast(lookup_table)

# Example data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Use the broadcast variable in a transformation
def add_department(name):
    return broadcast_lookup.value.get(name, "Unknown")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

add_department_udf = udf(add_department, StringType())

df_with_department = df.withColumn("Department", add_department_udf(df["Name"]))

df_with_department.show()


# COMMAND ----------

# Accumulators
# Accumulators are variables that can be used to aggregate information across the workers in an efficient and fault-tolerant way. They are typically used for counting or summing purposes.

# Initialize an accumulator
accumulator = sc.accumulator(0)

# Example data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Function to update the accumulator
def count_over_40(age):
    if age > 40:
        accumulator.add(1)
    return age

# Use the accumulator in a transformation
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

count_over_40_udf = udf(count_over_40, IntegerType())

df_with_count = df.withColumn("Age", count_over_40_udf(df["Age"]))

# Perform an action to trigger the computation
df_with_count.show()

# Print the accumulator value
print("Number of people over 40:", accumulator.value)


# COMMAND ----------

# MAGIC %md
# MAGIC Summary
# MAGIC
# MAGIC Broadcast Variables: Efficiently distribute large read-only data across all worker nodes. Useful for large lookup tables.
# MAGIC
# MAGIC Accumulators: Aggregate information across the workers in an efficient and fault-tolerant way. Useful for counting or summing values.

# COMMAND ----------

# MAGIC %md
# MAGIC compare direct sum and using accumulator:
# MAGIC     
# MAGIC Accumulators
# MAGIC
# MAGIC Accumulators are variables that are used to perform aggregations in a distributed manner, across all worker nodes. They are primarily used for side-effect operations, like counting events or tracking metrics, without altering the original RDD or DataFrame.
# MAGIC
# MAGIC Key Differences and Use Cases:
# MAGIC     
# MAGIC Distributed Aggregation:
# MAGIC
# MAGIC Accumulators: Aggregators that work across all worker nodes. They allow you to collect statistics or metrics while transformations are being executed, without modifying the dataset.
# MAGIC
# MAGIC Regular Summation: Directly sums up values from the dataset using transformations or DataFrame operations.
# MAGIC Side-Effects and Metrics Collection:
# MAGIC
# MAGIC Accumulators: Useful for collecting metrics and performing side-effect operations. For example, counting errors, or tracking progress, which do not affect the main computation.
# MAGIC
# MAGIC Regular Summation: Directly involved in the primary computation and affects the resulting dataset.
# MAGIC Fault Tolerance:
# MAGIC
# MAGIC Accumulators: Spark ensures that updates to accumulators are reliable and fault-tolerant. If a task is re-executed, the accumulator will only be updated once for each task.
# MAGIC
# MAGIC Regular Summation: Part of the main computation and benefits from Spark’s fault tolerance mechanisms naturally.
# MAGIC Usability in Actions vs Transformations:
# MAGIC
# MAGIC Accumulators: Typically updated within transformations but are only guaranteed to be consistent after an action is performed (like count, collect, etc.).
# MAGIC
# MAGIC Regular Summation: Often used as part of actions to derive a final value directly.

# COMMAND ----------

PartitionBy and Bucketing in Spark

PartitionBy and Bucketing are two techniques used in Spark to optimize the storage and processing of large datasets. They help in improving the performance of data reads and writes by organizing data in a way that reduces data shuffling and allows for more efficient querying.

PartitionBy

PartitionBy is used to physically partition data based on the values of one or more columns. This technique is particularly useful for optimizing queries that filter on partition columns.

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("PartitionByExample").getOrCreate()

# Example data
data = [("Alice", "Engineering", 34), ("Bob", "HR", 45), ("Catherine", "Finance", 29), ("David", "Engineering", 42)]
columns = ["Name", "Department", "Age"]
df = spark.createDataFrame(data, columns)

# Write the DataFrame partitioned by the "Department" column
output_path = "/path/to/output/partitioned_data"
df.write.partitionBy("Department").parquet(output_path)

# Read the partitioned data
partitioned_df = spark.read.parquet(output_path)
partitioned_df.show()


# COMMAND ----------

# In this example, the data is written to disk with a directory structure that reflects the partitioning by the "Department" column. For example:
    
# /path/to/output/partitioned_data/Department=Engineering/part-00000.parquet
# /path/to/output/partitioned_data/Department=HR/part-00000.parquet
# /path/to/output/partitioned_data/Department=Finance/part-00000.parquet


# COMMAND ----------

Bucketing

Bucketing divides data into a fixed number of buckets, based on the hash value of a specified column. Unlike partitioning, bucketing evenly distributes data across a specified number of buckets, which can lead to more balanced and efficient joins and aggregations.

When you bucket a table, Spark stores the data in a way that optimizes joins on the bucketed column. For instance, if you bucket two tables by the same column, joining them on that column will be more efficient.

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("BucketingExample").getOrCreate()

# Example data
data = [("Alice", "Engineering", 34), ("Bob", "HR", 45), ("Catherine", "Finance", 29), ("David", "Engineering", 42)]
columns = ["Name", "Department", "Age"]
df = spark.createDataFrame(data, columns)

# Write the DataFrame bucketed by the "Name" column into 3 buckets
output_path = "/path/to/output/bucketed_data"
df.write.bucketBy(3, "Name").saveAsTable("bucketed_table")

# Read the bucketed data
bucketed_df = spark.table("bucketed_table")
bucketed_df.show()


# COMMAND ----------

Differences and Use Cases
PartitionBy: Use when you frequently filter or query by specific columns. It improves read performance by reducing the amount of data read.
Bucketing: Use when you need to optimize joins and aggregations. It ensures an even distribution of data across buckets, which can lead to more efficient joins and reduce data shuffling.

# COMMAND ----------

Summary
PartitionBy organizes data into directory structures based on column values, optimizing query performance for specific filters.
Bucketing distributes data across a fixed number of buckets based on column values, optimizing join and aggregation operations.

# COMMAND ----------

Repartition and Coalesce in Spark
Repartition and coalesce are methods used to change the number of partitions in a DataFrame or RDD. They are useful for optimizing the distribution and parallelism of data processing tasks.



# COMMAND ----------

Repartition

Repartition is used to increase or decrease the number of partitions in a DataFrame or RDD. This method performs a full shuffle of the data, which means it redistributes data across all partitions evenly. It is useful when you need to increase parallelism or evenly distribute data.

# COMMAND ----------

# Example data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42), ("Eva", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Check the initial number of partitions
print("Initial number of partitions:", df.rdd.getNumPartitions())

# Repartition the DataFrame into 3 partitions
df_repartitioned = df.repartition(3)

# Check the number of partitions after repartitioning
print("Number of partitions after repartitioning:", df_repartitioned.rdd.getNumPartitions())


# COMMAND ----------

Coalesce
Coalesce is used to decrease the number of partitions in a DataFrame or RDD. Unlike repartition, coalesce is more efficient because it avoids a full shuffle when reducing the number of partitions. It is useful when you need to reduce the number of partitions to optimize performance, such as when writing output to disk.

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CoalesceExample").getOrCreate()

# Example data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42), ("Eva", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Repartition the DataFrame into 4 partitions
df_repartitioned = df.repartition(4)

# Check the number of partitions after repartitioning
print("Number of partitions after repartitioning:", df_repartitioned.rdd.getNumPartitions())

# Coalesce the DataFrame into 2 partitions
df_coalesced = df_repartitioned.coalesce(2)

# Check the number of partitions after coalescing
print("Number of partitions after coalescing:", df_coalesced.rdd.getNumPartitions())


# COMMAND ----------

# MAGIC %md
# MAGIC Key Differences
# MAGIC
# MAGIC Repartition:
# MAGIC ***********
# MAGIC Performs a full shuffle of the data.
# MAGIC Can increase or decrease the number of partitions.
# MAGIC
# MAGIC Evenly distributes data across the new number of partitions.
# MAGIC
# MAGIC Useful for increasing parallelism or redistributing data.
# MAGIC
# MAGIC Coalesce:
# MAGIC ********
# MAGIC Avoids a full shuffle when reducing the number of partitions.
# MAGIC
# MAGIC Can only decrease the number of partitions.
# MAGIC
# MAGIC More efficient for reducing the number of partitions than repartition.
# MAGIC
# MAGIC Useful for optimizing performance when writing output or performing final aggregations.

# COMMAND ----------

Summary

Use repartition when you need to increase or decrease the number of partitions and ensure an even distribution of data.

Use coalesce when you need to efficiently reduce the number of partitions without a full shuffle.

# COMMAND ----------

Checkpointing in Spark
Checkpointing is a mechanism in Spark that helps ensure fault tolerance and recovery of RDDs (Resilient Distributed Datasets) by saving their intermediate results to reliable storage like HDFS (Hadoop Distributed File System). This process helps to cut off the lineage of RDDs, thereby making them less expensive to recompute.

Why Checkpointing?
Fault Tolerance: By saving RDDs to a reliable storage system, you can recover from node failures.
Cutting Off Lineage: When RDDs have long lineages due to multiple transformations, recomputing them can become very costly. Checkpointing breaks the lineage by saving intermediate results.
Optimization: Checkpointing can optimize complex Spark jobs by avoiding repetitive computation of the same intermediate results.
How to Use Checkpointing
Set the Checkpoint Directory: Define where the checkpointed data should be stored.
Checkpoint the RDD/DataFrame: Apply the checkpoint method to the RDD or DataFrame.

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CheckpointExample").getOrCreate()
sc = spark.sparkContext

# Set checkpoint directory
sc.setCheckpointDir("/path/to/checkpoint/dir")

# Example data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Convert DataFrame to RDD and apply transformations
rdd = df.rdd.map(lambda row: (row['Name'], row['Age'] + 10))

# Checkpoint the RDD
rdd.checkpoint()

# Trigger an action to materialize the checkpoint
rdd.count()

# Perform further actions
print(rdd.collect())


# COMMAND ----------

Fault Tolerance in Spark
Fault tolerance is a key feature of Apache Spark, enabling it to handle failures gracefully and continue processing without losing data. Spark achieves fault tolerance through several mechanisms:

RDD Lineage (DAG): Spark keeps track of the lineage of each RDD, which is the sequence of transformations that were applied to create it. If a partition of an RDD is lost, Spark can recompute it from the original data using this lineage information.

Checkpointing: As mentioned above, checkpointing stores RDDs to a reliable storage system, enabling faster recovery in case of failures.

Data Replication: In cluster managers like Hadoop YARN or Apache Mesos, data can be replicated across multiple nodes to ensure that it is not lost in case of node failures.

Task Resilience: Spark tasks are designed to be idempotent (can be retried without causing issues). If a task fails, Spark will retry it up to a configurable number of times before giving up.

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FaultToleranceExample").getOrCreate()

# Example data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Perform some transformations
df_transformed = df.withColumn("AgeAfter10Years", df["Age"] + 10)

# Action to trigger computation
df_transformed.show()

# Simulate a failure and recovery
# In a real cluster, this would involve node failure and automatic recovery by Spark
try:
    # Suppose an error happens here
    raise Exception("Simulated failure")
except Exception as e:
    print(f"Error occurred: {e}")

# Spark will automatically handle the failure and retry the task
df_transformed.show()


# COMMAND ----------

Summary

Checkpointing: Used to save intermediate results to reliable storage, ensuring fault tolerance and optimizing the recomputation process by cutting off lineage.

Fault Tolerance: Achieved through RDD lineage tracking, checkpointing, data replication, and resilient task design. Spark's architecture ensures that it can recover from failures and continue processing efficiently.

# COMMAND ----------

AQE - Adaptive query execution

• Dynamically changes sort merge join into broadcast hash join.
• Dynamically coalesces partitions (combine small partitions into reasonably sized partitions) after shuffle exchange. Very small tasks have worse I/O throughput and tend to suffer more from scheduling overhead and task setup overhead. Combining small tasks saves resources and improves cluster throughput.
• Dynamically handles skew in sort merge join and shuffle hash join by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks.
• Dynamically detects and propagates empty relations


# COMMAND ----------

Spark architecture  
 
When a notebook is submitted to the driver program, it creates the Spark Context/Spark Session and communicates with the cluster manager to get the number of worker nodes.
  
The Spark Context parallelly creates a Directed Acyclic Graph (DAG) list of transformations. 
when an action is executed, in driver node jobs are created and submitted to the cluster manager as stages, which are then divided into tasks. 

The data is split across tasks based on the partitioning algorithm, and each task is assigned to an executor for processing.  
If the transformation is wide then there will shuffling of data across the node. Once the task are completed, data is returned back to driver node. 

# COMMAND ----------

When a notebook is submitted to the driver program, it initiates the creation of the Spark Context or Spark Session, which then communicates with the cluster manager (like YARN, Mesos, or Kubernetes) to get the number of available worker nodes.

The Spark Context parallelly constructs a Directed Acyclic Graph (DAG) of transformations. The DAG represents the logical execution plan, where each node in the DAG represents a transformation, and edges represent dependencies between these transformations.

When an action is executed (such as collect, count, save, etc.), the DAG is translated into a physical execution plan consisting of stages. Each stage consists of tasks based on the partitioning of the data.

Here are the additional points to enhance the understanding:

Job and Stage Creation:

The driver splits the DAG into stages. Each stage contains a set of transformations that can be executed without shuffling the data.
Stages are divided at shuffle boundaries. Each stage contains tasks based on the partitions of the data.
Task Assignment:

Tasks within a stage are distributed to the executors (worker nodes) for parallel execution.
Each task processes a single data partition.
Wide vs. Narrow Transformations:

Narrow Transformations: Operations like map and filter that do not require shuffling of data between partitions. These operations can be executed within a single stage.
Wide Transformations: Operations like reduceByKey and join that require shuffling data across the network, creating dependencies between different stages.
Data Shuffling:

Shuffling involves redistributing data across the network. It occurs when data from one partition needs to be moved to another partition, typically seen in wide transformations.
Shuffling is an expensive operation in terms of time and resources, and Spark optimizes to minimize shuffling.
Fault Tolerance:

Spark ensures fault tolerance through RDD lineage. If a task fails, Spark can recompute only the lost partitions using the lineage information.
Checkpointing can be used to save the state of an RDD to a reliable storage to break the lineage and optimize recomputation.
Execution and Result Collection:

Once all tasks in a stage are completed, the results are shuffled, combined, and moved to the next stage or returned to the driver node.
The driver collects the results and executes any subsequent actions or transformations.
