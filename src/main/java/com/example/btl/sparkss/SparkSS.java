package com.example.btl.sparkss;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class SparkSS {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Streaming with Kafka")
                //.master("local")
                .config("spark.dynamicAllocation.enabled","false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ReadStream(spark);
    }

    private static void ReadStream(SparkSession spark) throws TimeoutException, StreamingQueryException {

        StructType sc = new StructType().add("id", "INT").
                add("res_id", "INT").
                add("utc_timestamp", "TIMESTAMP").
                add("DE_KN_residential4_dishwasher", "FLOAT").
                add("DE_KN_residential4_ev", "FLOAT").
                add("DE_KN_residential4_freezer", "FLOAT").
                add("DE_KN_residential4_grid_export", "FLOAT").
                add("DE_KN_residential4_grid_import", "FLOAT").
                add("DE_KN_residential4_heat_pump", "FLOAT").
                add("DE_KN_residential4_pv", "FLOAT").
                add("DE_KN_residential4_refrigerator", "FLOAT").
                add("DE_KN_residential4_washing_machine", "FLOAT")
                ;

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.17.80.26:9092")
                .option("subscribe", "anhlq36_electric")
                .option("group.id","group1")
                .option("startingOffsets","earliest")
                .option("auto.offset.reset","true")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> df1 = df.selectExpr("CAST (value AS STRING)");
        Dataset<Row> df2 = df1.withColumn("value", functions.from_json(df1.col("value"), sc ,new HashMap<>()));
        df2.printSchema();

        Dataset<Row> df4 = df2.select(df2.col("value.id"), df2.col("value.res_id"), df2.col("value.utc_timestamp"),
                df2.col("value.DE_KN_residential4_dishwasher"), df2.col("value.DE_KN_residential4_ev"), df2.col("value.DE_KN_residential4_freezer"),
                df2.col("value.DE_KN_residential4_grid_export"), df2.col("value.DE_KN_residential4_grid_import"),
                df2.col("value.DE_KN_residential4_heat_pump"), df2.col("value.DE_KN_residential4_pv"),
                df2.col("value.DE_KN_residential4_refrigerator"), df2.col("value.DE_KN_residential4_washing_machine"));

        df4.printSchema();

        Dataset<Row> df5 = df4.withColumnRenamed("utc_timestamp", "time").
                withColumnRenamed("DE_KN_residential4_dishwasher", "dishwasher").
                withColumnRenamed("DE_KN_residential4_ev", "ev").
                withColumnRenamed("DE_KN_residential4_freezer", "freezer").
                withColumnRenamed("DE_KN_residential4_grid_export", "grid_export").
                withColumnRenamed("DE_KN_residential4_grid_import", "grid_import").
                withColumnRenamed("DE_KN_residential4_heat_pump", "heat_pump").
                withColumnRenamed("DE_KN_residential4_pv", "pv").
                withColumnRenamed("DE_KN_residential4_refrigerator", "refrigerator").
                withColumnRenamed("DE_KN_residential4_washing_machine", "washing_machine");

        StreamingQuery query = df5
                .writeStream()
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "hdfs://172.17.80.21:9000/user/anhlq36/btl/checkpoint")//  hdfs://172.17.80.21:9000/user/anhlq36/btl/checkpoint
                .option("path", "hdfs://172.17.80.21:9000/user/anhlq36/btl/output")//hdfs://172.17.80.21:9000/user/anhlq36/btl/output_parquet
                .start();
        query.awaitTermination();
    }
}
