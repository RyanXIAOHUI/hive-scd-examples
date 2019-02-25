# Managing Slowly Changing Dimensions Type2 (SCD2) with Apache Hive 1.1 and HBase 1.2 w/o HiveQL update operation

Forked from cartershanklin/hive-scd-examples, which provides sample datasets and scripts that demonstrate how to manage Slowly Changing Dimensions (SCDs) with Apache Hive's ACID MERGE capabilities, this project mainly focused on Apache 1.1 when there is no MERGE or UPDATE capabilities.

## Procedure

![SCD Strategies](SCDStrategies.png "SCD Strategies")

## Requirements

* [Apache Hive](https://hive.apache.org/) 1.1 or later
* [Apache HBase](https://hbase.apache.org/) 1.2 or later

## Instructions

* Clone this repository onto your Hadoop cluster
* Run load_data.sh to stage data into HDFS
* From Hive CLI or beeline, run `hive_type1_scd.sql`, `hive_type2_scd.sql` and `hive_type3_scd.sql`
