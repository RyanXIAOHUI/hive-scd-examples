# Managing Slowly Changing Dimensions Type2(SCD2) with Apache Hive 1.1(w/o UPDATE operation) and HBase 1.2 

This project is forked from cartershanklin/hive-scd-examples, which provides sample datasets and scripts. 
This project is mainly focused on Apache 1.1, and demonstrates how to manage SCD2 in Apache Hive platform without MERGE or UPDATE capabilities.

## Procedure

![SCD Strategies](SCDStrategies.png "SCD Strategies")

## Requirements

* [Apache Hive](https://hive.apache.org/) 1.1
* [Apache HBase](https://hbase.apache.org/) 1.2

## Instructions

* Clone this repository onto your Hadoop cluster
* Run load_data.sh to stage data into HDFS
* From Hive CLI or beeline, run `hive_hbase_scd2.sql`

## Result

* Hive Result Demo
  (hive_hbase_scd2_hive_screenshot.png "Hive Result Demo")
* HBase Result Demo
  (hive_hbase_scd2_hbase_screenshot.png "HBase Result Demo")
* Each Row Key in HBase has at most 2 versions: 
  - initial load version, with 1999/01/01 as valid_from and 9999/12/31 as valid_to
  - obsolete version, with 1999/01/01 as valid_from and current_date as valid_to
