-- SCD2 implementation

-- This example demonstrates Type 2 Slowly Changing Dimensions in Hive.
-- Be sure to stage data in before starting (load_data.sh)
-- Be sure to run this script in HiveCli or Beeline

drop database if exists hbase_test cascade;
create database hbase_test;
use hbase_test;

-- Create the Hive managed table with a row_key column mapping to HBase Row Key for our contacts.
-- The Row Key is encoded as YYYYMMDD-<10-digit Contact Code with leading zeros>
-- For example: 19000101-0000000001, 20190225-0000000032.

-- The Hive managed table also track a start and end date
-- These are SCD2 required fields and nullable are not allowed(In HBase, null value not stored).

create table scd2_contacts_target(
  row_key string comment 'surrogate key and hbase row key',
  info_code int comment 'natural code',
  info_name string comment 'contact name',
  info_email string comment 'contact email',
  info_state string comment 'contact state',
  valid_from date comment 'valid start date',
  valid_to date comment 'valid end date')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:code,info:name,info:email,info:state,valid:from,valid:to")
TBLPROPERTIES("hbase.table.name" = "scd2_contacts_target");

-- Create an external table pointing to our initial data load (1000 records)
create external table scd2_contacts_initial_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/initial_stage';

-- Copy the initial load into the managed table. We hard code the valid_from dates to the beginning of 1900.
insert into scd2_contacts_target
select
  concat('19000101-', lpad(scd2_contacts_initial_stage.id, 10, '0')),
  scd2_contacts_initial_stage.id as info_code,
  scd2_contacts_initial_stage.name as info_name,
  scd2_contacts_initial_stage.email as info_email,
  scd2_contacts_initial_stage.state as info_state,
  cast('1900-01-01' as date),
  cast('9999-12-31' as date)
from scd2_contacts_initial_stage;

-- Create an external table pointing to our refreshed data load (1100 records)
create external table scd2_contacts_update_stage(id int, name string, email string, state string)
  row format delimited fields terminated by ',' stored as textfile
  location '/tmp/merge_data/update_stage';

-- set hive parameters
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint=false;
set hive.exec.parallel=true;

-- 1. Load New Records
-- Use left-outer-join to simulate not-equal-to-join
insert into scd2_contacts_target
select
  concat('19000101-', lpad(scd2_contacts_update_stage.id, 10, '0')) as row_key,
  scd2_contacts_update_stage.id as info_code,
  scd2_contacts_update_stage.name as info_name,
  scd2_contacts_update_stage.email as info_email,
  scd2_contacts_update_stage.state as info_state,
  cast('1900-01-01' as date) as valid_from,
  cast('9999-12-31' as date) as valid_to
from scd2_contacts_update_stage left outer join scd2_contacts_target
on (scd2_contacts_target.info_code = scd2_contacts_update_stage.id) 
where scd2_contacts_target.info_code is null;

-- 2. Mark Old Version Obsolete
insert into scd2_contacts_target
select
  concat(regexp_replace(scd2_contacts_target.valid_from, '-', ''), '-', lpad(scd2_contacts_target.info_code, 10, '0')) as row_key,
  scd2_contacts_target.info_code,
  scd2_contacts_target.info_name,
  scd2_contacts_target.info_email,
  scd2_contacts_target.info_state,
  scd2_contacts_target.valid_from,
  cast(current_date() as date) as valid_to
from scd2_contacts_update_stage, scd2_contacts_target
where scd2_contacts_target.info_code = scd2_contacts_update_stage.id
and scd2_contacts_target.valid_to = cast('9999-12-31' as date)
and (scd2_contacts_update_stage.name <> scd2_contacts_target.info_name or scd2_contacts_update_stage.email <> scd2_contacts_target.info_email or scd2_contacts_update_stage.state <> scd2_contacts_target.info_state)
;

-- 3. Add New Version
insert into scd2_contacts_target
select
  concat(regexp_replace(cast(current_date() as date), '-', ''), '-', lpad(scd2_contacts_target.info_code, 10, '0')) as row_key,
  scd2_contacts_update_stage.id as info_code,
  scd2_contacts_update_stage.name as info_name,
  scd2_contacts_update_stage.email as info_email,
  scd2_contacts_update_stage.state as info_state,
  cast(current_date() as date) as valid_from,
  cast('9999-12-31' as date) as valid_to
from scd2_contacts_update_stage, scd2_contacts_target
where scd2_contacts_target.info_code = scd2_contacts_update_stage.id
and scd2_contacts_target.valid_to = cast(current_date() as date)
and (scd2_contacts_update_stage.name <> scd2_contacts_target.info_name or scd2_contacts_update_stage.email <> scd2_contacts_target.info_email or scd2_contacts_update_stage.state <> scd2_contacts_target.info_state)
;

