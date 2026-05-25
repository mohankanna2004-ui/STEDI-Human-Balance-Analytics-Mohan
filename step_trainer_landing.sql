CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_sample`.`step_trainer table` (
  `serialnumber` string,
  `sensorreadingtime` bigint,
  `distancefromobject` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-human-sample/step_trainer/'
TBLPROPERTIES ('classification' = 'json');