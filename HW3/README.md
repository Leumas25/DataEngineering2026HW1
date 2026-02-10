CREATE OR REPLACE EXTERNAL TABLE `terraform-485119.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'parquet',
  uris = [ 'gs://terraform-485119-zoomcampp-kestra-demo-1/yellow_tripdata_2024-*.parquet']
);



SELECT * FROM `terraform-485119.zoomcamp.external_yellow_tripdata_2024`


CREATE OR REPLACE TABLE `terraform-485119.zoomcamp.yellow_tripdata_2024_non_partitioned` AS
SELECT * FROM `terraform-485119.zoomcamp.external_yellow_tripdata_2024`;


SELECT DISTINCT(PULocationID) FROM `terraform-485119.zoomcamp.yellow_tripdata_2024_non_partitioned`

SELECT * FROM `terraform-485119.zoomcamp.yellow_tripdata_2024_non_partitioned` WHERE fare_amount=0


CREATE OR REPLACE TABLE `terraform-485119.zoomcamp.yellow_tripdata_2024_partitioned_clustered`
PARTITION BY
  DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS 
SELECT * FROM `terraform-485119.zoomcamp.external_yellow_tripdata_2024`


SELECT DISTINCT (VendorID) FROM `terraform-485119.zoomcamp.yellow_tripdata_2024_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'

SELECT DISTINCT (VendorID) FROM `terraform-485119.zoomcamp.yellow_tripdata_2024_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'

SELECT COUNT(*) FROM `terraform-485119.zoomcamp.yellow_tripdata_2024_non_partitioned`