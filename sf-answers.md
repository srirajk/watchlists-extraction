## 1. Snowflake Performance Optimization - Large Table Joins

To optimize the join query:

1. Use Clustering on customer_id
   - Create a clustering key on customers(customer_id). This helps optimize the pruning of micro-partitions.

   ```sql
   ALTER TABLE customers CLUSTER BY (customer_id);
   ```

2. Materialized Views or Aggregate Tables
    - If this query is frequently run, pre-aggregating data can help:

   ```sql
   CREATE MATERIALIZED VIEW total_orders_by_country AS
   SELECT c.country, SUM(o.amount) AS total_amount
   FROM orders o
   JOIN customers c ON o.customer_id = c.customer_id
   GROUP BY c.country;
   ```

3. Use Warehouse Auto-Scaling
    - Make sure your warehouse is set to auto-scale to distribute load better:

   ```sql
   ALTER WAREHOUSE my_wh SET AUTO_SUSPEND = 60, AUTO_RESUME = TRUE, SCALING_POLICY = 'STANDARD';
   ```

## 2. Snowflake Clustering and Micro-Partitioning

1. Clustering Keys
    - Since queries filter on region and sale_date, define a clustering key:

   ```sql
   ALTER TABLE sales_data CLUSTER BY (region, sale_date);
   ```

2. Micro-Partitioning Impact
    - Snowflake automatically partitions data, but clustering keys help Snowflake prune unnecessary partitions, improving performance.

## 3. Snowflake Caching and Result Reuse

1. Query Result Caching
    - Snowflake automatically caches query results for 24 hours if:
        - The query is identical.
        - The underlying data has not changed.
        - The same virtual warehouse is used.

2. Optimization Strategies
    - Use materialized views to cache query results.
    - Store pre-aggregated data in a separate table for faster lookups.

## 4. Snowflake Data Loading Best Practices

1. Flatten JSON Data
    - Use the FLATTEN function to extract nested fields:

   ```sql
   SELECT 
     value:id::STRING AS user_id,
     value:event_type::STRING AS event_type,
     value:metadata::STRING AS metadata
   FROM raw_events, LATERAL FLATTEN(input => raw_events.json_column);
   ```

2. Use Variant Data Type for JSON
    - Instead of storing raw JSON as text, use VARIANT for better performance:

   ```sql
   CREATE TABLE raw_events (
     event_time TIMESTAMP,
     event_data VARIANT
   );
   ```

## 5. Snowflake Query Performance and Query Profiling

### Optimizing Query Performance:

1. Use Query Profiling
   - Run the query and check bytes scanned, partitions scanned, and spillage in Query Profile.

   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
   WHERE QUERY_TEXT LIKE '%your_query%';
   ```

2. Reduce Bytes Scanned
    - Ensure clustering keys are used to optimize partition pruning.
    - Filter data efficiently using partition elimination.

3. Reduce Spillage to Local Disk
    - Spillage happens when Snowflake runs out of memory.
    - Fix this by increasing warehouse size or optimizing joins and aggregations.

4. Optimize Joins & Aggregations
    - Use CLUSTER BY on large datasets to optimize joins:

   ```sql
   ALTER TABLE big_table CLUSTER BY (join_column);
   ```

    - Consider materialized views for pre-aggregated data.

## 6. Snowflake Time Travel and Data Recovery

### Recovering a Deleted Table:

1. Using Time Travel
   If the table was deleted within the retention period (default 1-90 days, depending on edition):

   ```sql
   UNDROP TABLE customer_orders;
   ```

   Or, restore a previous state:

   ```sql
   CREATE TABLE customer_orders_restored AS
   SELECT * FROM customer_orders AT(TIMESTAMP => '2024-02-16 12:00:00');
   ```

2. If Time Travel Period Expired
    - Check if the table exists in Fail-Safe (available for 7 days on Enterprise edition).
    - Fail-Safe is not user-accessible; contact Snowflake Support for recovery.

## 7. Snowflake Data Sharing and Secure Views

### Secure Data Sharing Options:

1. Use Secure Views
    - Hide sensitive fields (e.g., email, phone_number) in a Secure View:

   ```sql
   CREATE SECURE VIEW shared_transactions AS
   SELECT customer_id, order_id, order_amount
   FROM transactions;
   ```

    - Secure Views ensure data masking and cannot be copied.

2. Use Secure Data Sharing
    - Create a data share to the partner company:

   ```sql
   CREATE SHARE customer_transactions_share;
   GRANT USAGE ON DATABASE mydb TO SHARE customer_transactions_share;
   GRANT SELECT ON shared_transactions TO SHARE customer_transactions_share;
   ```

    - Partner can query the shared data directly without data movement.

## 8. Snowflake External Tables vs. Internal Tables

### Key Differences:

| Feature           | Internal Table                   | External Table                        |
|-------------------|----------------------------------|---------------------------------------|
| Storage           | Snowflake Storage                | External (e.g., S3, Azure Blob)       |
| Query Performance | Faster                           | Slower (reads from external storage)  |
| Data Loading      | Requires copying data into Snowflake | No data movement required         |

### Creating an External Table for S3 Data:

1. Create an External Stage:

   ```sql
   CREATE STAGE my_s3_stage
   URL = 's3://my-bucket/data/'
   STORAGE_INTEGRATION = my_s3_integration;
   ```

2. Create the External Table:

   ```sql
   CREATE EXTERNAL TABLE ext_sales_data
   WITH LOCATION = @my_s3_stage
   FILE_FORMAT = (TYPE = PARQUET);
   ```

3. Query External Data:

   ```sql
   SELECT * FROM ext_sales_data;
   ```

## 9. Snowflake Streams and Change Data Capture (CDC)

### Using Streams for CDC in Snowflake:

1. Create a Stream on the Table:

   ```sql
   CREATE STREAM orders_stream ON TABLE orders;
   ```

2. Capture New & Changed Records:
    - The stream captures INSERTs, UPDATEs, DELETEs since the last query.
    - Query changes using:

   ```sql
   SELECT * FROM orders_stream;
   ```

3. Process and Merge Changes:
    - Apply changes to a target table using MERGE INTO:

   ```sql
   MERGE INTO orders_history AS target
   USING orders_stream AS source
   ON target.order_id = source.order_id
   WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN DELETE
   WHEN MATCHED THEN UPDATE SET amount = source.amount
   WHEN NOT MATCHED THEN INSERT (order_id, customer_id, amount, status) 
   VALUES (source.order_id, source.customer_id, source.amount, source.status);
   ```

4. Drop the Stream (if no longer needed):

   ```sql
   DROP STREAM orders_stream;
   ```