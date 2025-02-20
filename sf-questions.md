## 1. Snowflake Performance Optimization - Large Table Joins

### Scenario:
You are working with two large Snowflake tables:
- `orders` (500 million rows) with columns (order_id, customer_id, order_date, amount, status)
- `customers` (100 million rows) with columns (customer_id, name, country, created_at)

You frequently run the following query to get the total order amount per country:

```sql
SELECT c.country, SUM(o.amount) AS total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.country;
```

However, the query runs slowly despite having indexes and proper warehouse sizing.

### Question:
What optimizations would you apply in Snowflake to speed up this query?

## 2. Snowflake Clustering and Micro-Partitioning

### Scenario:
Your Snowflake table `sales_data` has 5 billion records with columns (sale_id, region, product_id, sale_amount, sale_date).
Users frequently query this table based on region and sale_date.

### Questions:
- How can you use clustering keys to improve query performance?
- How do micro-partitions impact query execution in Snowflake?

## 3. Snowflake Caching and Result Reuse

### Scenario:
A reporting dashboard frequently runs this query every 5 minutes:

```sql
SELECT region, SUM(sale_amount) AS total_sales
FROM sales_data
WHERE sale_date >= '2023-01-01'
GROUP BY region;
```

You notice that the query execution time is inconsistent—sometimes it runs in seconds, but other times it takes minutes.

### Questions:
- How does Snowflake's query caching work?
- What strategies would you use to optimize repeated queries in Snowflake?

## 4. Snowflake Data Loading Best Practices

### Scenario:
You are ingesting terabytes of JSON files into a Snowflake table (`raw_events`).
The JSON contains nested fields, and analysts are struggling to query the data efficiently.

### Questions:
- What is the best approach to flatten JSON data in Snowflake?
- How would you improve query performance on semi-structured data?

## 5. Snowflake Query Performance and Query Profiling

### Scenario:
You notice that a complex query running on a large table (10 billion rows) takes significantly longer than expected.
You run the query in the query profiler and see high spillage to local disk and high bytes scanned.

### Questions:
- How would you analyze and optimize the query?
- What does spillage to local disk indicate in Snowflake?

## 6. Snowflake Time Travel and Data Recovery

### Scenario:
A developer accidentally deleted a critical table (`customer_orders`).
The data loss was discovered 4 days after deletion, and you need to recover it.

### Questions:
- How would you recover the table using Time Travel?
- If the retention period has passed, what other options are available?

## 7. Snowflake Data Sharing and Secure Views

### Scenario:
Your company wants to share customer transaction data with a partner company, but you need to ensure that sensitive PII fields (like email and phone number) are not exposed.

### Questions:
- How would you securely share data in Snowflake?
- How do secure views work, and how would you implement them in this scenario?

## 8. Snowflake External Tables vs. Internal Tables

### Scenario:
You need to process terabytes of raw data stored in an S3 bucket and make it queryable in Snowflake, but without physically copying it into Snowflake storage.

### Questions:
- What is the difference between an internal table and an external table in Snowflake?
- How would you set up an external table for querying S3 data?

## 9. Snowflake Streams and Change Data Capture (CDC)

### Scenario:
A business intelligence team needs to track changes in a customer orders table (`orders`). They want to analyze only new and modified records every hour.

### Questions:
- How would you implement a Change Data Capture (CDC) process in Snowflake?
- How do Streams work in Snowflake for tracking changes?



