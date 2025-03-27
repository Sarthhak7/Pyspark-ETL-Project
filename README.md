# **Apple Product Purchase Analysis - PySpark ETL Project** 🚀  

## 📌 **Project Overview**  
This project demonstrates an **end-to-end Data Engineering Pipeline using PySpark** on Databricks.  
The objective is to process and analyze **Apple product purchases** using **CSV, Parquet and Delta tables**, applying transformations to derive insights.  
The final output is stored in **Databricks' DBFS** and **Delta tables**.  

---

## 📊 **Data Pipeline Workflow**  
The pipeline follows a standard **ETL (Extract, Transform, Load) process**:  

### **1️⃣ Extraction:**  
- Reads raw data from **CSV, Parquet and Delta tables**  

### **2️⃣ Transformation:**  
Processes data to identify:  
- **Customers who purchased AirPods immediately after buying an iPhone**  
- **Customers who bought only iPhones and AirPods, and nothing else**  

### **3️⃣ Loading:**  
- Stores the results in **DBFS (Databricks File System) & Delta tables**  

---

## 🚀 **Technologies Used**  
- **Apache Spark (PySpark)** - Data processing  
- **Databricks** - Development and orchestration  
- **Delta Lake** - Optimized storage  
- **DBFS (Databricks File System)** - Storing intermediate outputs


## 🛠 Challenges Faced & Solutions  

### 1️ **Issue: Window Functions Producing Incorrect Results**
**Problem:**  
When using the lead() window function to track purchases over time, the next_product_name column was not always correctly populated. Some customers who bought iPhones didn’t have AirPods as the next product in sequence. 

**Solution:**  
The issue happened because transactions were not always ordered correctly. To fix this, we explicitly specified orderBy("transaction_date") within the Window.partitionBy("customer_id").  
```python
windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
transformedDF = transactionInputDF.withColumn("Next_Product_Name", lead("product_name").over(windowSpec))

### 2 **Issue: Writing Data to DBFS Created Multiple Small Files**
**Problem:**  
When writing transformed data to DBFS, the output was split into too many small files. This made querying inefficient and storage unoptimized.

**Solution:**  
Instead of writing without partitions, we partitioned the data by location to reduce small files and improve performance. 
```python
final_df.write.mode("overwrite").partitionBy("location").csv("dbfs:/FileStore/tables/apple_analysis/output")

### 3 **Issue: Reading Delta Table Was Failing in Extractor**
**Problem:**  
When reading from the Delta Table, the extractor function returned an error because it was treating the Delta table as a file path instead of a table reference.

**Solution:**  
Instead of using .load(), we used .table() when reading Delta tables in the reader_factory.py.
```python
transcatioInputDF = spark.read.table("default.customer_delta_table_persist")



## 🎯 Key Learnings & Takeaways  

✔ **How to build an end-to-end PySpark ETL pipeline**  
✔ **Implementing window functions & aggregations** for complex transformations  
✔ **Optimizing data storage with partitions** to improve query performance  
✔ **Working with Delta Tables & Databricks File System (DBFS)** for efficient data management  
✔ **Utilized broadcast joins** to improve join performance and reduce shuffle overhead in large datasets  
