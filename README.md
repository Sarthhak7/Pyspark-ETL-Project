# **Apple Analysis - PySpark ETL Project** 🚀  

## 📌 **Project Overview**  
This project demonstrates an **end-to-end Data Engineering Pipeline using PySpark** on Databricks.  
The objective is to process and analyze **Apple product purchases** using **CSV and Delta tables**, applying transformations to derive insights.  
The final output is stored in **Databricks' DBFS** and **Delta tables**.  

---

## 📊 **Workflow**  
The pipeline follows a standard **ETL (Extract, Transform, Load) process**:  

### **1️⃣ Extraction:**  
- Reads raw data from **CSV, Parquet, and Delta tables**  

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
