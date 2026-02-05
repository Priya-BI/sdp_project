# Spark Declarative Pipelines

Lakeflow Spark Declarative Pipelines is a declarative framework for developing and running batch and streaming data pipelines in SQL and Python.

## Key components
1. **Flow**- A flow reads data from a source, applies user-defined processing logic, and writes the result into a target.
2. **Streaming table** - A streaming table is a form of Unity Catalog managed table that is also a streaming target for Lakeflow SDP. A streaming table can have one or more streaming flows (Append, AUTO CDC) written into it.
3. **Materialized views** - Unlike standard views, which recompute results on every query, materialized views cache the results and refreshes them on a specified interval.
4. **Sink** - A sink is a streaming target for a pipeline,can have one or more streaming flows (Append) written into it.
5. **Pipelines** - A pipeline is the unit of development and execution in Lakeflow Spark Declarative Pipelines. A pipeline can contain one or more flows, streaming tables, materialized views, and sinks


##  Focus of this project is on:
1. Declarative data modeling
2. Incremental processing using Auto CDC
3. Data Quality
4. Business-ready transformations

## Data Architecture ##

## Raw data (.csv file):
1. sales
2. product
3. customer
4. store

### Folder Structure 

1. `transformations` folder -- most of the relevant source code lives there.
Each layer is maintained in seperate folder
   - **Bronze** -Raw data with basic data quality rule.
   - **Silver** - Transformed ,standarized and applying required data quality checks.
   - **Gold**-  Designed fact and dimension tables using Kimball dimensional modeling concepts and applied business rules
1. `exploration` folder -- to test/explore your code.

2. `utilities` folder --folder contains reusable helper functions and shared logic

Pipeline editor:

![alt text](<Screenshot 2026-02-04 181744.png>)

Successfully runned pipeline :

![alt text](image.png)

For more tutorials and reference material, see https://learn.microsoft.com/azure/databricks/ldp.