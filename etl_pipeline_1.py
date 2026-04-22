from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator
)

PROJECT_ID = "gcp-learning-492715"
DATASET = "airflow_practise"

with DAG(
    dag_id="bq_staging_target_pipeline_demo",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery","composer","demo"]
) as dag:

    # -------------------------
    # Task 1 Start
    # -------------------------
    start = EmptyOperator(
        task_id="start"
    )

    # -------------------------
    # Task 2 Create 2 staging tables
    # 1000+ rows ,12+ columns
    # -------------------------

    create_staging_tables = BigQueryInsertJobOperator(
        task_id="create_staging_tables",
        configuration={
            "query": {
                "query": f"""

-- Customer staging table
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.customers_stg` AS
SELECT
customer_id,
CONCAT('Customer_',customer_id) customer_name,
CASE
WHEN MOD(customer_id,3)=0 THEN 'Toronto'
WHEN MOD(customer_id,3)=1 THEN 'Vancouver'
ELSE 'Montreal'
END city,
CASE
WHEN MOD(customer_id,2)=0 THEN 'Premium'
ELSE 'Standard'
END customer_type,
20 + MOD(customer_id,30) age,
DATE_SUB(CURRENT_DATE(), INTERVAL MOD(customer_id,365) DAY) signup_date
FROM UNNEST(GENERATE_ARRAY(1,1000)) customer_id;


-- Orders staging table
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.orders_stg` AS
SELECT
order_id,
MOD(order_id,1000)+1 customer_id,
ROUND(100+RAND()*900,2) amount,
CASE
WHEN MOD(order_id,4)=0 THEN 'Electronics'
WHEN MOD(order_id,4)=1 THEN 'Clothing'
WHEN MOD(order_id,4)=2 THEN 'Grocery'
ELSE 'Books'
END product_category,
DATE_SUB(CURRENT_DATE(), INTERVAL MOD(order_id,365) DAY) order_date,
MOD(order_id,5)+1 quantity,
ROUND(RAND()*50,2) discount,
'Canada' country,
'Online' channel,
'Completed' order_status,
'Card' payment_method,
CURRENT_TIMESTAMP() load_ts
FROM UNNEST(GENERATE_ARRAY(1,5000)) order_id;

                """,
                "useLegacySql": False,
                "multiStatementTransaction": False
            }
        }
    )


    # -------------------------
    # Task 3 Transformation Target
    # -------------------------

    transform_to_target = BigQueryInsertJobOperator(
        task_id="transform_to_target",
        configuration={
            "query": {
                "query": f"""

CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.customer_sales_tgt` AS

SELECT
c.customer_id,
c.customer_name,
c.city,
c.customer_type,

COUNT(o.order_id) total_orders,

SUM(o.amount) gross_sales,

ROUND(
SUM(o.amount)-SUM(o.discount),2
) net_sales,

AVG(o.amount) avg_order_value,

CASE
WHEN SUM(o.amount) > 3000 THEN 'High Value'
WHEN SUM(o.amount) > 1500 THEN 'Medium Value'
ELSE 'Low Value'
END customer_segment,

RANK() OVER(
ORDER BY SUM(o.amount) DESC
) sales_rank,

MIN(o.order_date) first_order_date,
MAX(o.order_date) last_order_date,

CURRENT_TIMESTAMP() audit_ts

FROM
`{PROJECT_ID}.{DATASET}.customers_stg` c
LEFT JOIN
`{PROJECT_ID}.{DATASET}.orders_stg` o
ON c.customer_id=o.customer_id

GROUP BY
c.customer_id,
c.customer_name,
c.city,
c.customer_type;

                """,
                "useLegacySql": False
            }
        }
    )


    # -------------------------
    # Task 4 Count records check
    # Expect 1000 customers
    # -------------------------

    record_count_check = BigQueryValueCheckOperator(
        task_id="record_count_check",
        sql=f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{DATASET}.customer_sales_tgt`
        """,
        pass_value=1000,
        tolerance=0
    )


    # -------------------------
    # Task 5 End
    # -------------------------

    end = EmptyOperator(
        task_id="end"
    )



    start >> create_staging_tables >> transform_to_target >> record_count_check >> end