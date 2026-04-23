--silver stage where normalize and standarize data 

--silver stage where normalize and standarize data 

CREATE OR REPLACE TABLE silver_customers AS
-- Transformación para Cliente A (Tenía First y Last Name)

WITH de_duped AS (
    SELECT 
        customer_id,
        COALESCE(NULLIF(TRIM(full_name), ''), 'Unknown') as full_name,
        CASE 
            WHEN email LIKE '%@%.%' THEN LOWER(TRIM(email)) 
            ELSE NULL 
        END as clean_email,
        loyalty_tier,
        signup_source,
        --source_system,
        is_active,
        ingested_at,
        -- Particionamos por ID y ordenamos por la fecha de carga más reciente
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, email, full_name 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM (
        -- Aquí va tu UNION ALL de las tablas RAW
        SELECT customer_id, 
        first_name || ' ' || last_name as full_name, 
        email, 
        loyalty_tier,
        signup_source,
        is_active,
        --'CLIENT_A' as source_system, 
        ingested_at, 
        --count(*) as duplicates_count 
        FROM ingestion.raw_customerA_table 
        --group by customer_id,full_name, email,source_system,ingested_at 
        --having count(*)=1
        
        UNION ALL
        
        SELECT 
        customer_id, 
        customer_name as full_name, 
        email,
        segment as loyalty_tier,
        null as signup_source,
        is_active,
        --'CLIENT_C' as source_system, 
        ingested_at, 
        --count(*) as duplicates_count
        FROM ingestion.raw_customerc_table 
        --group by customer_id,full_name, email,source_system,ingested_at 
        --having count(*)=1
    )
)
SELECT 
    customer_id,
    full_name,
    clean_email,
    loyalty_tier,
    signup_source,
    is_active,
    --source_system,
    ingested_at,
    row_num
FROM de_duped
WHERE row_num = 1
and customer_id is not null
and is_active = 'true'
order by full_name;

--select * from silver_customers


CREATE OR REPLACE TABLE silver.silver_orders AS

WITH CLN_ORDERS AS (

SELECT 

ORDER_ID,
CUSTOMER_ID,
ORDER_DATE,
ORDER_STATUS,
CHANNEL,
ingested_at
FROM
(
SELECT 
ORDER_ID,
CUSTOMER_ID,
ORDER_DATE,
ORDER_STATUS,
CHANNEL,
ingested_at
FROM ingestion.raw_ordersA_table

UNION ALL

SELECT 
ORDER_ID,
CUSTOMER_ID,
ORDER_DATE,
ORDER_STATUS,
NULL AS CHANNEL,
ingested_at,
FROM ingestion.raw_ordersc_table
)

)

SELECT
ORDER_ID,
CUSTOMER_ID,
COALESCE(order_date, '1900-01-01'::DATE) as order_date, --SINCE THERE IS NO WAY TO GET THIS VAL
ORDER_STATUS,
CHANNEL,
ingested_at
FROM CLN_ORDERS
WHERE ORDER_ID IS NOT NULL
AND order_id NOT LIKE '---%' 
  AND order_id NOT LIKE '%END OF FILE%'
  AND order_id IS NOT NULL;
---------------------------------------

CREATE OR REPLACE TABLE silver.silver_products_table AS

select 
sku,
prodcut_name,
category,
ABS(unit_price) as unit_price,-- since this is product price all values hsould come as positive bnumbers
currency,
is_active,
file_name, 
ingested_at
from
ingestion.raw_productsA_table
where sku is not null
and lower(is_active) = 'true'



----- normlaize transacion from XML & json file --------
--select * from raw_transactions
CREATE OR REPLACE TABLE SILVER.silver_xlm_flatten AS
SELECT 
        file_name,
        -- get nested tags (TransactionID -> Order -> OrderDate -> Customer -> )
        COALESCE(
        XMLGET(VALUE, 'TransactionID' ):"$"::STRING, --get data from file1 since is using different tags
        GET(XMLGET(src, 'TransactionID'), '$')::STRING
        ) AS trans_id,
        coalesce(
        XMLGET(XMLGET(src,'Order'),'OrderID'):"$"::STRING, 
        XMLGET(XMLGET(VALUE,'Order'),'OrderID'):"$"::STRING
        ) as order_id,
        coalesce(
        XMLGET(XMLGET(src,'Order'),'OrderDate'):"$"::STRING, 
        XMLGET(XMLGET(VALUE,'Order'),'OrderDate'):"$"::STRING
        ) as order_DATE,
        coalesce(
        XMLGET(XMLGET(XMLGET(VALUE, 'Order'), 'Customer'),'CustomerID'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(src, 'Order'), 'Customer'),'CustomerID'):"$"::STRING)
        AS customer_id, 
        COALESCE(
        XMLGET(XMLGET(XMLGET(XMLGET(value, 'Order'), 'Customer'),'Name'),'FirstName'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(XMLGET(SRC, 'Order'), 'Customer'), 'Name'),'FirstName'):"$"::STRING)
        || ' ' ||
        COALESCE(
        XMLGET(XMLGET(XMLGET(XMLGET(value, 'Order'), 'Customer'),'Name'),'LastLastName'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(XMLGET(value, 'Order'), 'Customer'),'Name'),'LastName'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(XMLGET(SRC, 'Order'), 'Customer'), 'Name'),'LastName'):"$"::STRING)
        AS full_name,
        COALESCE(
        XMLGET(XMLGET(XMLGET(value, 'Order'), 'Customer'),'Email'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(SRC, 'Order'), 'Customer'), 'Email'):"$"::STRING)
        AS email,
        COALESCE(
        XMLGET(XMLGET(XMLGET(value, 'Items'), 'Item'),'SKU'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(SRC, 'Items'), 'Item'), 'SKU'):"$"::STRING)
        as SKU,
        COALESCE(
        XMLGET(XMLGET(XMLGET(value, 'Items'), 'Item'),'Description'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(SRC, 'Items'), 'Item'), 'Description'):"$"::STRING)
        as Description,
        ABS(TRY_CAST(COALESCE(
        XMLGET(XMLGET(XMLGET(value, 'Items'), 'Item'),'Quantity'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(SRC, 'Items'), 'Item'), 'Quantity'):"$"::STRING) AS INTEGER)
        )as quantity,
        ABS(TRY_CAST(COALESCE(
        XMLGET(XMLGET(XMLGET(value, 'Items'), 'Item'),'UnitPrice'):"$"::STRING,
        XMLGET(XMLGET(XMLGET(SRC, 'Items'), 'Item'), 'UnitPrice'):"$"::STRING) AS FLOAT))
        AS UNIT_PRICE,
        COALESCE(
        XMLGET(XMLGET(XMLGET(value, 'Items'), 'Item'),'UnitPrice'):"@currency"::STRING,
        XMLGET(XMLGET(XMLGET(SRC, 'Items'), 'Item'), 'UnitPrice'):"@currency"::STRING)
        as Currency,
        COALESCE(
        XMLGET(XMLGET(value, 'Payment'), 'Method'):"$"::STRING,
        XMLGET(XMLGET(src, 'Payment'), 'Method'):"$"::STRING)
        as Payment_method,
        TRY_CAST(COALESCE(
        XMLGET(XMLGET(value, 'Payment'), 'Amount'):"$"::STRING,
        XMLGET(XMLGET(src, 'Payment'), 'Amount'):"$"::STRING)AS FLOAT)
        AS AMOUNT,
        ingested_at
        from ingestion.raw_transactions_test,
        LATERAL FLATTEN(INPUT => src:"$")
        --WHERE VALUE:"@" IN ('SalesData', 'Transaction', 'Entry');
        


--- there are some values I casn add to fit those missing values 
create or replace TABLE SILVER.silver_transactions_table AS
with final_transaction as (
select 
COALESCE(
    NULLIF(TRIM(trans_id), ''), 
    CONCAT('TEMP-', COALESCE(order_id, 'UNK'), '-', UUID_STRING())
) AS transaction_id,
COALESCE(NULLIF(TRIM(order_id), ''), 'UNKNOWN_Order')  as Order_id,
COALESCE(TRY_CAST(NULLIF(TRIM(order_DATE), '') AS DATE), '1900-01-01'::DATE) AS order_date,
COALESCE(NULLIF(TRIM(customer_id), ''), 'UNKNOWN_CUSTID')  as customer_id,
COALESCE(NULLIF(TRIM(full_name), ''), 'UNKNOWN_NAME') as full_name,
COALESCE(NULLIF(TRIM(email), ''), 'ivalid_email')  as email,
COALESCE(NULLIF(TRIM(sku), ''), 'UNKNOWN_SKU')  as SKU,
COALESCE(NULLIF(TRIM(description), ''), 'no description')  as description,
quantity,
unit_price,
currency,
payment_method,
AMOUNT,
ingested_at,
CASE 
        WHEN amount IS NULL THEN 'ERROR: Missing Revenue'
        WHEN amount <= 0 THEN 'WARNING: Zero or Negative Value'
        ELSE 'OK'
    END as amount_quality_status,
ROW_NUMBER() OVER (
            PARTITION BY transaction_id,customer_id, email, full_name,sku,description,quantity,unit_price,currency,payment_method,amount
            ORDER BY ingested_at DESC
        ) as row_num
from SILVER_XLM_FLATTEN 
where trans_id is not null
)

select 
transaction_id,
order_id,
Order_date,
customer_id,
full_name,
REGEXP_REPLACE(
        REGEXP_REPLACE(LOWER(TRIM(email)), '\\.{2,}', '.'), 
        '@{2,}', 
        '@'
    ) as email,
SKU,
description,
Quantity,
Unit_price,
Currency,
Payment_method,
amount,
INGESTED_AT,
amount_quality_status,
Row_num
from final_transaction
where row_num = 1
order by transaction_id




--- create silver table for payments

create or replace table silver.silver_payments as
select 
payment_id,
order_id,
payment_method,
amount,
currency,
status
from 
ingestion.raw_paymentsC_table
where payment_id is not null

