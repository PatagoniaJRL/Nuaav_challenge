CREATE OR REPLACE TABLE GOLD.FACT_SALES AS
with final_cte as (
SELECT 
    -- transaction facts 
    T.transaction_id,
    T.order_id,
    T.order_date,
    T.amount,
    T.currency,
    
    -- customer data (dim attributes)
    T.full_name AS customer_name,
    T.email,
    C.loyalty_tier, -- Premier, VIP, Regular
    C.is_active AS customer_status,
    
    -- order data (sales context)
    O.order_status, -- Complete, Pending
    O.channel, -- Web, Mobile, etc.
    
    -- product data
    P.product_name,
    P.category,
    
    -- payment data (fact validation)
    PY.payment_method,
    PY.status,
    T.amount_quality_status,
    ROW_NUMBER() OVER (
            PARTITION BY T.order_id,T.order_date,T.full_name 
            order by T.transaction_id
        ) as row_num
    
FROM SILVER.silver_transactions_table T
LEFT JOIN SILVER.silver_customers C ON T.customer_id = C.customer_id
LEFT JOIN SILVER.silver_orders O    ON T.order_id = O.order_id
LEFT JOIN SILVER.silver_products_table p  ON T.sku = p.sku
LEFT JOIN SILVER.silver_payments PY ON T.Order_id = PY.Order_id
order by T.transaction_id
)

select 
* 
from 
final_cte
where 
row_num = 1


CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMERS AS
SELECT customer_id, full_name, email, loyalty_tier, is_active
FROM SILVER.silver_customers;

CREATE OR REPLACE TABLE GOLD.DIM_PRODUCTS AS
SELECT sku, product_name, category, unit_price,currency,is_active
FROM silver.silver_products_table;