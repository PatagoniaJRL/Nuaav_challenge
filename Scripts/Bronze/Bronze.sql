LIST @my_load_stage; --- this will show that all the files are loaded into snowflake 

-- create an ingestion pipeline that read the xml files use VARIAN for dinamic schemas
create or replace table raw_transactions_test(src VARIANT,file_name STRING,
ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()) ;

COPY INTO raw_transactions_test(src,file_name)

FROM (
  SELECT $1, METADATA$FILENAME 
  FROM @my_load_stage
)
FILES = ('ClientA_Transactions_1.xml','ClientA_Transactions_2.xml','ClientA_Transactions_3.xml'
,'ClientA_Transactions_5.xml','ClientA_Transactions_6.xml','ClientA_Transactions_7.xml','ClientA_Transactions_4.txt')
FILE_FORMAT=(TYPE=XML) ON_ERROR='CONTINUE';

select * from raw_transactions_test

--select * from raw_transactions



--- load customers from CSV files

--LIST @my_load_stage;

--load data into customers
-- SO EACH CUSTOMER CSV FILE HAS THER OWN SCHEMA AND DIFFERENT HEADERS 
-- THE I CREATE DIFFERENT BRONZE TABLES FOR EACH FILE

COPY INTO raw_customerA_table (
    customer_id, 
    first_name, 
    last_name, 
    email, 
    loyalty_tier, 
    signup_source, 
    is_active, 
    file_name, 
    ingested_at
)
FROM (
  SELECT 
    $1, -- customer_id
    $2, -- first_name
    $3, -- last_name
    $4, -- email
    $5, -- loyalty_tier
    $6, -- signup_source
    $7, -- is_active
    METADATA$FILENAME, -- file name (Metadato real)
    CURRENT_TIMESTAMP() -- load date
  FROM @my_load_stage
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
FILES = ('Customer.csv')
ON_ERROR = 'CONTINUE';


-- load customer c data into their table 
COPY INTO raw_customerC_table (
    customer_id, 
    customer_name, 
    email, 
    segment,  
    is_active, 
    file_name, 
    ingested_at
)
FROM (
  SELECT 
    $1, -- customer_id
    $2, -- name
    $3, -- email
    $4, -- segment
    $5, -- is_active
    METADATA$FILENAME, -- file name (Metadato real)
    CURRENT_TIMESTAMP() -- load date
  FROM @my_load_stage
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
FILES = ('Customer.CSV')
ON_ERROR = 'CONTINUE';


/*select * from raw_customerA_table
select * from raw_customerC_table*/

/*DELETE FROM raw_customerC_table 
WHERE customer_id = 'customer_id' 
   OR customer_name = 'customer_name'
   OR email = 'email';*/

--truncate table raw_customerC_table

----LOAD ORDERS ------------------------------

create or replace TABLE raw_ordersA_table(
order_id string,
customer_id string,
order_date string,
order_status string,
channel string,
file_name STRING,
ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_ordersA_table (
    order_id,
    customer_id,
    order_date,
    order_status,
    channel,
    file_name, 
    ingested_at
)
FROM (
  SELECT 
    $1, -- order id
    $2, -- customerid
    $3, -- orderdate
    $4, -- orderstatus
    $5, -- channel
    METADATA$FILENAME, -- file name (Metadato real)
    CURRENT_TIMESTAMP() -- load date
  FROM @my_load_stage
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
FILES = ('Orders.csv')
ON_ERROR = 'CONTINUE';


--select * from raw_ordersA_table

create or replace TABLE raw_ordersC_table(
order_id string,
customer_id string,
order_date string,
order_status string,
file_name STRING,
ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_ordersC_table (
    order_id,
    customer_id,
    order_date,
    order_status,
    file_name, 
    ingested_at
)
FROM (
  SELECT 
    $1, -- order id
    $2, -- customerid
    $3, -- orderdate
    $4, -- orderstatus
    METADATA$FILENAME, -- file name (Metadato real)
    CURRENT_TIMESTAMP() -- load date
  FROM @my_load_stage
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
FILES = ('Order.csv')
ON_ERROR = 'CONTINUE';

--select * from raw_ordersC_table

-----------load products -------------------------------------

create or replace TABLE raw_productsA_table(
sku string,
prodcut_name string,
category string,
unit_price float,
currency string,
is_active string,
file_name STRING,
ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_productsA_table (
    sku,
    prodcut_name,
    category,
    unit_price,
    currency,
    is_active,
    file_name, 
    ingested_at
)
FROM (
  SELECT 
    $1, -- sku
    $2, -- product
    $3, -- category
    try_cast($4 as float), -- unit price
    $5, -- currency
    $6, -- is_active
    METADATA$FILENAME, -- file name (Metadato real)
    CURRENT_TIMESTAMP() -- load date
  FROM @my_load_stage

)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
FILES = ('Product.csv','Products.csv') -- Producto files has the same amount of columns
ON_ERROR = 'CONTINUE';

--select * from raw_productsA_table

---------LOAD PAYMENTS RAW DATA -------------------------------------------------

----load payments imagine are only for custoemr C

create or replace TABLE raw_paymentsC_table(
payment_id string,
order_id string,
payment_method string,
amount float,
currency string,
status string,
file_name STRING,
ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_paymentsC_table (
    payment_id,
    order_id,
    payment_method,
    amount,
    currency,
    status,
    file_name, 
    ingested_at
)
FROM (
  SELECT 
    $1, --  payment_id
    $2, -- order_id
    $3, -- payment_method
    try_cast($4 as float), -- amount
    $5, -- currency
    $6, -- status
    METADATA$FILENAME, -- file name (Metadato real)
    CURRENT_TIMESTAMP() -- load date
  FROM @my_load_stage

)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
FILES = ('Payments.csv')
ON_ERROR = 'CONTINUE';

select * from raw_paymentsC_table