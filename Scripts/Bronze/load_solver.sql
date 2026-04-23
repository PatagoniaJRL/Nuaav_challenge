-- this files was for detect anomalies in xml, csv and json files that have an issue
--for this scenario xml files has unclosed tags and make it very difficlt to work with for those case isolate and 
--notify the sender resposible to evaluate the file again


-- Forzamos la carga de los archivos problemáticos tratándolos como texto plano
COPY INTO raw_transactions_table (raw_payload, file_name)
FROM (
  SELECT 
    -- Intentamos parsear, y si falla, lo guardamos como string para no perderlo
    TRY_PARSE_XML($1), 
    METADATA$FILENAME 
  FROM @my_load_stage
)
FILES = ('ClientA_Transactions_7.xml', 'ClientA_Transactions_1.xml') -- Los nombres exactos de los fallidos
FILE_FORMAT = (
    TYPE = 'CSV' 
    FIELD_DELIMITER = NONE 
    RECORD_DELIMITER = NONE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
);

create or replace file format ff_plain_text
    type = CSV 
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = NONE
    TRIM_SPACE = TRUE;
    --  [ formatTypeOptions ]
    -- comment = '<comment>'

---isssues in different xml files due to unclosed tags or parsing xml errors 
-- for this two files I decided to worked as separate, this may works for those files always comes with issues

--load this data into a temp table 

CREATE OR REPLACE TABLE fix_xml_raw(
content STRING,
file_name STRING
);

COPY INTO fix_xml_raw (content, file_name)
FROM (
  SELECT 
    $1,                -- Todo el contenido del archivo va a 'content'
    METADATA$FILENAME  -- El nombre del archivo va a 'file_name'
  FROM @my_load_stage
)
FILES = ('ClientA_Transactions_1.xml', 'ClientA_Transactions_7.xml')
FILE_FORMAT = (
    TYPE = 'CSV' 
    FIELD_DELIMITER = NONE 
    RECORD_DELIMITER = NONE 
    -- Agregamos esto para que no se queje si el archivo es extraño
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';

-----identify what is happening and why loading is not complete from the files ('ClientA_Transactions_1.xml', 'ClientA_Transactions_7.xml')
SELECT 
    PARSE_XML(
        SUBSTR(
            content, 
            CHARINDEX('<', content), -- Empezamos en el primer '<'
            -- Calculamos el largo desde el primer '<' hasta el último '>'
            REGEXP_INSTR(content, '>') - CHARINDEX('<', content) + 1
        )
    ) as fixed_xml,
    file_name
FROM fix_xml_raw
WHERE fixed_xml IS NOT NULL;