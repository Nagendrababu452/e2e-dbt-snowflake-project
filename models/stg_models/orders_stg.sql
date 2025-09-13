{# {{
    config(
        materialized = 'incremental',
        unique_key = 'orderid',
        tags = ['stg']
    )
}} #}

{%
    set metadata_variables = {'rsi_etlsourceid' : 'Rsi'}
%}
SELECT
    OrderID,
    OrderDate,
    CustomerID,
    EmployeeID,
    StoreID,
    Status AS StatusCD,
    CASE
        WHEN Status = '01' THEN 'In Progress'
        WHEN Status = '02' THEN 'Completed'
        WHEN Status = '03' THEN 'Cancelled'
        ELSE NULL
    END AS StatusDesc,
    CASE
        WHEN StoreID = 1000 THEN 'Online'
        ELSE 'In-store'
    END AS ORDER_CHANNEL,
    Updated_at,
    current_timestamp as dbt_updated_at,
    -- Audit columns
    '{{ metadata_variables.rsi_etlsourceid }}' as etlsourceid,
    '{{ target.user }}' as idcreateuser,
    '{{ target.user }}' as idlastupdateuser,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as datecreated,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as datemodified
FROM
    {{ source('landing', 'orders') }} 

{# {% if is_incremental() %}
    WHERE o.updated_at >= (select dateadd(day,-1,max(started_at)) from {{ ref('dbt_results') }}
        WHERE database_name = split('{{this}}','.')[0]::text AND schema_name = split('{{this}}','.')[1]::text AND
        name = split('{{this}}','.')[2]::text)
{% endif %} #}