{# {{
    config(
        materialized = 'incremental',
        unique_key = 'orderitemid',
        tags = ['stg']
    )
}} #}
{%
    set metadata_variables = {'rsi_etlsourceid' : 'Rsi'}
%}
SELECT
    OrderItemID,
    OrderID,
    ProductID,
    Quantity,
    UnitPrice,
    Quantity * UnitPrice AS TotalPrice,
    Updated_at,
    -- Audit columns
    '{{ metadata_variables.rsi_etlsourceid }}' as etlsourceid,
    '{{ target.user }}' as idcreateuser,
    '{{ target.user }}' as idlastupdateuser,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as datecreated,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as datemodified
FROM
    {{ source('landing', 'orderitems') }} 

{# {% if is_incremental() %}
    WHERE oi.updated_at >= (select dateadd(day,-1,max(started_at)) from {{ ref('dbt_results') }}
        WHERE database_name = split('{{this}}','.')[0]::text AND schema_name = split('{{this}}','.')[1]::text AND
        name = split('{{this}}','.')[2]::text)
{% endif %} #}