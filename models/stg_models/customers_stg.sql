{# {{
    config(
        materialized = 'incremental',
        unique_key = 'CustomerID'
    )
}} #}

{%
    set metadata_variables = {'rsi_etlsourceid' : 'Rsi'}
%}

SELECT
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.Phone,
    c.Address,
    c.City,
    c.State,
    c.ZipCode,
    c.Updated_at,
    CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
    -- Audit columns
    '{{ metadata_variables.rsi_etlsourceid }}' as etlsourceid,
    '{{ target.user }}' as idcreateuser,
    '{{ target.user }}' as idlastupdateuser,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as datecreated,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as datemodified
FROM
    {{ source('landing', 'customers') }} c

{# {% if is_incremental() %}
    WHERE c.updated_at >= (select dateadd(day,-1,max(started_at)) from {{ ref('dbt_results') }}
        WHERE database_name = split('{{this}}','.')[0]::text AND schema_name = split('{{this}}','.')[1]::text AND
        name = split('{{this}}','.')[2]::text)
{% endif %} #}
    