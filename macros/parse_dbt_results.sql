{% macro parse_dbt_results(results) %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    {%- set ex_tms = [] %}
    -- Flatten results and add to list
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}
        {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}

        {% for i in run_result_dict.get('timing') %}
            {% if i['name']  == 'execute' %}
                {% do ex_tms.append(i['started_at']) %}
                {% do ex_tms.append(i['completed_at']) %}
            {%endif%}
        {% endfor %}

        {% set parsed_result_dict = {
                'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'started_at' : ex_tms[0] ,
                'completed_at' : ex_tms[1],
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'name': node.get('name'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),
                'execution_time': run_result_dict.get('execution_time'),
                'rows_affected': rows_affected
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    
    {{ return(parsed_results) }}
{% endmacro %}