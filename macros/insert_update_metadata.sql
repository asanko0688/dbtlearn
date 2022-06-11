{% macro insert_update_metadata (model_name,operation) %}


  {% if operation == 'INSERT' %}

    {%- call statement('get_new_run_id',fetch_result=True) -%}
      select to_number(ifnull(max(run_id),0)+1) from {{ source('ABC','JOB_RUN_HISTORY')}} where model_name='{{model_name}}'
    {%- endcall -%}

    {% if execute %}
      {%- set new_runid = load_result('get_new_run_id')['data'][0][0] -%}
    {% endif %}

    {% set query -%}
      insert into {{source('ABC','JOB_RUN_HISTORY')}} values ('{{new_runid}}','{{model_name}}',current_timestamp,null,'In Progress')
    {%- endset %}
    {% do run_query(query) %}

  {% elif operation == 'UPDATE' %}

    {%- call statement('get_last_run_id',fetch_result=True) -%}
      select to_number(ifnull(max(run_id),0)) from {{ source('ABC','JOB_RUN_HISTORY')}} where model_name='{{model_name}}' and STATUS='In Progress'
    {%- endcall -%}

    {% if execute %}
      {%- set curr_runid = load_result('get_last_run_id')['data'][0][0] -%}
    {% endif %}

    {% set query -%}
      update {{source('ABC','JOB_RUN_HISTORY')}} set END_TIME=CURRENT_TIMESTAMP, STATUS='COMPLETED' 
      where MODEL_NAME= '{{model_name}}' AND RUN_ID='{{curr_runid}}'
    {%- endset %}
    {% do run_query(query) %}

  {% endif%}
 


{% endmacro %}