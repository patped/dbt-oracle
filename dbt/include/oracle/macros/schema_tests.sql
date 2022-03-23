
{% macro oracle__test_accepted_values(model, column_name, values, quote=True) %}

with all_values as (

    select distinct
        {{ column_name }} as value_field

    from {{ model.include(False, True, True) }}

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        {% for value in values -%}
            {% if quote -%}
            '{{ value }}'
            {%- else -%}
            {{ value }}
            {%- endif -%}
            {%- if not loop.last -%},{%- endif %}
        {%- endfor %}
    )
)

select count(*)
from validation_errors

{% endmacro %}

{% macro oracle__test_not_null(model, column_name) %}

select {{ column_name }}
from {{ model.include(False, True, True) }}
where {{ column_name }} is null

{% endmacro %}

{% macro oracle__test_relationships(model, column_name, to, field) %}

select count(*) as validation_errors
from (
    select {{ column_name }} as id from {{ model }}
) child
left join (
    select {{ field }} as id from {{ to }}
) parent on parent.id = child.id
where child.id is not null
  and parent.id is null

{% endmacro %}

