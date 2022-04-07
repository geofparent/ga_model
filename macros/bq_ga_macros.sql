{% macro tableRange() -%}

_table_suffix between '{{var("rangeStart")}}' and '{{var("rangeEnd")}}'

{%- endmacro %}

{% macro previousdaypull() -%}

_table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY))

{%- endmacro %}
