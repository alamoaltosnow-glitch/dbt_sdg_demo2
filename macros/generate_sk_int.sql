{% macro generate_sk_int(columns, hex_len=15) %}
{#
  Generate a deterministic BIGINT surrogate key in Snowflake.
  - columns: list of columns (e.g. ['p_part_key'] or ['order_id','line_number'])
  - hex_len: how many hex chars to use from MD5 (default 15 -> safe for BIGINT)
#}

{%- set concat_parts = [] -%}
{%- for col in columns -%}
  {%- do concat_parts.append("COALESCE(CAST(" ~ col ~ " AS STRING), '')") -%}
{%- endfor -%}
{%- set expr = concat_parts | join(" || '|' || ") -%}

(
  CAST(
    ABS(
      MOD(
        (
          {%- set parts = [] -%}
          {%- for i in range(1, (hex_len + 1)) -%}
            {%- set pos = i - 0 -%}
            {%- set exp = hex_len - i -%}
            {%- if exp < 0 -%}
              {%- set exp = 0 -%}
            {%- endif -%}

            {# build CASE for the hex char at position i #}
            {%- set case_expr = (
               "CASE UPPER(SUBSTR(MD5(" ~ expr ~ "), " ~ pos|string ~ ", 1)) "
               ~ "WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2 WHEN '3' THEN 3 "
               ~ "WHEN '4' THEN 4 WHEN '5' THEN 5 WHEN '6' THEN 6 WHEN '7' THEN 7 "
               ~ "WHEN '8' THEN 8 WHEN '9' THEN 9 WHEN 'A' THEN 10 WHEN 'B' THEN 11 "
               ~ "WHEN 'C' THEN 12 WHEN 'D' THEN 13 WHEN 'E' THEN 14 WHEN 'F' THEN 15 "
               ~ "ELSE 0 END"
            ) -%}

            {# term = (case_expr) * POWER(16, exp) #}
            {%- set term = "(" ~ case_expr ~ " * POWER(16, " ~ exp|string ~ "))" -%}
            {%- do parts.append(term) -%}
          {%- endfor -%}

          {{ parts | join(" +\n          ") }}
        ),
        9223372036854775807
      )
    ) AS BIGINT
  )
)
{% endmacro %}