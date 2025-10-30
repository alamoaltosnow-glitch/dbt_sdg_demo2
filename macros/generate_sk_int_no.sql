{% macro generate_sk_int_no(columns) %}
    {#
        Generates a deterministic BIGINT surrogate key (integer)
        from one or more columns — fully compatible with Snowflake.
    #}

    {% set concat_expr = [] %}
    {% for col in columns %}
        {% do concat_expr.append("COALESCE(CAST(" ~ col ~ " AS STRING), '')") %}
    {% endfor %}
    {% set expr = concat_expr | join(" || '|' || ") %}

    CAST(
        ABS(
            MOD(
                TO_NUMBER(
                    SUBSTR(MD5({{ expr }}), 1, 15),
                    16
                ),
                9223372036854775807
            )
        ) AS BIGINT
    )
{% endmacro %}