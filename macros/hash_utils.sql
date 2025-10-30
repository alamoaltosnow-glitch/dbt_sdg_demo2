{% macro dv_hash(fields, separator='|', algorithm='sha2', length=256) %}
    {#-
        Data Vault Hash Generator
        --------------------------
        Genera un hash estable y limpio para claves o hashdiffs.

        Ejemplo de uso:
        {{ dv_hash(['c_custkey']) }}
        {{ dv_hash(['c_name', 'c_address', 'c_phone']) }}

        Parámetros:
        - fields: lista de campos
        - separator: separador entre valores (default: '|')
        - algorithm: sha2, md5, etc. (default: sha2)
        - length: bits del algoritmo (default: 256)
    -#}

    {% if fields is not iterable %}
        {% do exceptions.raise_compiler_error("El parámetro 'fields' debe ser una lista de campos.") %}
    {% endif %}

    {% set clean_fields = [] %}
    {% for field in fields %}
        {% do clean_fields.append("coalesce(cast(" ~ field ~ " as varchar), '')") %}
    {% endfor %}

    {{ return(algorithm ~ "(concat_ws('" ~ separator ~ "', " ~ clean_fields | join(', ') ~ "), " ~ length | string ~ ")") }}
{% endmacro %}