{#
    Override dbt's default schema-naming behaviour.

    By default, dbt prefixes any custom +schema config with the target's
    default schema (e.g. +schema: bronze becomes the physical schema
    "public_bronze"). Our sources.yml declares sources with a literal
    schema: bronze — sources are NOT affected by this macro, only models
    and seeds are — so without this override, seeded/built data lands in
    "public_bronze" while source() queries look for literal "bronze",
    causing a "relation does not exist" error even though everything
    upstream succeeded.

    This makes +schema configs (bronze / silver / gold) map to those exact
    schema names, matching sources.yml and the medallion architecture
    documented in scripts/init_db.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
