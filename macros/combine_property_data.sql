{%- macro combine_property_data() -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')()) }}
{%- endmacro -%}

{% macro default__combine_property_data() %}
    {% if not should_full_refresh() %}
        {# If incremental, then use static_incremental_days variable to find earliest shard to copy #}
        {%- set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int -%}
    {% else %}
        {# Otherwise use 'start_date' variable #}
        {%- set earliest_shard_to_retrieve = var('start_date')|int -%}
    {% endif %}

    {# 
        Multiple-project support

        If simple multi-property variable ([111111, 222222])
        the following will recreate the variable to match the multiple-project configuration
        [{'project': 'gcp_project_1', 'property_id': 111111},{'project': 'gcp_project_1', 'property_id': 222222}]

    #}
    {% if 'project' not in var('property_ids')[0] %}
        {% set property_dict_lst = [] %}
        {% for property_id in var('property_ids') %}
                {% set _ = property_dict_lst.append({'project': var('project'), 'property_id': property_id}) %}
            {% endfor %}
        {# Else we use the property specified by the configuration file #}
    {% else %}
        {% set property_dict_lst = var('property_ids') %}
    {% endif%}

    {# {% for property_id in var('property_ids') %} #}

    {# Will iterate over a list of the following format 
        [{'project': 'gcp_project_1', 'property_id': 111111},{'project': 'gcp_project_1', 'property_id': 222222}]
    #}
    {% for property_item in property_dict_lst %}
        {%- set property_id = property_item['property_id']|string -%}
        {%- set schema_name = "analytics_" + property_id|string -%}
        {# {%- set schema_name = "analytics_" + property_id|string -%} #}
        {%- set project_name = property_item['project']|string -%}

        {%- set combine_specified_property_data_query -%}
            create schema if not exists `{{project_name}}.{{var('combined_dataset')}}`;

            {# Copy intraday tables #}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_intraday_%', database=property_item['project']) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_intraday_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    create or replace table `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}` clone `{{property_item['project']}}.analytics_{{property_id}}.events_intraday_{{relation_suffix}}`;
                {%- endif -%}
            {% endfor %}

            {# Copy daily tables and drop old intraday table #}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='events_%', exclude='events_intraday_%', database=property_item['project']) -%}
            {% for relation in relations %}
                {%- set relation_suffix = relation.identifier|replace('events_', '') -%}
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    create or replace table `{{target.project}}.{{var('combined_dataset')}}.events_{{relation_suffix}}{{property_id}}` clone `{{property_item['project']}}.analytics_{{property_id}}.events_{{relation_suffix}}`;
                    drop table if exists `{{target.project}}.{{var('combined_dataset')}}.events_intraday_{{relation_suffix}}{{property_id}}`;
                {%- endif -%}
            {% endfor %}
        {%- endset -%}

        {% do run_query(combine_specified_property_data_query) %}

        {% if execute %}
            {{ log("Cloned from `" ~ property_item['project'] ~ ".analytics_" ~ property_id ~ ".events_*` to `" ~ target.project ~ "." ~ var('combined_dataset') ~ ".events_YYYYMMDD" ~ property_id ~ "`.", True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
