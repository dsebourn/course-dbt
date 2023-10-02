{#
  Grants SELECT on a model to a set of roles if target is `production`.

  Arguments:
    new_grantees: Array of Snowflake roles that should be granted SELECT privileges
#}

{% macro grant_select_on_model_to_roles(new_grantees) %}
  {% if execute %} {# This check is necessary so the macro does not run on parsing steps #}
    {% for grantee in new_grantees %}
      {% if 'production' in target.name  %}
        grant usage on schema {{ this.schema|lower }} to role {{ grantee|lower }};
        grant select on table {{ this|lower }} to role {{ grantee|lower }};
      {% endif %}

      {#
        Log grant details to the dbt.log file.
        Prefix with [Dry-Run] if executing against a non-production target
        where the grants were not actually applied
      #}
      {% set is_test = ('production' not in target.name) %}
      {{ log(is_test * "[Dry-Run] " ~ "Granted SELECT on table " ~ this|lower ~ " to role " ~ grantee|lower, False) }}
      {{ log(is_test * "[Dry-Run] " ~ "Granted USAGE on schema " ~ this.schema|lower ~ " to role " ~ grantee|lower, False) }}
    {% endfor %}
  {% endif %}

{% endmacro %}