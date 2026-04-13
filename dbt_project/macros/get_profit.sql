{% macro get_profit(revenue_column, budget_column) %}
    -- Calcula o lucro, retornando NULL se algum campo for nulo
    CASE 
        WHEN {{ revenue_column }} IS NOT NULL AND {{ budget_column }} IS NOT NULL 
            THEN {{ revenue_column }} - {{ budget_column }}
        ELSE NULL 
    END
{% endmacro %}