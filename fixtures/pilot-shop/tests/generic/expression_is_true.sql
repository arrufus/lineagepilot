{#- Fails for each row of the model where the expression does not hold. -#}
{% test expression_is_true(model, expression) %}

select *
from {{ model }}
where not ({{ expression }})

{% endtest %}
