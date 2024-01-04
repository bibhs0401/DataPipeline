WITH teams AS (
    SELECT * FROM {{ref("test")}}
),
final as(
    select *
    from teams
)

SELECT *
FROM final
