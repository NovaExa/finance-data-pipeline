-- Test: Verify 3-month users with zero engagement are classified as 'N'
-- This validates the new logic added for 3-month user classification
with three_month_users_zero_engagement as (
    select
        user_id,
        user_age_in_months,
        t1, t2, t3,
        monthly_classification
    from {{ ref('engagement_cohorts') }}
    where user_age_in_months = 3
        and (coalesce(t1, 0) + coalesce(t2, 0) + coalesce(t3, 0)) = 0
        and monthly_classification != 'N'
)

select * from three_month_users_zero_engagement

