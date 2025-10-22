-- Test: Verify user_age_in_months is calculated correctly
-- Should be at least 1 and registration date should be before evaluation date
with user_age_validation as (
    select
        user_id,
        first_cutting_machine_registration_month_start_date,
        first_day_of_evaluation_month,
        user_age_in_months
    from {{ ref('engagement_cohorts') }}
    where user_age_in_months < 1
        or first_cutting_machine_registration_month_start_date > first_day_of_evaluation_month
)

select * from user_age_validation

