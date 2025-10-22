-- Test: Verify 'Onboarding' classification is only for users aged 1-2 months
-- Users in onboarding should be in their first or second month
with onboarding_validation as (
    select
        user_id,
        user_age_in_months,
        monthly_classification
    from {{ ref('engagement_cohorts') }}
    where monthly_classification = 'Onboarding'
        and user_age_in_months not in (1, 2)
)

select * from onboarding_validation

