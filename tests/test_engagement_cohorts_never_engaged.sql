-- Test: Verify 'Never Engaged' users have zero lifetime cuts
-- Users classified as 'Never Engaged' should have days_cut_lifetime = 0
with never_engaged_validation as (
    select
        user_id,
        user_age_in_months,
        days_cut_lifetime,
        monthly_classification
    from {{ ref('engagement_cohorts') }}
    where monthly_classification = 'Never Engaged'
        and (days_cut_lifetime != 0 or user_age_in_months < 12)
)

select * from never_engaged_validation

