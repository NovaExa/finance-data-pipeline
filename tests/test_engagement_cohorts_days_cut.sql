-- Test: Verify days_cut_lifetime is cumulative and non-negative
-- days_cut_lifetime should be >= days_cut_this_month and >= 0
with days_cut_validation as (
    select
        user_id,
        first_day_of_evaluation_month,
        days_cut_lifetime,
        days_cut_this_month
    from {{ ref('engagement_cohorts') }}
    where days_cut_lifetime < 0
        or days_cut_this_month < 0
        or days_cut_lifetime < days_cut_this_month
)

select * from days_cut_validation

