-- Test: Verify 6-month users with high projected engagement are classified correctly
-- Validates the enhanced 6-month user classification thresholds
with six_month_classification as (
    select
        user_id,
        user_age_in_months,
        projected_months_cut,
        t1, t2, t3, t4, t5, t6,
        monthly_classification,
        case
            when projected_months_cut >= 8 
                and (coalesce(t1,0) + coalesce(t2,0) + coalesce(t3,0) >= 1) 
                and (coalesce(t4,0) + coalesce(t5,0) + coalesce(t6,0) >= 1) 
                then 'M'
            when projected_months_cut >= 6 
                and (coalesce(t1,0) + coalesce(t2,0) + coalesce(t3,0) >= 2) 
                then 'Q'
            when (coalesce(t1,0) + coalesce(t2,0) + coalesce(t3,0) >= 1) 
                and (coalesce(t4,0) + coalesce(t5,0) + coalesce(t6,0) >= 1) 
                then 'Q'
            when (coalesce(t1,0) + coalesce(t2,0) + coalesce(t3,0) >= 2) 
                then 'R'
            when (coalesce(t1,0) + coalesce(t2,0) + coalesce(t3,0) + coalesce(t4,0) + coalesce(t5,0) + coalesce(t6,0) >= 1) 
                then 'R'
            when (coalesce(t1,0) + coalesce(t2,0) + coalesce(t3,0) + coalesce(t4,0) + coalesce(t5,0) + coalesce(t6,0) = 0) 
                then 'N'
            else 'Unclassified'
        end as expected_classification
    from {{ ref('engagement_cohorts') }}
    where user_age_in_months = 6
)

select 
    user_id,
    user_age_in_months,
    projected_months_cut,
    t1, t2, t3, t4, t5, t6,
    monthly_classification,
    expected_classification
from six_month_classification
where monthly_classification != expected_classification

