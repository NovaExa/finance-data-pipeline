-- Test: Verify monthly_classification and monthly_classification_desc are consistent
-- Ensures the description matches the classification code
with classification_mapping as (
    select
        user_id,
        monthly_classification,
        monthly_classification_desc,
        case
            when monthly_classification = 'M' and days_cut_this_month >= 5 then 'Monthly Power'
            when monthly_classification = 'M' and days_cut_this_month < 5 then 'Monthly'
            when monthly_classification = 'Q' then 'Quarterly'
            when monthly_classification = 'R' then 'Occasional'
            when monthly_classification = 'N' then 'Non Engaged'
            when monthly_classification = 'Never Engaged' then 'Never Engaged'
            when monthly_classification = 'Onboarding' then 'Onboarding'
            else 'Unclassified'
        end as expected_desc,
        days_cut_this_month
    from {{ ref('engagement_cohorts') }}
)

select 
    user_id,
    monthly_classification,
    monthly_classification_desc,
    expected_desc
from classification_mapping
where monthly_classification_desc != expected_desc

