-- ============================================================================
-- Engagement Cohorts - Comprehensive Data Quality Tests
-- ============================================================================
-- This file contains all business logic and data quality validations for
-- the engagement_cohorts model. Each CTE represents a separate test.
-- All tests should return 0 rows to pass.
-- ============================================================================

-- TEST 1: 3-Month Users with Zero Engagement Should Be Classified as 'N'
-- Validates the new logic added for 3-month user classification
with test_three_month_zero_engagement as (
    select
        'test_three_month_zero_engagement' as test_name,
        user_id,
        user_age_in_months,
        t1, t2, t3,
        monthly_classification,
        'Expected N classification for 3-month users with zero engagement' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where user_age_in_months = 3
        and (coalesce(t1, 0) + coalesce(t2, 0) + coalesce(t3, 0)) = 0
        and monthly_classification != 'N'
),

-- TEST 2: Classification and Description Consistency
-- Ensures the description matches the classification code
test_classification_consistency as (
    select
        'test_classification_consistency' as test_name,
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
        'Classification description mismatch' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where monthly_classification_desc != case
            when monthly_classification = 'M' and days_cut_this_month >= 5 then 'Monthly Power'
            when monthly_classification = 'M' and days_cut_this_month < 5 then 'Monthly'
            when monthly_classification = 'Q' then 'Quarterly'
            when monthly_classification = 'R' then 'Occasional'
            when monthly_classification = 'N' then 'Non Engaged'
            when monthly_classification = 'Never Engaged' then 'Never Engaged'
            when monthly_classification = 'Onboarding' then 'Onboarding'
            else 'Unclassified'
        end
),

-- TEST 3: User Age Calculation Validation
-- Should be at least 1 and registration date should be before evaluation date
test_user_age_validation as (
    select
        'test_user_age_validation' as test_name,
        user_id,
        first_cutting_machine_registration_month_start_date,
        first_day_of_evaluation_month,
        user_age_in_months,
        'Invalid user age or date ordering' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where user_age_in_months < 1
        or first_cutting_machine_registration_month_start_date > first_day_of_evaluation_month
),

-- TEST 4: Days Cut Validation
-- days_cut_lifetime should be >= days_cut_this_month and both >= 0
test_days_cut_validation as (
    select
        'test_days_cut_validation' as test_name,
        user_id,
        first_day_of_evaluation_month,
        days_cut_lifetime,
        days_cut_this_month,
        'Invalid days_cut values (negative or lifetime < current month)' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where days_cut_lifetime < 0
        or days_cut_this_month < 0
        or days_cut_lifetime < days_cut_this_month
),

-- TEST 5: Never Engaged Classification Validation
-- Users classified as 'Never Engaged' should have days_cut_lifetime = 0 and age >= 12 months
test_never_engaged_validation as (
    select
        'test_never_engaged_validation' as test_name,
        user_id,
        user_age_in_months,
        days_cut_lifetime,
        monthly_classification,
        'Never Engaged users should have zero lifetime cuts and age >= 12 months' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where monthly_classification = 'Never Engaged'
        and (days_cut_lifetime != 0 or user_age_in_months < 12)
),

-- TEST 6: Onboarding Classification Validation
-- Users in onboarding should be in their first or second month only
test_onboarding_validation as (
    select
        'test_onboarding_validation' as test_name,
        user_id,
        user_age_in_months,
        monthly_classification,
        'Onboarding classification should only apply to users aged 1-2 months' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where monthly_classification = 'Onboarding'
        and user_age_in_months not in (1, 2)
),

-- TEST 7: 6-Month User Classification Logic
-- Validates the enhanced 6-month user classification thresholds
test_six_month_classification as (
    select
        'test_six_month_classification' as test_name,
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
        end as expected_classification,
        '6-month classification logic mismatch' as failure_reason
    from {{ ref('engagement_cohorts') }}
    where user_age_in_months = 6
        and monthly_classification != case
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
        end
),

-- Combine all test results
all_test_failures as (
    select * from test_three_month_zero_engagement
    union all
    select test_name, user_id, monthly_classification, monthly_classification_desc, expected_desc, failure_reason 
    from test_classification_consistency
    union all
    select test_name, user_id, first_cutting_machine_registration_month_start_date::varchar, first_day_of_evaluation_month::varchar, user_age_in_months::varchar, failure_reason 
    from test_user_age_validation
    union all
    select test_name, user_id, first_day_of_evaluation_month::varchar, days_cut_lifetime::varchar, days_cut_this_month::varchar, failure_reason 
    from test_days_cut_validation
    union all
    select test_name, user_id, user_age_in_months::varchar, days_cut_lifetime::varchar, monthly_classification, failure_reason 
    from test_never_engaged_validation
    union all
    select test_name, user_id, user_age_in_months::varchar, null::varchar, monthly_classification, failure_reason 
    from test_onboarding_validation
    union all
    select test_name, user_id, user_age_in_months::varchar, projected_months_cut::varchar, monthly_classification, failure_reason 
    from test_six_month_classification
)

-- Final output: should be empty if all tests pass
select 
    test_name,
    count(*) as failures
from all_test_failures
group by test_name

