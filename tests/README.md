# Engagement Cohorts Tests

This directory contains data quality and business logic tests for the `engagement_cohorts` model.

## Test Overview

### Schema Tests (in `models/marts/fact/schema.yml`)

**Column-level Tests:**
- `user_id`, `first_cutting_machine_registration_month_start_date`, `first_day_of_evaluation_month`, `user_age_in_months`, `dw_load_date`: NOT NULL validation
- `t1` through `t12`: Binary indicators validated to only contain 0 or 1
- `monthly_classification`: NOT NULL + accepted values (M, Q, R, N, Never Engaged, Onboarding, Unclassified)
- `monthly_classification_desc`: NOT NULL + accepted values (Monthly Power, Monthly, Quarterly, Occasional, Non Engaged, Never Engaged, Onboarding, Unclassified)
- `prev_days_cut_this_month`: NOT NULL validation
- `prev_monthly_classification_desc`: NOT NULL + accepted values

### Custom SQL Tests

#### 1. `test_engagement_cohorts.sql`
**Purpose:** Validates new 3-month user classification logic

**What it tests:**
- 3-month users with zero engagement (t1+t2+t3 = 0) should be classified as 'N' (Non Engaged)
- This is a new business rule added to better segment early-stage users

**Expected result:** No rows (test passes when empty)

---

#### 2. `test_engagement_cohorts_classification_consistency.sql`
**Purpose:** Ensures classification code and description are aligned

**What it tests:**
- `monthly_classification_desc` matches the expected description for each `monthly_classification` value
- Validates Monthly Power vs Monthly based on days_cut_this_month threshold (5 days)

**Expected result:** No rows (test passes when empty)

---

#### 3. `test_engagement_cohorts_user_age.sql`
**Purpose:** Validates user age calculation logic

**What it tests:**
- `user_age_in_months` is at least 1
- Registration date is before or equal to evaluation date

**Expected result:** No rows (test passes when empty)

---

#### 4. `test_engagement_cohorts_days_cut.sql`
**Purpose:** Validates cutting activity metrics

**What it tests:**
- `days_cut_lifetime` is non-negative
- `days_cut_this_month` is non-negative
- `days_cut_lifetime` is greater than or equal to `days_cut_this_month` (cumulative logic)

**Expected result:** No rows (test passes when empty)

---

#### 5. `test_engagement_cohorts_never_engaged.sql`
**Purpose:** Validates 'Never Engaged' classification

**What it tests:**
- Users classified as 'Never Engaged' have `days_cut_lifetime = 0`
- 'Never Engaged' classification only applies to users aged 12+ months

**Expected result:** No rows (test passes when empty)

---

#### 6. `test_engagement_cohorts_onboarding.sql`
**Purpose:** Validates 'Onboarding' classification

**What it tests:**
- Only users aged 1-2 months are classified as 'Onboarding'

**Expected result:** No rows (test passes when empty)

---

#### 7. `test_engagement_cohorts_six_month_classification.sql`
**Purpose:** Validates enhanced 6-month user classification logic

**What it tests:**
- 6-month users with `projected_months_cut >= 8` and engagement in both quarters → 'M'
- 6-month users with `projected_months_cut >= 6` and t1+t2+t3 >= 2 → 'Q'
- Additional Q and R classification rules for better segmentation

**Expected result:** No rows (test passes when empty)

---

## Running Tests

### Run all tests
```bash
dbt test
```

### Run tests for engagement_cohorts model only
```bash
dbt test --select engagement_cohorts
```

### Run specific test
```bash
dbt test --select test_engagement_cohorts
```

### Run schema tests only
```bash
dbt test --select engagement_cohorts,test_type:schema
```

### Run custom data tests only
```bash
dbt test --select engagement_cohorts,test_type:data
```

## Test Interpretation

- **Pass (0 rows):** The test query returned no violations - data quality check passed
- **Fail (n rows):** The test found n records that violate the business rule - investigate these records

## Recent Changes Validated

These tests cover the recent changes made to the engagement classification logic:

1. ✅ 3-month users with zero engagement now classified as 'N' instead of 'R'
2. ✅ Enhanced 6-month user classification with more granular thresholds
3. ✅ NOT NULL constraints added to key classification columns
4. ✅ DISTINCT clause added to final SELECT (prevents duplicate rows)

## Maintenance

When updating the engagement classification logic:
1. Update the corresponding test SQL to match new business rules
2. Add new tests for any additional edge cases
3. Update this README with test descriptions

