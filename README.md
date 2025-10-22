# PR Review Demo - dbt Project

This dbt project contains data models for user engagement cohort analysis.

## Project Structure

```
pr_review_demo/
├── dbt_project.yml          # dbt project configuration
├── models/                  # Data models
│   └── marts/              # Business-level models
│       └── fact/           # Fact tables
│           ├── engagement_cohorts.sql
│           └── schema.yml
└── README.md
```

## Models

### Fact Models

#### `engagement_cohorts`
Tracks user engagement patterns based on cutting machine usage. Classifies users into engagement segments:
- **Monthly Power**: Active users cutting 5+ days per month
- **Monthly**: Active users cutting 1-4 days per month  
- **Quarterly**: Users cutting at least once per quarter
- **Occasional**: Infrequent users with some activity
- **Non Engaged**: Users with no recent activity
- **Never Engaged**: Users who have never used their machine
- **Onboarding**: New users in first 2 months

**Materialization**: Table  
**Distribution Key**: `user_id`  
**Sort Key**: `first_day_of_evaluation_month`

## Setup

1. Install dbt:
```bash
pip install dbt-redshift  # or dbt-snowflake, dbt-bigquery, etc.
```

2. Create a `profiles.yml` file in `~/.dbt/` directory:
```yaml
pr_review_demo:
  target: dev
  outputs:
    dev:
      type: redshift  # or your warehouse type
      host: your-cluster.region.redshift.amazonaws.com
      user: your_user
      password: your_password
      port: 5439
      dbname: your_database
      schema: prd_dw
      threads: 4
```

3. Test your connection:
```bash
dbt debug
```

4. Run the models:
```bash
dbt run
dbt test
```

## Key Features

- **Cohort Analysis**: Tracks users from first machine registration through lifecycle
- **Rolling Windows**: Uses 12-month rolling windows (t1-t12) for engagement tracking
- **Dynamic Classification**: Adjusts engagement buckets based on user age and activity patterns
- **State Persistence**: Maintains previous month's classification for trend analysis

## Dependencies

This model depends on the following source tables:
- `prd_dw.fact.machine_registration`
- `prd_dw.fact.cut_session_master`
- `prd_dw.dim.user_profile`
- `dw.dim.machine`
- `dw.dim.date`

## Notes

- The model is configured for Redshift with distribution and sort keys
- The evaluation date is currently hardcoded to '2025-03-01' - consider parameterizing this
- The `user_previous_evaluation` CTE references itself (self-referencing model)

