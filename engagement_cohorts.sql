CREATE TABLE IF NOT EXISTS prd_dw.fact.engagement_cohorts
(
    user_id                                             bigint encode az64 distkey,
    first_cutting_machine_registration_month_start_date date encode az64,
    first_day_of_evaluation_month                       date encode az64,
    user_age_in_months                                  bigint encode az64,
    days_cut_lifetime                                   bigint encode az64,
    days_cut_this_month                                 bigint encode az64,
    t1                                                  integer encode az64,
    t2                                                  integer encode az64,
    t3                                                  integer encode az64,
    t4                                                  integer encode az64,
    t5                                                  integer encode az64,
    t6                                                  integer encode az64,
    t7                                                  integer encode az64,
    t8                                                  integer encode az64,
    t9                                                  integer encode az64,
    t10                                                 integer encode az64,
    t11                                                 integer encode az64,
    t12                                                 integer encode az64,
    projected_months_cut                                numeric(6) encode az64,
    monthly_classification                              varchar(50),
    prev_monthly_classification                         varchar(50),
    prev_days_cut_this_month                            integer encode az64,
    monthly_classification_desc                         varchar(50),
    prev_monthly_classification_desc                    varchar(50),
    dw_load_date                                        timestamp
)
    diststyle key
    sortkey (first_day_of_evaluation_month)


;INSERT INTO prd_dw.fact.engagement_cohorts
(
	user_id
	,first_cutting_machine_registration_month_start_date
	,first_day_of_evaluation_month
	,user_age_in_months
	,days_cut_lifetime
	,days_cut_this_month
	,t1
	,t2
	,t3
	,t4
	,t5
	,t6
	,t7
	,t8
	,t9
	,t10
	,t11
	,t12
	,projected_months_cut
	,monthly_classification
	,prev_monthly_classification
	,prev_days_cut_this_month
	,monthly_classification_desc
	,prev_monthly_classification_desc
    ,dw_load_date
)
	WITH reg_data AS
	(
		SELECT mr.user_id
			 , DATE_TRUNC('month', MIN(mr.registration_date))::DATE AS first_cutting_machine_registration_month_start_date
		FROM prd_dw.fact.machine_registration mr
		LEFT JOIN dw.dim.machine m ON mr.serial_number = m.serial_number
        WHERE m.is_connected IS TRUE
		AND mr.registration_date IS NOT NULL
		group by 1
	)
	, cut_data AS
	(
		SELECT dd.first_day_of_month AS first_day_of_evaluation_month
		 , du.user_id
		 , CASE WHEN SUM(cuts_count_projects) > 0 THEN 1 ELSE 0 END AS did_cut_this_month
		 , SUM(CASE WHEN cuts_count_projects > 0 THEN 1 ELSE 0 END) AS days_cut_this_month
		FROM dw.dim.date AS dd
			 JOIN prd_dw.dim.user_profile AS du
				  ON dd.date_id >=  date_add('month',-1,date_trunc('month',du.created_date))::DATE
			 LEFT JOIN (SELECT user_id
							 , cut_session_start_date::date as evaluation_date_id
							 , COUNT(DISTINCT project_id)           as cuts_count_projects
						FROM prd_dw.fact.cut_session_master
						WHERE cut_session_start_date::date between '2019-01-01' and date_add('month',1,'2025-03-01')::DATE
						GROUP BY 1, 2) udm 
					   ON dd.date_id = udm.evaluation_date_id and du.user_id = udm.user_id
		WHERE first_day_of_month BETWEEN '2019-01-01' and date_add('month',1,'2025-03-01')::DATE
        GROUP BY 1,2
	)
	, user_current_evaluation AS
	(
		SELECT c.user_id
			, r.first_cutting_machine_registration_month_start_date
			, c.first_day_of_evaluation_month
			, DATEDIFF(months , r.first_cutting_machine_registration_month_start_date, c.first_day_of_evaluation_month) + 1 AS user_age_in_months
			, c.days_cut_this_month
			, SUM(c.days_cut_this_month) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS days_cut_lifetime
			, did_cut_this_month AS t1
			, LAG(did_cut_this_month, 1) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t2
			, LAG(did_cut_this_month, 2) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t3
			, LAG(did_cut_this_month, 3) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t4
			, LAG(did_cut_this_month, 4) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t5
			, LAG(did_cut_this_month, 5) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t6
			, LAG(did_cut_this_month, 6) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t7
			, LAG(did_cut_this_month, 7) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t8
			, LAG(did_cut_this_month, 8) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t9
			, LAG(did_cut_this_month, 9) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t10
			, LAG(did_cut_this_month, 10) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t11
			, LAG(did_cut_this_month, 11) OVER (PARTITION BY c.user_id ORDER BY first_day_of_evaluation_month) AS t12
			, CASE
				WHEN user_age_in_months >= 6 AND user_age_in_months < 12 THEN
																			ROUND((
																				(
																					nvl(t1,0)
																					+ nvl(t2,0)
																					+ nvl(t3,0)
																					+ nvl(t4,0)
																					+ nvl(t5,0)
																					+ nvl(t6,0)
																					+ nvl(t7,0)
																					+ nvl(t8,0)
																					+ nvl(t9,0)
																					+ nvl(t10,0)
																					+ nvl(t11,0)
																				)
																					* 12
																			)::NUMERIC(5,2) / user_age_in_months::NUMERIC(5, 2))
				ELSE 0
				END AS projected_months_cut
			, CASE
				WHEN user_age_in_months IN (1, 2) THEN 'Onboarding'
				WHEN user_age_in_months in (3, 4, 5) AND (t1 + t2 + t3 >= 2) THEN 'M'
				WHEN user_age_in_months in (3, 4, 5) AND (t1 + t2 + t3 = 1 ) THEN 'Q'
				WHEN user_age_in_months in (3, 4, 5) AND (t1 + t2 + t3 = 0 ) THEN 'R'
				when user_age_in_months = 6 and projected_months_cut >= 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1) then 'M'
				when user_age_in_months = 6 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1) then 'Q'
				when user_age_in_months = 6 and (t1 + t2 + t3 + t4 + t5 + t6 >= 1) then 'R'
				when user_age_in_months = 6 and (t1 + t2 + t3 + t4 + t5 + t6 = 0) then 'N'
				when user_age_in_months = 7 and projected_months_cut >= 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1) then 'M'
				when user_age_in_months = 7 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1) then 'Q'
				when user_age_in_months = 7 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 >= 1) then 'R'
				when user_age_in_months = 7 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 = 0) then 'N'
				when user_age_in_months = 8 and projected_months_cut >= 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1) then 'M'
				when user_age_in_months = 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1) then 'Q'
				when user_age_in_months = 8 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 >= 1) then 'R'
				when user_age_in_months = 8 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 = 0) then 'N'
				when user_age_in_months = 9 and projected_months_cut >= 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1)
																								  and (t7 + t8 + t9 >= 1) then 'M'
				when user_age_in_months = 9 and
											 (
												(
												case when (t1 + t2 + t3 >= 1) then 1 else 0 end
												)
												+
												(
												case when (t4 + t5 + t6 >= 1) then 1 else 0 end
												)
												+
												(
												case when (t7 + t8 + t9 >= 1) then 1 else 0 end
												)
												>= 2
											 ) then 'Q'
				when user_age_in_months = 9 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 >= 1) then 'R'
				when user_age_in_months = 9 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 = 0) then 'N'
				when user_age_in_months = 10 and projected_months_cut >= 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1)
																								  and (t7 + t8 + t9 >= 1) then 'M'
				when user_age_in_months = 10 and
											  (
												(
												case when (t1 + t2 + t3 >= 1) then 1 else 0 end
												)
												+
												(
												case when (t4 + t5 + t6 >= 1) then 1 else 0 end
												)
												+
												(
												case when (t7 + t8 + t9 >= 1) then 1 else 0 end
												)
												>= 2
											 ) then 'Q'
				when user_age_in_months = 10 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 >= 1) then 'R'
				when user_age_in_months = 10 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 = 0) then 'N'
				when user_age_in_months = 11 and projected_months_cut >= 8 and (t1 + t2 + t3 >= 1) and (t4 + t5 + t6 >= 1)
																								  and (t7 + t8 + t9 >= 1) then 'M'
				when user_age_in_months = 11 and
											  (
												(
												case when (t1 + t2 + t3 >= 1) then 1 else 0 end
												)
												+
												(
												case when (t4 + t5 + t6 >= 1) then 1 else 0 end
												)
												+
												(
												case when (t7 + t8 + t9 >= 1) then 1 else 0 end
												)
												>= 2
											 ) then 'Q'
				when user_age_in_months = 11 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 >= 1) then 'R'
				when user_age_in_months = 11 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 = 0) then 'N'
				WHEN user_age_in_months >= 12 and days_cut_lifetime = 0 then 'Never Engaged'
				WHEN user_age_in_months >= 12 AND (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 >= 8)
					AND (t1 + t2 + t3 >= 1)
					AND (t4 + t5 + t6 >= 1)
					AND (t7 + t8 + t9 >= 1)
					AND (t10 + t11 + t12 >= 1) THEN 'M'
				WHEN user_age_in_months >= 12
					 and (
						(CASE WHEN t1 + t2 + t3 >= 1 THEN 1 else 0 end) +
						(CASE WHEN t4 + t5 + t6 >= 1 THEN 1 else 0 end) +
						(CASE WHEN t7 + t8 + t9 >= 1 THEN 1 else 0 end) +
						(CASE WHEN t10 + t11 + t12 >= 1 THEN 1 else 0 end)
					 ) >= 3 THEN 'Q'
				WHEN user_age_in_months >= 12 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 >= 1) THEN 'R'
				WHEN user_age_in_months >= 12 and (t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 = 0) THEN 'N'
				else 'Unclassified'
				end as monthly_classification_cal
		FROM cut_data AS c
			JOIN reg_data AS r ON r.user_id = c.user_id
				AND c.first_day_of_evaluation_month >= r.first_cutting_machine_registration_month_start_date
	)
, user_previous_evaluation AS
(
	SELECT
		user_id
		, monthly_classification AS previous_monthly_classification
		, coalesce(monthly_classification_desc,'Unclassified') AS previous_monthly_classification_desc
		, days_cut_this_month as prev_days_cut_this_month
	FROM prd_dw.fact.engagement_cohorts
	WHERE first_day_of_evaluation_month = '2025-03-01'::DATE - INTERVAL '1 month'
)
, bucket_scores AS
(
	SELECT 'M' AS monthly_classification_score, 0 AS bucket_score
	UNION ALL
	SELECT 'Q' AS monthly_classification_score, 1 AS bucket_score
	UNION ALL
	SELECT 'R' AS monthly_classification_score, 2 AS bucket_score
	UNION ALL
	SELECT 'N' AS monthly_classification_score, 3 AS bucket_score
	UNION ALL
	SELECT 'Never Engaged' AS monthly_classification_score, 4 AS bucket_score
	UNION ALL
	SELECT 'Unclassified' AS monthly_classification_score, 5 AS bucket_score
)
SELECT DISTINCT
	a.user_id
	, a.first_cutting_machine_registration_month_start_date
	, a.first_day_of_evaluation_month
	, a.user_age_in_months
	, a.days_cut_lifetime
	, a.days_cut_this_month
	, a.t1
	, a.t2
	, a.t3
	, a.t4
	, a.t5
	, a.t6
	, a.t7
	, a.t8
	, a.t9
	, a.t10
	, a.t11
	, a.t12
	, a.projected_months_cut
	, CASE
		WHEN a.user_age_in_months IN (1, 2) THEN a.monthly_classification_cal
		WHEN a.user_age_in_months = 3 THEN a.monthly_classification_cal
		WHEN c.previous_monthly_classification IS NULL THEN a.monthly_classification_cal
		WHEN a.t1 = 1 AND b.bucket_score > d.bucket_score THEN c.previous_monthly_classification
		WHEN a.t1 = 1 AND b.bucket_score <= d.bucket_score THEN a.monthly_classification_cal
		WHEN a.t1 = 0 AND b.bucket_score < d.bucket_score THEN c.previous_monthly_classification
		WHEN a.t1 = 0 AND b.bucket_score >= d.bucket_score THEN a.monthly_classification_cal
		ELSE NULL
	END AS monthly_classification
	, c.previous_monthly_classification as prev_monthly_classification
	, c.prev_days_cut_this_month
	,CASE
		WHEN monthly_classification = 'M' AND a.days_cut_this_month >= 5 THEN 'Monthly Power'
		WHEN monthly_classification = 'M' AND a.days_cut_this_month < 5 THEN 'Monthly'
		WHEN monthly_classification = 'Q' THEN 'Quarterly'
		WHEN monthly_classification = 'R' THEN 'Occasional'
		WHEN monthly_classification = 'N' THEN 'Non Engaged'
		WHEN monthly_classification = 'Never Engaged' THEN 'Never Engaged'
		WHEN monthly_classification = 'Onboarding' THEN 'Onboarding'
		ELSE 'Unclassified'
	END AS monthly_classification_desc
	,coalesce(c.previous_monthly_classification_desc,'Unclassified') prev_monthly_classification_desc
    ,current_timestamp as dw_load_date
FROM user_current_evaluation a
LEFT JOIN bucket_scores b
	ON a.monthly_classification_cal = b.monthly_classification_score
LEFT JOIN user_previous_evaluation c
	ON a.user_id = c.user_id
LEFT JOIN bucket_scores d
	ON c.previous_monthly_classification = d.monthly_classification_score
WHERE a.first_day_of_evaluation_month = '2025-03-01'::DATE
