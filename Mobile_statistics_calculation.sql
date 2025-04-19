-- Primary key assignment for the "performance" table
-------------------------------------------------------------------
ALTER TABLE performance ADD PRIMARY KEY ("DT", "EL_ID");
SELECT * FROM performance;

-- Primary key assignment for the "traffic" table
-------------------------------------------------------------------
ALTER TABLE traffic ADD PRIMARY KEY ("DT", "EL_ID");
SELECT * FROM traffic;

-- Primary key assignment for the "users" table
-------------------------------------------------------------------
ALTER TABLE users ADD PRIMARY KEY ("DT", "EL_ID");
SELECT * FROM users;

-- Primary key assignment for the "regions" table
-------------------------------------------------------------------
ALTER TABLE regions ADD PRIMARY KEY ("ADDRESS_ID");
SELECT * FROM regions;

-- Primary key and foreign key assignment for the "config" table
-------------------------------------------------------------------
ALTER TABLE config ADD PRIMARY KEY ("EL_ID");
ALTER TABLE config ADD CONSTRAINT fk_regions
FOREIGN KEY ("ADDRESS_ID") REFERENCES regions ("ADDRESS_ID");
SELECT * FROM config;



-- Division function
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION f_div(numerator DOUBLE PRECISION, denominator DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
AS $$
BEGIN
	IF denominator = 0 THEN
		RAISE WARNING '%/0. Returning 0 instead of performing division.', numerator;
		RETURN 0;
	ELSE 
		RETURN numerator/denominator;
	END IF;
END;
$$ LANGUAGE plpgsql;



--Hourly statistics view
-------------------------------------------------------------------
CREATE OR REPLACE VIEW v_hourly_stats AS
	WITH kpi AS (
		SELECT
			performance."DT",
			performance."EL_ID",
			f_div("N_AVAILABILITY", "D_AVAILABILITY") *100 AS "AVAILABILITY",
			f_div("N_THRP", "D_THRP") /1000 AS "THRP",
			f_div("N_UTILIZATION", "D_UTILIZATION") AS "UTILIZATION",
			"SIGNAL_STRENGTH",
			"CALL_DROP_RATE",
			"TRAFFIC_GB",
			"AVG_USERS_CONNECTED"
		FROM performance
		LEFT JOIN traffic ON performance."DT" = traffic."DT" AND performance."EL_ID" = traffic."EL_ID"
		LEFT JOIN users ON performance."DT" = users."DT" AND performance."EL_ID" = users."EL_ID"	
	),
	ranked_hours AS (
		SELECT
			"DT",
			"EL_ID",
			"UTILIZATION",
			ROW_NUMBER() OVER (PARTITION BY DATE("DT"), "EL_ID" ORDER BY "UTILIZATION") AS rank_hours
		FROM kpi
	)
	SELECT
		DATE(kpi."DT") AS date,
		EXTRACT(HOUR FROM kpi."DT") AS hour,
		kpi."EL_ID",
		ROUND("AVAILABILITY"::numeric,2) AS "AVAILABILITY",
		ROUND("THRP"::numeric,2) AS "THRP",
		ROUND(kpi."UTILIZATION"::numeric,2) AS "UTILIZATION",
		"SIGNAL_STRENGTH",
		"CALL_DROP_RATE",
		"TRAFFIC_GB",
		"AVG_USERS_CONNECTED",
		rank_hours <=8 AS business_hours,
		rank_hours =1 AS peak_hour
	FROM kpi
	LEFT JOIN ranked_hours ON kpi."DT" = ranked_hours."DT" AND kpi."EL_ID" = ranked_hours."EL_ID"
	ORDER BY "EL_ID", date, hour;



--Daily statistics view
-------------------------------------------------------------------
CREATE OR REPLACE VIEW v_day_stats AS
WITH agregation AS(
	SELECT
		DATE(performance."DT") AS DAY,
		performance."EL_ID",
		SUM("N_AVAILABILITY") AS "SUM_N_AVAILABILITY",
		SUM("D_AVAILABILITY") AS "SUM_D_AVAILABILITY",
		AVG("SIGNAL_STRENGTH") AS "AVG_SIGNAL_STRENGTH",
		AVG("CALL_DROP_RATE") AS "AVG_CALL_DROP_RATE",
		SUM("N_THRP") AS "SUM_N_THRP",
		SUM("D_THRP") AS "SUM_D_THRP",
		SUM("N_UTILIZATION") AS "SUM_N_UTILIZATION",
		SUM("D_UTILIZATION") AS "SUM_D_UTILIZATION",
		SUM("TRAFFIC_GB") AS "SUM_TRAFFIC_GB",
		AVG("AVG_USERS_CONNECTED") AS "AVG_AVG_USERS_CONNECTED"
	FROM performance
	LEFT JOIN  traffic ON performance."DT" = traffic."DT" AND performance."EL_ID" = traffic."EL_ID" 
	LEFT JOIN  users ON performance."DT" = users."DT" AND performance."EL_ID" = users."EL_ID" 
	GROUP BY DATE(performance."DT"), performance."EL_ID"
)
SELECT
	DAY,
	"EL_ID",
	f_div("SUM_N_AVAILABILITY", "SUM_D_AVAILABILITY") *100 AS "AVAILABILITY",
	f_div("SUM_N_THRP", "SUM_D_THRP") /1000 AS "THRP",
	f_div("SUM_N_UTILIZATION", "SUM_D_UTILIZATION") AS "UTILIZATION",
	"AVG_SIGNAL_STRENGTH",
	"AVG_CALL_DROP_RATE",
	"SUM_TRAFFIC_GB",
	"AVG_AVG_USERS_CONNECTED"
FROM agregation
ORDER BY DAY, "EL_ID";



-- Regional traffic distribution view
-------------------------------------------------------------------
CREATE OR REPLACE VIEW v_regional_traffic_distribution AS
WITH region_traffic AS(
	SELECT
	 v_day_stats.DAY,
	 regions."REGION",
	 SUM(v_day_stats."SUM_TRAFFIC_GB") AS total_region_traffic
	FROM v_day_stats
	LEFT JOIN config ON v_day_stats."EL_ID" = config."EL_ID"
	LEFT JOIN regions ON config."ADDRESS_ID" = regions."ADDRESS_ID"
	GROUP BY v_day_stats.DAY, regions."REGION"
)
SELECT
	DAY,
	"REGION",
	total_region_traffic,
	ROUND((f_div(total_region_traffic, SUM(total_region_traffic) OVER (PARTITION BY DAY)) * 100)::numeric, 2) AS traffic_perc
FROM region_traffic
ORDER BY DAY, "REGION";
	











