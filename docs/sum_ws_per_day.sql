SELECT
	-- generate sum from all ws dot seconds
	SUM(sub_query.ws * sub_query.seconds) AS ws_dot_seconds,
	(SUM(sub_query.ws * sub_query.seconds) / 3600 / 1000) AS kw_h
FROM (
	SELECT
		-- use watt seconds
		ws,
		-- calculate difference between rows in seconds
		EXTRACT (
			-- extract seconds
			EPOCH FROM (
				created_at - LAG(created_at) OVER (ORDER BY created_at)
			)
		) as seconds,
		created_at
	FROM airflow_data_dev.mystrom_coffee_report
	-- filter for day
	WHERE created_at > '2025-04-07' AND created_at < '2025-04-08'
) sub_query;