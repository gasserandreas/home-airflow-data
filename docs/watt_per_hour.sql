SELECT
	SUM(inner_q.ws_dot_seconds) as watt_per_hour,
	inner_q.date as date,
	date_trunc('hour', inner_q.time) as hour
FROM (
	SELECT
		ws * EXTRACT (
			-- extract seconds
			EPOCH FROM (
				created_at - LAG(created_at) OVER (ORDER BY created_at)
			)
		) as ws_dot_seconds,
		created_at::date as date,
		created_at::time as time
	FROM airflow_data_dev.mystrom_coffee_report
) AS inner_q
GROUP BY inner_q.date, date_trunc('hour', inner_q.time)
ORDER BY date DESC, hour DESC