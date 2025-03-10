SELECT
	-- generate sum from all ws dot seconds
	SUM(sub_query.ws * sub_query.seconds)
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
		) as seconds
	FROM mystrom_coffee_report
	-- filter for day
	WHERE created_at > '2025-02-15' AND created_at < '2025-02-17'
) sub_query;