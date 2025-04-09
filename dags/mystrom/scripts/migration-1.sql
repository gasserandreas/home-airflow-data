-- update table to support price
ALTER TABLE public.mystrom_coffee_report ADD COLUMN price_per_kwh NUMERIC(5, 2);

-- calculate price based on created_at hours
UPDATE mystrom_coffee_report
SET price_per_kwh = CASE
    WHEN EXTRACT(HOUR FROM created_at) >= 0 AND EXTRACT(HOUR FROM created_at) < 7 THEN 26.14
    WHEN EXTRACT(HOUR FROM created_at) >= 7 AND EXTRACT(HOUR FROM created_at) < 20 THEN 31.54
    WHEN EXTRACT(HOUR FROM created_at) >= 20 AND EXTRACT(HOUR FROM created_at) <= 23 THEN 26.14
    ELSE NULL -- This should never happen if all hours are covered
END
WHERE price_per_kwh IS NULL;