--------------------------------------------
-- Cumulative Analysis 
-- [Cumulative Measure] By [Date Dimension]
--------------------------------------------

-- Calculate the total sales per month and the running total of sales over time.

WITH t1 AS
(
	SELECT
		EXTRACT(YEAR FROM order_date) AS order_year,
		TO_CHAR(order_date, 'yyyy-mm') AS order_month,
		SUM(sales_amount) AS total_sales,
		ROUND(AVG(price)::NUMERIC, 2) AS avg_price
	FROM
		gold.fact_sales
	WHERE
		order_date IS NOT NULL
	GROUP BY
		EXTRACT(YEAR FROM order_date),
		TO_CHAR(order_date, 'yyyy-mm') 
)

SELECT
	order_month,
	total_sales,
	SUM(total_sales) OVER (
		PARTITION BY order_year 
		ORDER BY order_month 
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	) AS ytd_sales,
	avg_price,
	ROUND(AVG(avg_price) OVER (
		PARTITION BY order_year 
		ORDER BY order_month 
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	)::NUMERIC, 2) AS moving_avg_price
FROM
	t1
ORDER BY
	order_month;