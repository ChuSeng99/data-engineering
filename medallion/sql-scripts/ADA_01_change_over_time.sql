---------------------------------
-- Change-Over-Time Analysis
-- [Measure] By [Date Dimension] 
---------------------------------

-- Analyze Sales Performance Over Time.

SELECT
	EXTRACT(YEAR FROM order_date) AS order_year,
	EXTRACT(MONTH FROM order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT customer_key) AS total_customers
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	EXTRACT(YEAR FROM order_date),
	EXTRACT(MONTH FROM order_date)
ORDER BY
	EXTRACT(YEAR FROM order_date),
	EXTRACT(MONTH FROM order_date);



SELECT
	DATE_TRUNC('MONTH', order_date) AS order_date,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT customer_key) AS total_customers
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	DATE_TRUNC('MONTH', order_date)
ORDER BY
	DATE_TRUNC('MONTH', order_date);



SELECT
	TO_CHAR(order_date, 'yyyy-mm') AS order_date,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT customer_key) AS total_customers
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	TO_CHAR(order_date, 'yyyy-mm') 
ORDER BY
	TO_CHAR(order_date, 'yyyy-mm');