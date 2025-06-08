---------------------------------
-- DATA SEGMENTATION
-- [Measure] By [Measure]
-- Total Products By Sales Range
-- Total Customers By Age
---------------------------------

/* Segment products into cost ranges and count how many products fall into each segment */

WITH product_segments AS
(
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 600 AND 1000 THEN '100-1000'
			ELSE 'Above 1000'
		END AS cost_range
	FROM
		gold.dim_products
)

SELECT
	cost_range,
	COUNT(product_key) AS total_products
FROM
	product_segments
GROUP BY
	cost_range
ORDER BY
	total_products DESC;

/* Group customers into three segments based on their spending behaviour */

WITH customer_spending AS
(
	SELECT
		c.customer_key,
		SUM(f.sales_amount) AS total_spending,
		MIN(f.order_date) AS first_order,
		MAX(f.order_date) AS last_order,
		EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12 +
		EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date))) AS lifespan_months
	FROM
		gold.fact_sales f
			LEFT JOIN
		gold.dim_customers c ON f.customer_key = c.customer_key
	GROUP BY
		c.customer_key
),

customer_segments AS
(
	SELECT
		customer_key,
		total_spending,
		lifespan_months,
		CASE
			WHEN lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
			WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_segment
	FROM
		customer_spending
)

SELECT
	customer_segment,
	COUNT(customer_key) AS total_customers
FROM
	customer_segments
GROUP BY
	customer_segment
ORDER BY
	total_customers DESC;