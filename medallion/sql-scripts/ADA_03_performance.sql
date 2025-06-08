--------------------------------------
-- Performance Analysis 
-- Current[Measure] - Target[Measure]
-- Current - Avg
-- Current - Previous
-- Current - Lowest/Highest
--------------------------------------

/* 
	Analyze the yearly performance of products by comparing each product's sales 
	to both its average sales performance and the previous year's sales. 
*/

WITH t1 AS
(
	SELECT
		EXTRACT(YEAR FROM order_date) AS order_year,
		product_key,
		SUM(sales_amount) AS yearly_sales
	FROM
		gold.fact_sales
	WHERE
		order_date IS NOT NULL
	GROUP BY
		EXTRACT(YEAR FROM order_date),
		product_key
)

SELECT
	*,
	ROUND(AVG(yearly_sales) OVER (PARTITION BY product_key)::NUMERIC, 0) AS avg_yearly_sales,
	yearly_sales - ROUND(AVG(yearly_sales) OVER (PARTITION BY product_key)::NUMERIC, 0) AS vs_avg,
	LAG(yearly_sales) OVER (PARTITION BY product_key ORDER BY order_year) AS prev_year_sales,
	yearly_sales - LAG(yearly_sales) OVER (PARTITION BY product_key ORDER BY order_year) AS vs_prev
FROM
	t1
ORDER BY 
	order_year DESC, 
	vs_prev DESC,
	vs_avg DESC;


-- SOLUTION
WITH yearly_product_sales AS
(
	SELECT
		EXTRACT(YEAR FROM f.order_date) AS order_year,
		p.product_name,
		SUM(f.sales_amount) AS current_sales
	FROM
		gold.fact_sales f
			LEFT JOIN
		gold.dim_products p ON f.product_key = p.product_key
	WHERE
		f.order_date IS NOT NULL
	GROUP BY
		EXTRACT(YEAR FROM f.order_date),
		p.product_name
)

SELECT 
	order_year,
	product_name,
	current_sales,
	TRUNC(AVG(current_sales) OVER (PARTITION BY product_name)) AS avg_sales,
	TRUNC(current_sales - AVG(current_sales) OVER (PARTITION BY product_name)) AS avg_diff,
	CASE
		WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
		WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
		ELSE 'Avg'
	END AS avg_change,
	TRUNC(LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year)) AS py_sales,
	TRUNC(current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year)) AS py_diff,
	CASE
		WHEN LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) IS NULL THEN NULL
		WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		ELSE 'No Change'
	END AS py_change
FROM 
	yearly_product_sales
ORDER BY
	product_name,
	order_year;