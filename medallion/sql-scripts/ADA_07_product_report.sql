-----------------------------
-- Product Report
-----------------------------

/*
1. Gathers essential fields such as product name, category, subcategory, and cost
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers
3. Aggregate product-level metrics:
	- total orders
	- total sales
	- total quantity sold
	- total customers (unique)
	- lifespan (months)
4. Calculates valuable KPIs
	- recency (months)
	- average order revenue
	- average monthly revenue
*/

CREATE OR REPLACE VIEW gold.report_products AS
WITH base_query AS
(
	SELECT
		f.order_number,
		f.customer_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	FROM
		gold.fact_sales f
			LEFT join
		gold.dim_products p ON f.product_key = p.product_key
	WHERE
		f.order_date IS NOT NULL
),

product_aggregation AS
(
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT customer_key) AS total_customers,
		TRUNC(AVG(sales_amount / quantity)) AS avg_selling_price,
		MAX(order_date) AS last_order,
		EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 +
		EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
	FROM
		base_query
	GROUP BY
		product_key,
		product_name,
		category,
		subcategory,
		cost
)

SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	avg_selling_price,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	last_order,
	EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_order)) * 12 +
	EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_order)) AS recency,
	lifespan,
	CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders 
	END AS avg_order_revenue,
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE CEIL(total_sales / lifespan)
	END AS avg_monthly_revenue,
	CASE
		WHEN total_sales >= 225000 THEN 'High-Performers'
		WHEN total_sales >= 55000 THEN 'Mid-Range'
		ELSE 'Low-Performers'
	END AS product_segment
FROM
	product_aggregation;

-- Query View
SELECT 
	product_name,
	product_segment,
	total_sales,
	CEIL(AVG(total_sales) OVER ()) AS average_sales,
	CASE WHEN total_sales > CEIL(AVG(total_sales) OVER ()) THEN 1 ELSE 0 END AS is_above_average,
	NTILE(3) OVER (ORDER BY total_sales DESC) AS sales_quartile,
	ROUND((CUME_DIST() OVER (ORDER BY total_sales DESC) * 100)::NUMERIC, 2) AS cumulative_dist
FROM 
	gold.report_products
GROUP BY
	product_name,
	total_sales,
	product_segment;

-- PERCENTILE
SELECT
    percentiles[1] AS q1,
    percentiles[2] AS median,
    percentiles[3] AS q3,
    percentiles[4] AS max
FROM (
    SELECT
        PERCENTILE_CONT(ARRAY[0.25, 0.5, 0.75, 1.00]) WITHIN GROUP (ORDER BY total_sales) AS percentiles
    FROM
        gold.report_products
) AS sub;