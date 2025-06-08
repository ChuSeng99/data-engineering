-----------------------------------------------------
-- Part-To-Whole Analysis 
-- ([Measure] / Total[Measure]) * 100 By [Dimension]
-----------------------------------------------------

-- Which categories contribute the most to overall sales

WITH category_sales AS
(
	SELECT
		p.category,
		SUM(f.sales_amount) AS total_sales
	FROM
		gold.fact_sales f
			LEFT JOIN
		gold.dim_products p ON f.product_key = p.product_key
	WHERE
		f.order_date IS NOT NULL
	GROUP BY
		p.category
)

SELECT 
	*,
	ROUND((total_sales / SUM(total_sales) OVER ()) * 100, 3) AS total_sales_pct
FROM 
	category_sales
ORDER BY
	total_sales_pct DESC;