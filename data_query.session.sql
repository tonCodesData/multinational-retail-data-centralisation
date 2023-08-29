--1. how many stores does the business have and in which countries?

SELECT country_code, COUNT(country_code) as total_no_stores
FROM stores
GROUP BY 1
ORDER BY 2 DESC; 

--2. which locations has the most sotres?
SELECT locality, COUNT(locality) AS total_no_stores
FROM stores
GROUP BY 1
ORDER BY 2 DESC
LIMIT 7;

--3. which months produce the average highest cost of sales typically?
WITH cte AS(
    SELECT o.product_code, o.product_quantity, d.month
    FROM orders o
    JOIN dates d
        ON o.date_uuid = d.date_uuid
)
SELECT cte.month, SUM(p.product_price*cte.product_quantity)
FROM products p
JOIN cte
    ON p.product_code = cte.product_code
GROUP BY 1
ORDER BY 2 DESC;


--4. how many sales are coming from online?

WITH cte AS (
	SELECT 
		o.product_quantity, s.store_type,
		CASE
			WHEN s.store_type = 'Web Portal' THEN 'Web'
			ELSE 'Offline'
		END AS location 
	FROM orders o
	JOIN stores s
	ON o.store_code = s.store_code
)
SELECT 
	count(cte.location) as numbers_of_sales, 
	SUM(cte.product_quantity) as product_quantity_count, 
	cte.location
FROM cte
GROUP BY 3;


--5. what % of sale come through each type of store

WITH cte1 AS (
    SELECT 
        o.product_code, o.product_quantity, s.store_type
    FROM orders o
    JOIN stores s ON o.store_code = s.store_code
),
cte2 AS (
    SELECT 
        cte1.store_type, 
    	round(SUM(cte1.product_quantity * p.product_price)) AS total_sales
    FROM cte1
    JOIN products p ON cte1.product_code = p.product_code
    GROUP BY 1
),
cte3 AS (
    SELECT 
        SUM(total_sales) AS overall_total_sales
    FROM cte2
)
SELECT 
    cte2.store_type,
    cte2.total_sales as total_sales,
    CAST(((cte2.total_sales / cte3.overall_total_sales) * 100) AS numeric(10, 2)) AS percentage_sales
FROM cte2, cte3
ORDER BY 2 DESC;

--6. which month in each year produced the highest cost of sales

WITH cte1 AS(
    SELECT o.product_code, o.product_quantity, d.month, d.year
    FROM orders o
    JOIN dates d
        ON o.date_uuid = d.date_uuid
),
cte2 AS(
	SELECT cte1.month, cte1.year, SUM(p.product_price*cte1.product_quantity) AS total_sales
	FROM products p
	JOIN cte1
		ON p.product_code = cte1.product_code
	GROUP BY 2,1
),
cte3 AS(
	SELECT *, 
	ROW_NUMBER() OVER(
		PARTITION BY cte2.year
		ORDER BY cte2.total_sales DESC
	) AS rn
FROM cte2
)
SELECT 
	CAST(cte3.total_sales AS numeric(10,2)) AS total_sales, 
	cte3.year, 
	cte3.month
FROM cte3
ORDER BY 1 DESC;


--7. what is the staff headcount?
SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM stores
GROUP BY 2
ORDER BY 1 DESC;

--8. which ggerman store type is selling the most?

WITH cte AS(
    SELECT o.product_code, o.product_quantity, s.store_type, s.country_code
    FROM orders o
    JOIN stores s
        ON o.store_code = s.store_code
	WHERE s.country_code = 'DE'
)
SELECT 
	cte.store_type, 
	CAST(SUM(p.product_price*cte.product_quantity) AS numeric(10,2)) AS total_sales, 
	cte.country_code
FROM products p
JOIN cte
    ON p.product_code = cte.product_code
GROUP BY 1, 3
ORDER BY 2;

