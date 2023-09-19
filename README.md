# Multinational Retail Data Centralisation

In this project, I assume the role of a Data Analyst working for a multinational company that sells various goods across the globe. Their sales data was spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, it is imperative to make its sales data accessible from one centralised location. 

And so, my first goal was to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. Then I queried the database to get up-to-date metrics for the business.

## Tools and dependencies
- Python (pandas, tabula, requests, boto3, sqlalchemy)
- PostgreSQL (psql client, SQL Tools in VS Code)

## Data storage and File types

|serial| data regarding      | stored in     | file type |
|:-----| :-------------------| :------------ |:----------|
|1     | historical user data| AWS RDS       |           |
|2     | users card details  | AWS S3 bucket | PDF       |
|3     | store data          | API           |           |
|4     | product information | AWS S3 bucket | CSV       |
|5     | orders              | AWS RDS       |           |
|6     | sales               | AWS S3 bucket | JSON      | 

## File description

|File | Description |
|-----|-------------|
|data_extraction.py| contain DataExtractor class. This works as a utility class by creating methods to help extract data from different data sources |
|data_cleaning.py| contain DataCleaning class to clean data of each data sources |
|database_utils.py| contain DatabaseConnector class. This class is used to connect with and upload data to the database |

## Extract, Transform and Load(ETL):
In the etl_execution.py file, for each of the 6 information types(user, card, store, product, orders, and sales), the three different classes DataExtractor, DataCleaning, and DatabaseConnector are initialised. Then classses, as discussed previously, contain methods to create connection to source storage and extract data of each information type from different sources, clean the extracted data, and finally create connection to PostgreSQL server to load the cleaned data into sql database. 

## Create a star based schema for efficient data query:
table_alter_update.sql file alters column types to create uniformed column types accross the six tables. This file also contains script that updates the products table with new column weight_category. 
Next, we assign primary key constraint to appropriate columns of all tables except orders table and add foreign keys to orders table to create a star-shaped schema. 

## Query the database for getting insights: 
At this point, the database is ready to be queried. Here are the queries I performed. 

1. how many stores does the business have and in which countries?
```
SELECT country_code, COUNT(country_code) as total_no_stores
FROM stores
GROUP BY 1
ORDER BY 2 DESC; 
```

2. which locations has the most sotres?
```SELECT locality, COUNT(locality) AS total_no_stores
FROM stores
GROUP BY 1
ORDER BY 2 DESC
LIMIT 7;```

3. which months produce the average highest cost of sales typically?
```WITH cte AS(
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
ORDER BY 2 DESC;```

4. how many sales are coming from online?

```WITH cte AS (
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
GROUP BY 3;```

--5. what % of sale come through each type of store

```WITH cte1 AS (
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
ORDER BY 2 DESC;```

6. which month in each year produced the highest cost of sales

```WITH cte1 AS(
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
ORDER BY 1 DESC;```

7. what is the staff headcount?
```SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM stores
GROUP BY 2
ORDER BY 1 DESC;```

8. which ggerman store type is selling the most?

```WITH cte AS(
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
ORDER BY 2;```

