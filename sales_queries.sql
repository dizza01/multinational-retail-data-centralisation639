-- -- -- The Operations team would like to know which countries we currently operate in and which country now has the most stores. Perform a query on the database to get the information, it should return the following information:

-- -- +----------+-----------------+
-- -- | country  | total_no_stores |
-- -- +----------+-----------------+
-- -- | GB       |             265 |
-- -- | DE       |             141 |
-- -- | US       |              34 |
-- -- +----------+-----------------+

SELECT 
    country_code,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
WHERE store_code not like '%WEB%'
GROUP BY 
    1
ORDER BY 
    total_no_stores DESC;



-- -- The business stakeholders would like to know which locations currently have the most stores.
-- -- They would like to close some stores before opening more in other locations.
-- -- Find out which locations have the most stores currently. The query should return the following:

-- -- +-------------------+-----------------+
-- -- |     locality      | total_no_stores |
-- -- +-------------------+-----------------+
-- -- | Chapletown        |              14 |
-- -- | Belper            |              13 |
-- -- | Bushley           |              12 |
-- -- | Exeter            |              11 |
-- -- | High Wycombe      |              10 |
-- -- | Arbroath          |              10 |
-- -- | Rutherglen        |              10 |

SELECT 
    locality,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    total_no_stores DESC
LIMIT 7;



-- -- -- Query the database to find out which months have produced the most sales. The query should return the following information:
-- +-------------+-------+
-- | total_sales | month |
-- +-------------+-------+
-- |   673295.68 |     8 |
-- |   668041.45 |     1 |
-- |   657335.84 |    10 |
-- |   650321.43 |     5 |
-- |   645741.70 |     7 |
-- |   645463.00 |     3 |
-- +-------------+-------+



select SUM(pr.product_price * o.product_quantity) as total_sales, d.month
	from orders_table o
	join dim_date_times d
	on d.date_uuid = o.date_uuid
    JOIN dim_products pr 
    ON o.product_code = pr.product_code
	group by 2
    ORDER BY 1 DESC;


-- -- The company is looking to increase its online sales.
-- -- They want to know how many sales are happening online vs offline.
-- -- Calculate how many products were sold and the amount of sales made for online and offline purchases.
-- +------------------+-------------------------+----------+
-- | numbers_of_sales | product_quantity_count  | location |
-- +------------------+-------------------------+----------+
-- |            26957 |                  107739 | Web      |
-- |            93166 |                  374047 | Offline  |
-- +------------------+-------------------------+----------+

select count(*) as numbers_of_sales, sum(product_quantity) as product_quantity_count, CASE WHEN store_code = 'WEB-1388012W' THEN 'web'
	else 'offline' end as location
from orders_table
group by 3;




-- -- -- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

-- -- -- Find out the total and percentage of sales coming from each of the different store types.
-- +-------------+-------------+---------------------+
-- | store_type  | total_sales | percentage_total(%) |
-- +-------------+-------------+---------------------+
-- | Local       |  3440896.52 |               44.87 |
-- | Web portal  |  1726547.05 |               22.44 |
-- | Super Store |  1224293.65 |               15.63 |
-- | Mall Kiosk  |   698791.61 |                8.96 |
-- | Outlet      |   631804.81 |                8.10 |
-- +-------------+-------------+---------------------+
SELECT 
    s.store_type, 
    SUM(pr.product_price * o.product_quantity) AS total_sales, 
    SUM(pr.product_price * o.product_quantity) * 100.0 / 
    (SELECT SUM(pr_inner.product_price * o_inner.product_quantity)
     FROM orders_table o_inner 
     JOIN dim_products pr_inner ON o_inner.product_code = pr_inner.product_code) AS "percentage_total(%)"
FROM 
    orders_table o
JOIN 
    dim_store_details s ON o.store_code = s.store_code
JOIN 
    dim_products pr ON o.product_code = pr.product_code
GROUP BY 
    s.store_type
ORDER BY 
    total_sales DESC;




-- -- The company stakeholders want assurances that the company has been doing well recently.

-- -- Find which months in which years have had the most sales historically.

-- -- The query should return the following information:

-- -- +-------------+------+-------+
-- -- | total_sales | year | month |
-- -- +-------------+------+-------+
-- -- |    27936.77 | 1994 |     3 |
-- -- |    27356.14 | 2019 |     1 |
-- -- |    27091.67 | 2009 |     8 |
-- -- |    26679.98 | 1997 |    11 |
-- -- |    26310.97 | 2018 |    12 |
-- -- |    26277.72 | 2019 |     8 |
-- -- |    26236.67 | 2017 |     9 |
-- -- |    25798.12 | 2010 |     5 |
-- -- |    25648.29 | 1996 |     8 |
-- -- |    25614.54 | 2000 |     1 |
-- -- +-------------+------+-------+

SELECT 
    SUM(pr.product_price * o.product_quantity) AS total_sales,
    d.year, 
    d.month
FROM 
    orders_table o
LEFT JOIN 
    dim_date_times d ON d.date_uuid = o.date_uuid
JOIN 
    dim_products pr ON o.product_code = pr.product_code
GROUP BY 
    d.year, 
    d.month
ORDER BY 
    total_sales DESC;


-- -- -- The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

-- -- -- The query should return the values:

-- -- -- +---------------------+--------------+
-- -- -- | total_staff_numbers | country_code |
-- -- -- +---------------------+--------------+
-- -- -- |               13307 | GB           |
-- -- -- |                6123 | DE           |
-- -- -- |                1384 | US           |
-- -- -- +---------------------+--------------+
SELECT SUM(STAFF_NUMBERS) AS total_staff_numbers, COUNTRY_CODE
FROM DIM_STORE_DETAILS
GROUP BY 2
ORDER BY 1 DESC;



-- -- The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

-- -- The query will return:

-- -- +--------------+-------------+--------------+
-- -- | total_sales  | store_type  | country_code |
-- -- +--------------+-------------+--------------+
-- -- |   198373.57  | Outlet      | DE           |
-- -- |   247634.20  | Mall Kiosk  | DE           |
-- -- |   384625.03  | Super Store | DE           |
-- -- |  1109909.59  | Local       | DE           |
-- +--------------+-------------+--------------+

select SUM(pr.product_price * o.product_quantity) AS total_sales, d.store_type, COUNTRY_CODE
from orders_table o
join DIM_STORE_DETAILS d
on d.store_code = o.store_code
JOIN 
dim_products pr ON o.product_code = pr.product_code
where country_code = 'DE'
group by 2, 3
order by 1;






-- -- Sales would like the get an accurate metric for how quickly the company is making sales.

-- -- Determine the average time taken between each sale grouped by year, the query should return the following information:

-- --  +------+-------------------------------------------------------+
-- --  | year |                           actual_time_taken           |
-- --  +------+-------------------------------------------------------+
-- --  | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
-- --  | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
-- --  | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
-- --  | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
-- --  | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
-- --  +------+-------------------------------------------------------+
 
-- -- Hint: You will need the SQL command LEAD.




WITH sales_with_timestamp AS (
    SELECT 
        o.date_uuid, 
        d.year,
        TO_TIMESTAMP(d.year::TEXT || '-' || d.month::TEXT || '-' || d.day::TEXT || ' ' || d.timestamp, 'YYYY-MM-DD HH24:MI:SS') AS sale_timestamp,
        TO_TIMESTAMP(d.year::TEXT || '-' || d.month::TEXT || '-' || d.day::TEXT || ' ' || d.timestamp, 'YYYY-MM-DD HH24:MI:SS') - 
        LAG(TO_TIMESTAMP(d.year::TEXT || '-' || d.month::TEXT || '-' || d.day::TEXT || ' ' || d.timestamp, 'YYYY-MM-DD HH24:MI:SS')) 
        OVER (PARTITION BY d.year ORDER BY TO_TIMESTAMP(d.year::TEXT || '-' || d.month::TEXT || '-' || d.day::TEXT || ' ' || d.timestamp, 'YYYY-MM-DD HH24:MI:SS')) AS time_diff
    FROM 
        orders_table o
    JOIN 
        dim_date_times d
    ON 
        o.date_uuid = d.date_uuid
),
average_time_by_year AS (
    SELECT
        year,
        AVG(EXTRACT(EPOCH FROM time_diff)) * 1000 AS avg_time_diff_ms
    FROM 
        sales_with_timestamp
    WHERE 
        time_diff IS NOT NULL
    GROUP BY 
        year
)
SELECT 
    year,
    CONCAT(
        '"hours": ', FLOOR(avg_time_diff_ms / 3600000)::TEXT, ', ',
        '"minutes": ', FLOOR((avg_time_diff_ms % 3600000) / 60000)::TEXT, ', ',
        '"seconds": ', FLOOR((avg_time_diff_ms % 60000) / 1000)::TEXT, ', ',
        '"milliseconds": ', FLOOR(avg_time_diff_ms % 1000)::TEXT
    ) AS actual_time_taken
FROM 
    average_time_by_year
ORDER BY 
    2 desc;