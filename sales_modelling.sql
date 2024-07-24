

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details';


-- - Orders 
DELETE FROM dim_store_details
where store_code not like '%-%';

ALTER TABLE orders_table
    ALTER COLUMN date_uuid SET DATA TYPE UUID USING (date_uuid::UUID),
    ALTER COLUMN user_uuid SET DATA TYPE UUID USING (user_uuid::UUID);

ALTER TABLE orders_table
    ALTER COLUMN card_number SET DATA TYPE VARCHAR(50),
    ALTER COLUMN store_code SET DATA TYPE VARCHAR(255),
    ALTER COLUMN product_code SET DATA TYPE VARCHAR(255);

ALTER TABLE orders_table
    ALTER COLUMN product_quantity SET DATA TYPE SMALLINT USING (product_quantity::SMALLINT);



-- --dim_users

ALTER TABLE dim_users
    ALTER COLUMN first_name SET DATA TYPE VARCHAR(255),
    ALTER COLUMN last_name SET DATA TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth SET DATA TYPE DATE USING (date_of_birth::DATE),
    ALTER COLUMN join_date SET DATA TYPE DATE USING (join_date::DATE),
    ALTER COLUMN country_code SET DATA TYPE VARCHAR(10),
    ALTER COLUMN user_uuid SET DATA TYPE UUID USING (user_uuid::UUID);




-- dim_store_details

-- There are two latitude columns in the store details table. Using SQL, merge one of the columns into the other so you have one latitude column.

-- Then set the data types for each column as shown below:

-- +---------------------+-------------------+------------------------+
-- | store_details_table | current data type |   required data type   |
-- +---------------------+-------------------+------------------------+
-- | longitude           | TEXT              | FLOAT                  |
-- | locality            | TEXT              | VARCHAR(255)           |
-- | store_code          | TEXT              | VARCHAR(?)             |
-- | staff_numbers       | TEXT              | SMALLINT               |
-- | opening_date        | TEXT              | DATE                   |
-- | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
-- | latitude            | TEXT              | FLOAT                  |
-- | country_code        | TEXT              | VARCHAR(?)             |
-- | continent           | TEXT              | VARCHAR(255)           |
-- +---------------------+-------------------+------------------------+
-- There is a row that represents the business's website change the location column values from N/A to NULL.




-- --Merge latitude into lat
UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat);

-- --Drop the redundant lat column
ALTER TABLE dim_store_details DROP COLUMN IF EXISTS lat;




-- Change data types of columns
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING REGEXP_REPLACE(staff_numbers, '[^0-9]', '', 'g')::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(255),
    ALTER COLUMN continent TYPE VARCHAR(255);



-- dim_products

-- You will need to do some work on the products table before casting the data types correctly.

-- The product_price column has a £ character which you need to remove using SQL.

-- The team that handles the deliveries would like a new human-readable column added for the weight so they can quickly make decisions on delivery weights.

-- Add a new column weight_class which will contain human-readable values based on the weight range of the product.

-- +--------------------------+-------------------+
-- | weight_class VARCHAR(?)  | weight range(kg)  |
-- +--------------------------+-------------------+
-- | Light                    | < 2               |
-- | Mid_Sized                | >= 2 - < 40       |
-- | Heavy                    | >= 40 - < 140     |
-- | Truck_Required           | => 140            |
-- +----------------------------+-----------------+


-- Remove the £ character from product_price

UPDATE dim_products
SET product_price = CAST(REPLACE(CAST(product_price AS TEXT), '£', '') AS FLOAT);

-- Cast the data types correctly
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

--  Add the weight_class column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- -- Update the weight_class column based on weight ranges
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE NULL
END;


-- --  dim_products   | current data type  | required data type |
-- -- +-----------------+--------------------+--------------------+
-- -- | product_price   | TEXT               | FLOAT              |
-- -- | weight          | TEXT               | FLOAT              |
-- -- | EAN             | TEXT               | VARCHAR(?)         |
-- -- | product_code    | TEXT               | VARCHAR(?)         |
-- -- | date_added      | TEXT               | DATE               |
-- -- | uuid            | TEXT               | UUID               |
-- -- | still_available | TEXT               | BOOL               |
-- -- | weight_class    | TEXT               | VARCHAR(?)         |

-- ALTER TABLE dim_products DROP COLUMN IF EXISTS "Unnamed: 0";


-- Change the data types
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(255),
ALTER COLUMN product_code TYPE VARCHAR(255),
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
ALTER COLUMN weight_class TYPE VARCHAR(255);


--  dim_date_times
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(255),
ALTER COLUMN year TYPE VARCHAR(255),
ALTER COLUMN day TYPE VARCHAR(255),
ALTER COLUMN time_period TYPE VARCHAR(255),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;


-- Alter the column data types
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(50),
ALTER COLUMN expiry_date TYPE VARCHAR(5);
-- ALTER COLUMN date_payment_confirmed TYPE DATE USING to_date(date_payment_confirmed, 'YYYY-MM-DD');




 -- -- Add Primary keys 

-- Add primary key to `dim_date_times`
ALTER TABLE dim_date_times
ADD CONSTRAINT pk_dim_date_times_date_uuid PRIMARY KEY (date_uuid);

-- Add primary key to `dim_users`
ALTER TABLE dim_users
ADD CONSTRAINT pk_dim_users_user_uuid PRIMARY KEY (user_uuid);

-- Add primary key to `dim_card_details`
ALTER TABLE dim_card_details
ADD CONSTRAINT pk_dim_card_details_card_number PRIMARY KEY (card_number);

DELETE FROM dim_store_details
WHERE store_code = 'NULL';

-- -- Add primary key to `dim_store_details`
ALTER TABLE dim_store_details
ADD CONSTRAINT pk_dim_store_details_store_code PRIMARY KEY (store_code);

----Add primary key to `dim_products`
ALTER TABLE dim_products
ADD CONSTRAINT pk_dim_products_product_code PRIMARY KEY (product_code);








---Add foreign key constraint for `date_uuid`
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid)
ON DELETE CASCADE;

-- Add foreign key constraint for `user_uuid`
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid)
ON DELETE CASCADE;

-- Add foreign key constraint for `card_number`
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number)
ON DELETE CASCADE;


-- Add foreign key constraint for `store_code`
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code)
ON DELETE CASCADE;

-- Add foreign key constraint for `product_code`
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code)
ON DELETE CASCADE;
