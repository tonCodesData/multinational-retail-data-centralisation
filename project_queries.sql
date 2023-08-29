
SELECT * FROM orders LIMIT 10;
SELECT * FROM cards LIMIT 10;
SELECT * FROM dates LIMIT 10;
SELECT * FROM products LIMIT 10;
SELECT * FROM stores LIMIT 10;
SELECT * FROM users LIMIT 10;

DROP TABLE stores;
--------------ORDERS-------------------

--- date_uuid recast -----
ALTER TABLE orders
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

--- user_uuid recast -----
ALTER TABLE orders
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;


--- card_number recast -----
ALTER TABLE orders
ALTER COLUMN card_number TYPE VARCHAR(30);


--- store_code recast -----

ALTER TABLE orders
ALTER COLUMN store_code TYPE VARCHAR(20);


--- product_code recast -----

ALTER TABLE orders
ALTER COLUMN product_code TYPE VARCHAR(12);

--- product_quantity recast -----

ALTER TABLE orders
ALTER COLUMN product_quantity TYPE SMALLINT;


----------------- CARDS ---------------------------
ALTER TABLE cards
ALTER COLUMN card_number TYPE VARCHAR(30);

ALTER TABLE cards   
ALTER COLUMN expiry_date TYPE VARCHAR(20);

ALTER TABLE cards
ALTER COLUMN date_payment_confirmed TYPE DATE;


--------------- add primary keys

ALTER TABLE cards
ADD PRIMARY KEY (card_number);

-------------- add foreign keys

ALTER TABLE orders
ADD FOREIGN KEY (card_number) REFERENCES cards (card_number);


-----------DATES============================================
ALTER TABLE dates
ALTER COLUMN month TYPE VARCHAR(2);

ALTER TABLE dates
ALTER COLUMN year TYPE VARCHAR(4);

ALTER TABLE dates
ALTER COLUMN day TYPE VARCHAR(2);

ALTER TABLE dates
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dates
ALTER COLUMN date_uuid TYPE UUID using date_uuid::UUID;

--------------- add primary keys

ALTER TABLE dates
ADD PRIMARY KEY (date_uuid);

-------------- add foreign keys

ALTER TABLE orders
ADD FOREIGN KEY (date_uuid) REFERENCES dates (date_uuid);



--================PRODUCTS=======================

ALTER TABLE products
ADD COLUMN weight_class VARCHAR(20);

UPDATE products
SET weight_class =
    CASE
        WHEN weight_kg < 2 THEN 'Light'
        WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_sized'
        WHEN weight_kg >= 40 AND weight_kg < 100 THEN 'Heavy'
        ELSE 'Truck_required'
    END;

ALTER TABLE products
RENAME COLUMN removed TO still_available;

ALTER TABLE products
RENAME COLUMN weight_kg TO weight;

ALTER TABLE products
ALTER COLUMN "EAN" TYPE text;

SELECT MAX(CHAR_LENGTH("EAN")) FROM products;

ALTER TABLE products
ALTER COLUMN "EAN" TYPE VARCHAR(20);

SELECT MAX(CHAR_LENGTH(product_code)) FROM products;

ALTER TABLE products
ALTER COLUMN product_code TYPE VARCHAR(12);

ALTER TABLE products
ALTER COLUMN date_added TYPE DATE;

ALTER TABLE products
ALTER COLUMN uuid TYPE UUID using uuid::UUID;

ALTER TABLE products
ADD COLUMN availability BOOL;

UPDATE products
SET availability = 
    CASE 
        WHEN still_available = 'Still_avaliable' THEN TRUE
        WHEN still_available = 'Removed' THEN FALSE
        ELSE NULL
    END;

ALTER TABLE products
DROP COLUMN still_available;

ALTER TABLE products
RENAME COLUMN availability to still_available;

--------------- add primary keys

ALTER TABLE products
ADD PRIMARY KEY (product_code);

-------------- add foreign keys

ALTER TABLE orders
ADD FOREIGN KEY (product_code) REFERENCES products (product_code);



--------------USERS-----
ALTER TABLE users
ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE users
ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE users
ALTER COLUMN date_of_birth TYPE DATE;

ALTER TABLE users
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE users
ALTER COLUMN join_date TYPE DATE;

--------------- add primary keys

ALTER TABLE users
ADD PRIMARY KEY (user_uuid);

-------------- add foreign keys

ALTER TABLE orders
ADD FOREIGN KEY (user_uuid) REFERENCES users (user_uuid);


--===============stores=======================
ALTER TABLE stores
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE stores
ALTER COLUMN store_code TYPE VARCHAR(20);

ALTER TABLE stores
ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE stores
ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE stores
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE stores
ALTER COLUMN continent TYPE VARCHAR(255);

ALTER TABLE stores
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN store_type DROP NOT NULL;

--------------- add primary keys
ALTER TABLE stores
ADD PRIMARY KEY (store_code);

-------------- add foreign keys
ALTER TABLE orders
ADD FOREIGN KEY (store_code) REFERENCES users (store_code);





