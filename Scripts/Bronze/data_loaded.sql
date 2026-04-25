SHOW VARIABLES LIKE 'secure_file_priv';

SET sql_mode = '';

---------------------------------------------customers table--------------------------
TRUNCATE TABLE customers;

LOAD DATA INFILE '/var/lib/mysql-files/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


select * from customers limit 10;

---------------------------------------------orders table--------------------------
TRUNCATE TABLE orders;


LOAD DATA INFILE '/var/lib/mysql-files/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from orders limit 10;


---------------------------------------------order_items table--------------------------

TRUNCATE TABLE order_items;


LOAD DATA INFILE '/var/lib/mysql-files/order_items.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



select * from order_items limit 10;

--------------------------------------------- truncate table--------------------------

TRUNCATE TABLE reviews;



--------------------------------------------- reviews table--------------------------
LOAD DATA INFILE '/var/lib/mysql-files/reviews.csv'
IGNORE
INTO TABLE reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from reviews limit 10;

---------------------------------------------products table--------------------------

TRUNCATE TABLE products;

LOAD DATA INFILE '/var/lib/mysql-files/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



select * from products limit 10;

