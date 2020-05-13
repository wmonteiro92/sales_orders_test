-- no external tables use PKs since there is no way to ensure there are no duplicate rows
create external schema s3_raw
from data catalog
database 'dev'
iam_role 'arn:aws:iam::822203870541:role/spectrum_role'
create external database if not exists;

CREATE EXTERNAL TABLE s3_raw.Customers(
customer_id char(32),
customer_unique_id char(32),
customer_zip_code_prefix integer,
customer_city varchar(128),
customer_state char(2))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/customers'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.Geolocation(
geolocation_zip_code_prefix char(7),
geolocation_lat float8,
geolocation_lng float8,
geolocation_city varchar(128),
geolocation_state char(2))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/geolocation'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.OrderHeaders(
order_id varchar(34),
customer_id varchar(34),
order_status varchar(32),
order_purchase_timestamp timestamp,
order_approved_at timestamp,
order_delivered_carrier_date timestamp,
order_delivered_customer_date timestamp,
order_estimated_delivery_date timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/orders/headers'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.OrderItems(
order_id char(32),
order_item_id smallint,
product_id char(32),
seller_id char(32),
shipping_limit_date timestamp,
price decimal(8,2),
freight_value decimal(8,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/orders/items'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.OrderPayments(
order_id char(32),
payment_sequential smallint,
payment_type varchar(32),
payment_installments smallint,
payment_value decimal(8,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/orders/payments'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.OrderReviews(
review_id char(32),
order_id char(32),
review_score smallint,
review_comment_title nvarchar(256),
review_comment_message nvarchar(2048),
review_creation_date timestamp,
review_answer_timestamp timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/orders/reviews'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.Products(
product_id varchar(36),
product_category_name varchar(128),
product_name_lenght smallint,
product_description_lenght smallint,
product_photos_qty smallint,
product_weight_g smallint,
product_length_cm smallint,
product_height_cm smallint,
product_width_cm smallint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/products'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.Sellers(
seller_id char(32),
seller_zip_code_prefix integer,
customer_city varchar(128),
customer_state char(2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/sellers'
TABLE PROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE s3_raw.CategoryTranslations(
product_category_name varchar(128),
product_category_name_english varchar(128)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION 's3://olist-hiring-dev/engineered/translations/products'
TABLE PROPERTIES ('skip.header.line.count'='1');