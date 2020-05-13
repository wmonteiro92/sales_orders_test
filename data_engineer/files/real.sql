CREATE SCHEMA olist;

CREATE TABLE olist.ZipCodes(
zip_code char(5) not null,
latitude float8,
longitude float8,
city varchar(128),
state char(2),
primary key(zip_code));

-- Redshift does not support unsigned ints
CREATE TABLE olist.ProductCategories(
id_category integer identity,
name_pt varchar(128),
name_en varchar(128),
primary key(id_category));

-- Redshift does not support unsigned ints
-- Creating a new ID as a bigint uses way less space for any indexes instead of using a char(32)
CREATE TABLE olist.Sellers(
id_seller bigint identity,
guid_seller char(32),
zip_code char(5),
primary key(id_seller),
foreign key(zip_code) references olist.ZipCodes(zip_code));

-- Redshift does not support unsigned ints
-- Creating a new ID as a bigint uses way less space for any indexes instead of using a char(32)
-- Fixing typos (e.g. 'lenght' instead of 'length')
CREATE TABLE olist.Products(
id_product bigint identity,
guid_product char(32),
id_category integer,
name_length smallint,
description_length smallint,
photos_qty smallint,
weight_g smallint,
length_cm smallint,
height_cm smallint,
width_cm smallint,
primary key(id_product),
foreign key(id_category) references olist.ProductCategories(id_category));

-- Redshift does not support unsigned ints
-- Creating a new ID as a bigint uses way less space for any indexes instead of using a char(32)
CREATE TABLE olist.Customers(
id_customer bigint identity,
guid_customer char(32),
zip_code char(5),
primary key(id_customer),
foreign key(zip_code) references olist.ZipCodes(zip_code));

-- Redshift does not support unsigned ints
-- Creating a new ID as a bigint uses way less space for any indexes instead of using a char(32)
CREATE TABLE olist.OrderStatus(
id_status integer identity,
description varchar(32),
primary key(id_status));

CREATE TABLE olist.OrderHeaders(
id_order bigint identity,
guid_order char(32),
id_customer char(32),
id_status integer,
purchased_at timestamp,
approved_at timestamp,
delivered_at_carrier timestamp,
delivered_at_customer timestamp,
estimated_at_delivery timestamp,
primary key(id_order),
foreign key(id_status) references olist.OrderStatus(id_status),
foreign key(id_customer) references olist.Customers(id_customer));

CREATE TABLE olist.OrderItems(
id_order bigint,
id_item smallint,
id_product bigint,
id_seller bigint,
shipping_limit_date timestamp,
price decimal(8,2),
freight_value decimal(8,2),
primary key(id_order, id_item),
foreign key(id_order) references olist.OrderHeaders(id_order),
foreign key(id_product) references olist.Products(id_product),
foreign key(id_seller) references olist.Sellers(id_seller));

CREATE TABLE olist.OrderPaymentTypes(
id_payment integer identity,
description varchar(32),
primary key(id_payment));

CREATE TABLE olist.OrderPayments(
id_order bigint,
id_sequential smallint,
id_payment integer,
installments smallint,
value decimal(8,2),
primary key(id_order, id_sequential),
foreign key(id_order) references olist.OrderHeaders(id_order),
foreign key(id_payment) references olist.OrderPaymentTypes(id_payment));

CREATE TABLE olist.OrderReviews(
id_order bigint,
score smallint,
comment_title nvarchar(256),
comment_message nvarchar(2048),
creation_date timestamp,
answer_timestamp timestamp,
primary key(id_order),
foreign key(id_order) references olist.OrderHeaders(id_order));

-- latitude and longitude remains as separate columns in order to be easily read by data scientists
-- the POINT data type uses 16 bytes while float8 x2 uses up to 16 bytes
-- SELECT DISTINCT ON is not supported by Redshift
CREATE OR REPLACE PROCEDURE sp_create_zip_codes() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.ZipCodes
  SELECT DISTINCT
  trim('"' FROM geolocation_zip_code_prefix) as zip_code,
  geolocation_lat AS latitude,
  geolocation_lng AS longitude,
  geolocation_city AS city,
  geolocation_state AS state
  from s3_raw.geolocation;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_product_categories() LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM olist.ProductCategories WHERE id_category >= 0;
  INSERT INTO olist.ProductCategories(name_pt, name_en)
  SELECT DISTINCT
  product_category_name AS name_pt,
  product_category_name_english AS name_en
  from s3_raw.CategoryTranslations;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_sellers() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.Sellers(guid_seller, zip_code)
  SELECT DISTINCT
  seller_id As guid_seller,
  lpad(c.customer_zip_code_prefix,5,'0') AS zip_code
  from s3_raw.Sellers s;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_products() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.Products(guid_product, id_category, name_length, description_length, photos_qty, weight_g, length_cm, height_cm, width_cm)
  SELECT DISTINCT
  trim('"' FROM product_id) as guid_product,
  cat.id_category AS id_category,
  product_name_lenght AS name_length,
  product_description_lenght AS description_length,
  product_photos_qty AS photos_qty,
  product_weight_g AS weight_g,
  product_length_cm AS length_cm,
  product_height_cm AS height_cm,
  product_width_cm AS width_cm
  from s3_raw.Products p
  LEFT JOIN olist.ProductCategories cat ON cat.name_pt = p.product_category_name;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_customers() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.Customers(guid_customer, zip_code)
  SELECT DISTINCT
  trim('"' FROM customer_id) as guid_customer,
  lpad(c.customer_zip_code_prefix,5,'0') AS zip_code
  from s3_raw.Customers c;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_payment_types() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.OrderPaymentTypes(description)
  SELECT DISTINCT
  trim(payment_type) as description
  from s3_raw.OrderPayments;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_order_statuses() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.OrderStatus(description)
  SELECT DISTINCT
  trim(order_status) as description
  from s3_raw.OrderHeaders;
END;
$$;

CREATE OR REPLACE PROCEDURE sp_create_order() LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO olist.OrderHeaders(guid_order, id_customer, id_status, purchased_at, approved_at, delivered_at_carrier, delivered_at_customer, estimated_at_delivery)
  SELECT 
  trim('"' FROM order_id) AS guid_order,
  c.id_customer AS id_customer,
  s.id_status AS id_status,
  order_purchase_timestamp AS purchased_at,
  order_approved_at AS approved_at,
  order_delivered_carrier_date AS delivered_at_carrier,
  order_delivered_customer_date AS delivered_at_customer,
  order_estimated_delivery_date AS estimated_at_delivery
  from s3_raw.OrderHeaders h
  LEFT JOIN olist.Customers c ON c.guid_customer = trim('"' FROM h.customer_id)
  LEFT JOIN olist.OrderStatus s ON s.description = trim(h.order_status);
  
  INSERT INTO olist.OrderItems(id_order, id_item, id_product, id_seller, shipping_limit_date, price, freight_value)
  SELECT
  h.id_order AS id_order,
  order_item_id AS id_item,
  p.id_product AS id_product,
  s.id_seller AS id_seller,
  shipping_limit_date,
  price,
  freight_value
  from s3_raw.OrderItems i
  LEFT JOIN olist.OrderHeaders h ON h.guid_order = i.order_id
  LEFT JOIN olist.Products p ON p.guid_product = i.product_id
  LEFT JOIN olist.Sellers s ON s.guid_seller = i.seller_id;
  
  INSERT INTO olist.OrderPayments(id_order, id_sequential, id_payment, installments, value)
  SELECT 
  h.id_order AS id_order,
  payment_sequential AS id_sequential,
  t.id_payment AS id_payment,
  payment_installments AS installments,
  payment_value AS value
  from s3_raw.OrderPayments p
  LEFT JOIN olist.OrderHeaders h ON h.guid_order = p.order_id
  LEFT JOIN olist.OrderPaymentTypes t ON t.description = trim(p.payment_type);
  
  INSERT INTO olist.OrderReviews(id_order, score, comment_title, comment_message, creation_date, answer_timestamp)
  SELECT 
  h.id_order AS id_order,
  review_score AS score,
  review_comment_title AS comment_title,
  review_comment_message AS comment_message,
  review_creation_date AS creation_date,
  review_answer_timestamp AS answer_timestamp
  from s3_raw.OrderReviews p
  LEFT JOIN olist.OrderHeaders h ON h.guid_order = p.order_id;  
END;
$$;


CALL sp_create_zip_codes();
CALL sp_create_product_categories();
CALL sp_create_sellers();
CALL sp_create_products();
CALL sp_create_customers();
CALL sp_create_payment_types();
CALL sp_create_order_statuses();
CALL sp_create_order();