
1. Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv

-- 2. Store raw data into hdfs location

 hadoop fs -put filename '/hdfs/path/'
  

-- 3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table.
  
Solution- 
  
  CREATE TABLE sales_order_csv (
  ORDERNUMBER INT,
  QUANTITYORDERED INT,
  PRICEEACH INT,
  ORDERLINENUMBER INT,
  SALES INT,
  STATUS STRING,
  QTR_ID INT,
  MONTH_ID INT,
  YEAR_ID INT,
  PRODUCTLINE INT,
  MSRP INT,
  PRODUCTCODE INT,
  PHONE INT,
  CITY STRING,
  STATE STRING,
  POSTALCODE INT,
  COUNTRY STRING,
  TERRITORY STRING,
  CONTACTLASTNAME STRING,
  CONTACTFIRSTNAME STRING,
  DEALSIZE INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

  

-- 4. Load data from hdfs path into "sales_order_csv" 

-- Solution - 

  LOAD DATA INPATH '/user/hive/raw_data/sales_order_data.csv' INTO TABLE sales_order_csv;


-- 5. Create an internal hive table which will store data in ORC format "sales_order_orc"

-- Solution 

 CREATE TABLE sales_order_csv (
  ORDERNUMBER INT,
  QUANTITYORDERED INT,
  PRICEEACH INT,
  ORDERLINENUMBER INT,
  SALES INT,
  STATUS STRING,
  QTR_ID INT,
  MONTH_ID INT,
  YEAR_ID INT,
  PRODUCTLINE INT,
  MSRP INT,
  PRODUCTCODE INT,
  PHONE INT,
  CITY STRING,
  STATE STRING,
  POSTALCODE INT,
  COUNTRY STRING,
  TERRITORY STRING,
  CONTACTLASTNAME STRING,
  CONTACTFIRSTNAME STRING,
  DEALSIZE INT
)
STORED AS ORC;
  

-- 6. Load data from "sales_order_csv" into "sales_order_orc"

-- Solution
    INSERT INTO TABLE sales_order_orc SELECT * FROM sales_order_csv;



-- Perform below menioned queries on "sales_order_orc" table :

-- a. Calculatye total sales per year

-- Solution - 

SELECT YEAR_ID AS year, SUM(SALES) AS total_sales
FROM sales_order_csv
GROUP BY YEAR_ID;

   
-- b. Find a product for which maximum orders were placed

SELECT PRODUCTCODE, COUNT(*) AS order_count
FROM sales_order_csv
GROUP BY PRODUCTCODE
ORDER BY order_count DESC
LIMIT 1;


-- c. Calculate the total sales for each quarter
SELECT QTR_ID, SUM(SALES) AS total_sales
FROM sales_order_csv
GROUP BY QTR_ID;

-- d. In which quarter sales was minimum
SELECT QTR_ID, SUM(SALES) AS total_sales
FROM sales_order_csv
GROUP BY QTR_ID
ORDER BY total_sales
LIMIT 1;


-- e. In which country sales was maximum and in which country sales was minimum

  SELECT COUNTRY, SUM(SALES) AS total_sales
FROM sales_order_csv
GROUP BY COUNTRY
ORDER BY total_sales DESC
LIMIT 1;

SELECT COUNTRY, SUM(SALES) AS total_sales
FROM sales_order_csv
GROUP BY COUNTRY
ORDER BY total_sales
LIMIT 1;

  
-- f. Calculate quartelry sales for each city

SELECT QTR_ID, CITY, SUM(SALES) AS total_sales
FROM sales_order_csv
GROUP BY QTR_ID, CITY;

-- h. Find a month for each year in which maximum number of quantities were sold

SELECT CONCAT(YEAR_ID, '-', MONTH_ID) AS month_year, MAX(QUANTITYORDERED) AS max_quantity
FROM sales_order_csv
GROUP BY CONCAT(YEAR_ID, '-', MONTH_ID);









