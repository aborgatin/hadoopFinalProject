/*Exercise 4*/
CREATE EXTERNAL TABLE IF NOT EXISTS ab.purchases (product_name STRING, price FLOAT, purchase_dt STRING, category STRING, ip_address  STRING)
 COMMENT 'External table for purchase data' PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/cloudera/hive-ext/';

LOAD DATA INPATH '/user/cloudera/events/2018/10/10' INTO TABLE ab.purchases PARTITION (dt = '2018-10-10');
LOAD DATA INPATH '/user/cloudera/events/2018/10/11' INTO TABLE ab.purchases PARTITION (dt = '2018-10-11');
LOAD DATA INPATH '/user/cloudera/events/2018/10/12' INTO TABLE ab.purchases PARTITION (dt = '2018-10-12');
LOAD DATA INPATH '/user/cloudera/events/2018/10/13' INTO TABLE ab.purchases PARTITION (dt = '2018-10-13');
LOAD DATA INPATH '/user/cloudera/events/2018/10/14' INTO TABLE ab.purchases PARTITION (dt = '2018-10-14');
LOAD DATA INPATH '/user/cloudera/events/2018/10/15' INTO TABLE ab.purchases PARTITION (dt = '2018-10-15');
LOAD DATA INPATH '/user/cloudera/events/2018/10/16' INTO TABLE ab.purchases PARTITION (dt = '2018-10-16');
LOAD DATA INPATH '/user/cloudera/events/2018/10/17' INTO TABLE ab.purchases PARTITION (dt = '2018-10-17');


/*Exercise 5.1*/
create table ab.ex5_1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
as
select p.category, count(*) as c
from ab.purchases as p
group by p.category
order by c desc
limit 10;

/*Exercise 5.2*/

create table ab.ex5_2 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
as
select category, product_name, c
from
    (
    select category, product_name, c, row_number() over(partition by category order by c desc) n
    from
        (
        select category, product_name, count(*) c
        FROM ab.purchases
        group by product_name, category
        ) A
    ) B
WHERE n <= 10;



/*6. Table for geodata*/

CREATE TABLE IF NOT EXISTS ab.geo_blocks
(ip_mask STRING, country_id STRING)
COMMENT 'Table for geo data blocks'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/hive-geo/blocks'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/cloudera/geo/GeoLite2-Country-Blocks-IPv4.csv' INTO TABLE ab.geo_blocks;

CREATE TABLE IF NOT EXISTS ab.geo_locations
(id STRING, locale_code STRING, continent_code STRING,continent_name STRING,country_iso_code STRING, country_name STRING)
COMMENT 'Table for geo data locations'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/hive-geo/locations'
tblproperties ("skip.header.line.count"="1");


LOAD DATA INPATH '/user/cloudera/geo/GeoLite2-Country-Locations-en.csv' INTO TABLE ab.geo_locations;


select b.ip_mask, l.country_name
from ab.geo_blocks b
    join ab.geo_locations l
    on b.country_id = l.id;

/*Add custom UDF function*/
add jar ip-udf.jar;
create temporary function ip_match as "com.griddynamics.aborgatin.udf.MatchIPMask" using jar 'ip-udf.jar';
select ip_match('192.168.56.25', '192.168.56.0/24');
select ip_match('192.168.57.25', '192.168.56.0/24');


create table ab.geo_joined ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/cloudera/geo-joined/'
 as
select b.ip_mask, l.country_name, b.country_id
from ab.geo_blocks b
  left outer join ab.geo_locations l
  on b.country_id = l.id;


create table ab.purchases_geo ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/cloudera/hive-geo/'
  as
select p.product_name, p.price,p.category,p.ip_address, g.country_name, g.country_id
from ab.purchases p
  left outer join ab.geo_joined g
  on true
where ip_match(p.ip_address, g.ip_mask);


create table ab.ex6 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
as
select country_name, sum(price) total_sum
from ab.purchases_geo
group by country_name
order by total_sum desc
limit 10;

sqoop export --connect jdbc:mysql://localhost:3306/ab --username root --password cloudera --table ex5_1 --export-dir /user/hive/warehouse/ab.db/ex5_1;
sqoop export --connect jdbc:mysql://localhost:3306/ab --username root --password cloudera --table ex5_2 --export-dir /user/hive/warehouse/ab.db/ex5_2;
sqoop export --connect jdbc:mysql://localhost:3306/ab --username root --password cloudera --table ex6 --export-dir /user/hive/warehouse/ab.db/ex6;



mysql:
create database ab;

create table ex5_1 (category varchar(40), purchase_count integer);
create table ex5_2 (category varchar(40), product_name varchar(100), purchase_count DOUBLE);
create table ex6 (country varchar(40),  total_money_sum DOUBLE);

