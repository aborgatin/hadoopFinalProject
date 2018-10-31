# hadoopFinalProject
Final project of learning path https://gridu.litmos.com/home/LearningPath/57126

Structure:

    /hadoopFinalProject
        /eventproducer          - Implement Random Events producer
        /UDFip_mask_checker     - UDF function for apply ip to mask
        flume-agent.properties  - properties for Flume agent
        hive.sql                - queries for hive 
        


Flow Event producer -> Flume -> HDFS -> HIVE -> SQOOP -> MYSQL

1. Copy file with flume configuration (hadoopFinalProject\flume-agent.properties) to Hadoop cluster
2. Start Flume agent
    ```bash
    /usr/bin/flume-ng agent -n ag1 --conf conf -f flume-agent.properties -Dflume.root.logger=INFO,console
    ```
3. Start event-producer
    ```bash
    java -jar event-producer.jar 
    ```
4. Start hive<br/>

4.1 Create database

    ```sql
    CREATE DATABASE ab;
    ```

4.2 Create external Hive table to process data <br/>
     
        CREATE EXTERNAL TABLE IF NOT EXISTS ab.purchases
        (product_name STRING, 
        price FLOAT, 
        purchase_dt STRING, 
        category STRING, 
        ip_address  STRING)
        COMMENT 'External table for purchase data' 
        PARTITIONED BY(dt STRING) 
        ROW FORMAT 
            DELIMITED FIELDS TERMINATED BY ',' 
        LOCATION '/user/cloudera/events2';
    
Load all partitions <br/>    
     
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-19') LOCATION '/user/cloudera/events/2018/10/19';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-20') LOCATION '/user/cloudera/events/2018/10/20';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-21') LOCATION '/user/cloudera/events/2018/10/21';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-22') LOCATION '/user/cloudera/events/2018/10/22';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-23') LOCATION '/user/cloudera/events/2018/10/23';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-24') LOCATION '/user/cloudera/events/2018/10/24';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-25') LOCATION '/user/cloudera/events/2018/10/25';
     ALTER TABLE ab.purchases ADD PARTITION (dt='2018-10-26') LOCATION '/user/cloudera/events/2018/10/26';
     
     Anover way:
     LOAD DATA INPATH '/user/cloudera/events2/2018/10/16' INTO TABLE ab.purchases PARTITION (dt = '2018-10-16');

         
4.3 Select top 10  most frequently purchased categories and put them into table ex5_1 <br/>

      create table ab.ex5_1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      as
      select p.category, count(*) as c
      from ab.purchases as p
      group by p.category
      order by c desc
      limit 10;
4.4 Select top 10 most frequently purchased product in each category and put them into table ex5_2 <br/>

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

4.5 Download and put to hdfs files blocks.csv and locations.csv from https://dev.maxmind.com/geoip/geoip2/geolite2/
wget http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip
curl -O http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip

4.6 Create tables and load data with blocks and locations <br/>

    CREATE TABLE IF NOT EXISTS ab.geo_blocks
    (ip_mask STRING, country_id STRING)
    COMMENT 'Table for geo data blocks'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION '/user/cloudera/hive-geo/blocks'
    tblproperties ("skip.header.line.count"="1");
    
    LOAD DATA INPATH '/user/cloudera/geo/blocks.csv' INTO TABLE ab.geo_blocks;
    
    CREATE TABLE IF NOT EXISTS ab.geo_locations
    (id STRING, locale_code STRING, continent_code STRING,continent_name STRING,country_iso_code STRING, country_name STRING)
    COMMENT 'Table for geo data locations'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION '/user/cloudera/hive-geo/locations'
    tblproperties ("skip.header.line.count"="1");
    
    LOAD DATA INPATH '/user/cloudera/geo/locations.csv' INTO TABLE ab.geo_locations;


4.7 Join blocks and locations by country_id

    create table ab.geo_joined ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/cloudera/geo-joined/'
     as
    select b.ip_mask, l.country_name, b.country_id
    from ab.geo_blocks b
      left outer join ab.geo_locations l
      on b.country_id = l.id;

4.8 Create UDF function <br/>
4.8.1 compile project UDFip_mask_checker<br/>
4.8.2 put file /target/ip-udf.jar to hdfs<br/>
4.8.3 add jar

    add jar ip-udf.jar;

4.8.4 create function

    create temporary function ip_match as "com.griddynamics.aborgatin.udf.MatchIPMask" using jar 'ip-udf.jar';

4.8.5 check creation (must return true)

    select ip_match('192.168.56.25', '192.168.56.0/24');

4.9 join tables

    create table ab.purchases_geo ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/cloudera/hive-geo/'
      as
    select p.product_name, p.price,p.category,p.ip_address, g.country_name, g.country_id
    from ab.purchases p
      left outer join ab.geo_joined g
      on true
    where ip_match(p.ip_address, g.ip_mask);

4.10 Select top 10 countries with the highest money spending and put them to table ex6

    create table ab.ex6 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    as
    select country_name, sum(price) total_sum
    from ab.purchases_geo
    group by country_name
    order by total_sum desc
    limit 10;

5 Connect to MySQl and create database and tables <br/>
    
    mysql -u root -p
    create database ab;
    use ab;
    create table ex5_1 (category varchar(40), purchase_count integer);
    create table ex5_2 (category varchar(40), product_name varchar(100), purchase_count integer);
    create table ex6 (country varchar(40), total_money_sum DOUBLE);
    
6 Sqoop <br/>
6.1 Move data from hive ex5_1 to MySQL ex5_1

    sqoop export --connect jdbc:mysql://10.0.0.21:3306/ab --username root --password cloudera --table ex5_1 --export-dir /user/hive/warehouse/ab.db/ex5_1;
    sqoop export --connect jdbc:mysql://localhost:3306/ab --username root --password cloudera --table ex5_1 --export-dir /user/hive/warehouse/ab.db/ex5_1;

6.2 Move data from hive ex5_2 to MySQL ex5_2

    sqoop export --connect jdbc:mysql://10.0.0.21:3306/ab --username root --password cloudera --table ex5_2 --export-dir /user/hive/warehouse/ab.db/ex5_2;
    sqoop export --connect jdbc:mysql://localhost:3306/ab --username root --password cloudera --table ex5_2 --export-dir /user/hive/warehouse/ab.db/ex5_2;

6.3 Move data from hive ex6 to MySQL ex6

    sqoop export --connect jdbc:mysql://10.0.0.21:3306/ab --username root --password cloudera --table ex6 --export-dir /user/hive/warehouse/ab.db/ex6;
    sqoop export --connect jdbc:mysql://localhost:3306/ab --username root --password cloudera --table ex6 --export-dir /user/hive/warehouse/ab.db/ex6;


