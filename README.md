# hadoopFinalProject
Final project of learning path https://gridu.litmos.com/home/LearningPath/57126

Structure:

    /hadoopFinalProject
        /eventproducer          - Implement Random Events producer
        /UDFip_mask_checker     - UDF function for apply ip to mask
        flume-agent.properties  - properties for Flume agent


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
    beeline
    !connect jdbc:hive2://localhost:10000/ab

4.1 Create database

    ```sql
    CREATE DATABASE ab;
    ```

4.2 Create external Hive table to process data <br/>

        CREATE EXTERNAL TABLE IF NOT EXISTS purchases
        (product_name STRING,
        price FLOAT,
        purchase_dt STRING,
        category STRING,
        ip_address  STRING)
        COMMENT 'External table for purchase data'
        PARTITIONED BY(dt STRING)
        ROW FORMAT
            DELIMITED FIELDS TERMINATED BY ','
        LOCATION '/user/cloudera/events';

Load all partitions <br/>

     ALTER TABLE purchases ADD PARTITION (dt='2018-10-19') LOCATION '/user/cloudera/purchases/2018/10/19';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-20') LOCATION '/user/cloudera/purchases/2018/10/20';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-21') LOCATION '/user/cloudera/purchases/2018/10/21';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-22') LOCATION '/user/cloudera/purchases/2018/10/22';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-23') LOCATION '/user/cloudera/purchases/2018/10/23';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-24') LOCATION '/user/cloudera/purchases/2018/10/24';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-25') LOCATION '/user/cloudera/purchases/2018/10/25';
     ALTER TABLE purchases ADD PARTITION (dt='2018-10-26') LOCATION '/user/cloudera/purchases/2018/10/26';

     Anover way:
     LOAD DATA INPATH '/user/cloudera/events2/2018/10/16' INTO TABLE ab.purchases PARTITION (dt = '2018-10-16');

4.3 Select top 10  most frequently purchased categories and put them into table ex5_1 <br/>

      create table ab2.ex5_1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      as
      select p.category, count(*) as c
      from ab2.purchases as p
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

4.5 Create UDF function <br/>
4.5.1 compile project UDFip_mask_checker<br/>
4.5.2 put file /target/find-country-udf.jar to hdfs<br/>
4.5.3 add jar

    add JAR hdfs:///user/cloudera/find-country-udf.jar;

4.5.4 create function

    create temporary function findCountry as "com.griddynamics.aborgatin.udf.FindCountryUDF" using jar 'hdfs:///user/cloudera/find-country-udf.jar';

4.5.5 check creation

    select findCountry('1.200.133.13');

4.6 Select top 10 countries with the highest money spending and put them to table ex6

    create table ex6 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    as
    select * from
    (
    select a.country, sum(a.price) total_sum
    from
    (
    select p.price, findCountry(p.ip_address) country
    from purchases p
    ) a
    where country is not null
    group by a.country
    order by total_sum desc
    ) b
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


