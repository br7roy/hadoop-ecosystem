package com.rust.scala.cdh

/**
  * 从HDFS将数据导入mysql
  * @author Rust
  */
object Text5 {

  /*
  You have been given following mysql database details as well as other info.
    user=retail_dba
  password=cloudera
  database=retail_db
  jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  Please accomplish following.
  1. Create a table in retailedb with following definition.
  CREATE table departments_export (department_id int(11), department_name varchar(45),
    created_date T1MESTAMP DEFAULT NOWQ);
  2. Now import the data from following directory into departments_export table,
  /user/cloudera/departments new
      Answer:
    See the explanation for Step by Step Solution and configuration.
  Explanation:
    Solution :
  Step 1 : Login to mysql db
    mysql --user=retail_dba -password=cloudera
  show databases; use retail_db; show tables;
  step 2 : Create a table as given in problem statement.
  CREATE table departments_export (departmentjd int(11), department_name varchar(45),
    created_date T1MESTAMP DEFAULT NOW()); show tables;
  Step 3 : Export data from /user/cloudera/departmentsnew to new table departments_export sqoop
  export -connect jdbc:mysql://quickstart:3306/retail_db \
    -username retaildba \
  --password cloudera \
    --table departments_export \
    -export-dir /user/cloudera/departments_new \
    -batch
  Step 4 : Now check the export is correctly done or not. mysql -user*retail_dba - password=cloudera
  show databases; use retail _db;
  show tables;
  select' from departments_export;
  */

}
