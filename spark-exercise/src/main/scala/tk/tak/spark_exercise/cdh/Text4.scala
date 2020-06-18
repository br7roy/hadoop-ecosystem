package tk.tak.spark_exercise.cdh

/**
  * 将mysql中表数据导入Hive表中(HDFS,hive,warehouse中)
  * @author Tak
  */
object Text4 {

  /*
  You have been given MySQL DB with following details.
  user=retail_dba
  password=cloudera
  database=retail_db
  table=retail_db.categories
  jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  Please accomplish following activities.
  Import Single table categories (Subset data} to hive managed table , where category_id between 1
  and 22

  Answer:
  See the explanation for Step by Step Solution and configuration.
  Explanation:
  Solution :
  Step 1 : Import Single table (Subset data)
  sqoop import --connect jdbc:mysql://quickstart:3306/retail_db -username=retail_dba -
  password=cloudera -table=categories -where "\'category_id\' between 1 and 22" --hive- import --m 1
  Note: Here the ' is the same you find on ~ key
  This command will create a managed table and content will be created in the following directory.
  /user/hive/warehouse/categories
  Step 2 : Check whether table is created or not (In Hive)
  show tables;
  select * from categories;
*/


}
