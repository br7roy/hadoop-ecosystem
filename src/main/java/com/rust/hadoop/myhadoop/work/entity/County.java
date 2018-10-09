 /*
  * Package com.rust.hadoop.myhadoop.work.entity
  * FileName: County
  * Author:   Rust
  * Date:     2018/10/9 23:36
  */
 package com.rust.hadoop.myhadoop.work.entity;

 /**
  * Author:      Rust
  * Date:        2018/10/9 23:36
  */
 public class County {

   private String name;
   private String code;
   public void setName(String name) {
    this.name = name;
   }
   public String getName() {
    return name;
   }

   public void setCode(String code) {
    this.code = code;
   }
   public String getCode() {
    return code;
   }

     @Override
     public String toString() {
         return "County{" +
                 "name='" + name + '\'' +
                 ", code='" + code + '\'' +
                 '}';
     }
 }
