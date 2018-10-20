 /*
  * Package com.rust.hadoop.myhadoop.work.entity
  * FileName: City
  * Author:   Rust
  * Date:     2018/10/9 23:36
  */
 package com.rust.hadoop.myhadoop.work.entity;

 import java.util.List;

 /**
  * Author:      Rust
  * Date:        2018/10/9 23:36
  */
 public class City {

	 private String name;
	 private String code;
	 private List<County> county;

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

	 public void setCounty(List<County> county) {
		 this.county = county;
	 }

	 public List<County> getCounty() {
		 return county;
	 }

	 @Override
	 public String toString() {
		 return "City{" +
				 "name='" + name + '\'' +
				 ", code='" + code + '\'' +
				 ", county=" + county +
				 '}';
	 }
 }
