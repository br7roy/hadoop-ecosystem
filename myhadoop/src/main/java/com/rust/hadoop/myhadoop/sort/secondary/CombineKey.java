 /*
  * Package com.rust.hadoop.myhadoop.sort.secondary.mr
  * FileName: CombineKey
  * Author:   Rust
  * Date:     2018/10/20 21:44
  */
 package com.rust.hadoop.myhadoop.sort.secondary;

 import org.apache.hadoop.io.WritableComparable;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;

 /**
  * 创建组合Key
  *
  * @author Rust
  */
 public class CombineKey implements WritableComparable<CombineKey> {
	 private int year;
	 private int temp;

	 public CombineKey() {
	 }

	 public CombineKey(int year, int temp) {
		 this.year = year;
		 this.temp = temp;
	 }

	 public int getYear() {
		 return year;
	 }

	 public void setYear(int year) {
		 this.year = year;
	 }

	 public int getTemp() {
		 return temp;
	 }

	 public void setTemp(int temp) {
		 this.temp = temp;
	 }

	 @Override
	 public int compareTo(CombineKey o) {
		 int oYear = o.getYear();
		 int oTemp = o.getTemp();
		 if (this.year != oYear) {
			 // 年份升序
			 return this.year - oYear;
		 } else {
			 // 气温降序
			 return oTemp - this.temp;
		 }
	 }

	 @Override
	 public void write(DataOutput out) throws IOException {
		 out.writeInt(year);
		 out.writeInt(temp);
	 }

	 @Override
	 public void readFields(DataInput in) throws IOException {
		 this.year = in.readInt();
		 this.temp = in.readInt();
	 }

	 @Override
	 public String toString() {
		 return year + " " + temp;
	 }
 }
