 /*
  * Package com.rust.hadoop.myhadoop.work
  * FileName: AreaDBWritable
  * Author:   Rust
  * Date:     2018/10/9 20:22
  */
 package com.rust.hadoop.myhadoop.work;

 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.WritableComparable;
 import org.apache.hadoop.mapreduce.lib.db.DBWritable;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;
 import java.sql.Timestamp;

 /**
  * FileName:    AreaDBWritable
  * Author:      Rust
  * Date:        2018/10/9
  * Description:
  */
 public class AreaDBWritable implements DBWritable, WritableComparable<AreaDBWritable> {
	 private int id;
	 private String areaRelNo;
	 private String region2code;
	 private String region2name;
	 private String cityCode;
	 private String cityName;
	 private Timestamp createTime;
	 private Timestamp updateTime;
	 private String createBy;
	 private String updateBy;


	 public int getId() {
		 return id;
	 }

	 public void setId(int id) {
		 this.id = id;
	 }

	 public String getAreaRelNo() {
		 return areaRelNo;
	 }

	 public void setAreaRelNo(String areaRelNo) {
		 this.areaRelNo = areaRelNo;
	 }

	 public String getRegion2code() {
		 return region2code;
	 }

	 public void setRegion2code(String region2code) {
		 this.region2code = region2code;
	 }

	 public String getRegion2name() {
		 return region2name;
	 }

	 public void setRegion2name(String region2name) {
		 this.region2name = region2name;
	 }

	 public String getCityCode() {
		 return cityCode;
	 }

	 public void setCityCode(String cityCode) {
		 this.cityCode = cityCode;
	 }

	 public String getCityName() {
		 return cityName;
	 }

	 public void setCityName(String cityName) {
		 this.cityName = cityName;
	 }

	 public Timestamp getCreateTime() {
		 return createTime;
	 }

	 public void setCreateTime(Timestamp createTime) {
		 this.createTime = createTime;
	 }

	 public Timestamp getUpdateTime() {
		 return updateTime;
	 }

	 public void setUpdateTime(Timestamp updateTime) {
		 this.updateTime = updateTime;
	 }

	 public String getCreateBy() {
		 return createBy;
	 }

	 public void setCreateBy(String createBy) {
		 this.createBy = createBy;
	 }

	 public String getUpdateBy() {
		 return updateBy;
	 }

	 public void setUpdateBy(String updateBy) {
		 this.updateBy = updateBy;
	 }

	 @Override
	 public int compareTo(AreaDBWritable o) {
		 return new IntWritable(this.id).compareTo(new IntWritable(o.getId()));
	 }

	 @Override
	 public void write(DataOutput out) throws IOException {
		 out.writeInt(this.id);
		 out.writeUTF(this.areaRelNo);
		 out.writeUTF(this.region2code);
		 out.writeUTF(region2name);
		 out.writeUTF(cityCode);
		 out.writeUTF(cityName);
/*		 out.writeLong(createTime);
		 out.writeLong(updateTime);*/
		 out.writeUTF(createBy);
		 out.writeUTF(updateBy);
	 }

	 @Override
	 public void readFields(DataInput in) throws IOException {

	 }

	 @Override
	 public void write(PreparedStatement statement) throws SQLException {
		 int i = 0;
		 statement.setString(++i, areaRelNo);
		 statement.setString(++i, region2code);
		 statement.setString(++i, region2name);
		 statement.setString(++i, cityCode);
		 statement.setString(++i, cityName);
		 statement.setTimestamp(++i, createTime);
		 statement.setTimestamp(++i, updateTime);
		 statement.setString(++i, createBy);
		 statement.setString(++i, updateBy);
	 }

	 @Override
	 public void readFields(ResultSet resultSet) throws SQLException {

	 }
 }
