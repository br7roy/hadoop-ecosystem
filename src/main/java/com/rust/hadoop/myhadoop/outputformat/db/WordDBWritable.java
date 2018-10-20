 /*
  * Package com.rust.hadoop.myhadoop.outputformat.db
  * FileName: WordDBWritable
  * Author:   Rust
  * Date:     2018/10/7 12:56
  */
 package com.rust.hadoop.myhadoop.outputformat.db;

 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.io.WritableComparable;
 import org.apache.hadoop.mapreduce.lib.db.DBWritable;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;

 /**
  * FileName:    WordDBWritable
  * Author:      Rust
  * Date:        2018/10/7
  * Description:
  */
 public class WordDBWritable implements DBWritable, WritableComparable<WordDBWritable> {
	 private String word;
	 private int num;

	 public String getWord() {
		 return word;
	 }

	 public void setWord(String word) {
		 this.word = word;
	 }

	 public int getNum() {
		 return num;
	 }

	 public void setNum(int num) {
		 this.num = num;
	 }

	 @Override
	 public void write(PreparedStatement statement) throws SQLException {
		 statement.setString(1, word);
		 statement.setInt(2, num);
	 }

	 @Override
	 public void readFields(ResultSet resultSet) throws SQLException {

	 }

	 @Override
	 public void write(DataOutput out) throws IOException {
		 out.writeUTF(word);
		 out.writeInt(num);
	 }

	 @Override
	 public void readFields(DataInput in) throws IOException {
		 this.word = in.readUTF();
		 this.num = in.readInt();
	 }

	 public int compareTo(@SuppressWarnings("NullableProblems") WordDBWritable o) {
		 return new Text(this.word).compareTo(new Text(o.word));
	 }
 }
