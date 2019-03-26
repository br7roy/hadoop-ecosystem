/*
 * Package entity
 * FileName: T1
 * Author:   Rust
 * Date:     2019/3/26 17:38
 * Description:
 * email: br7roy@gmail.com
 *===============================================================================================
 *   author：          time：                             version：           desc：
 *   Rust           2019/3/26  17:38                      1.0
 *===============================================================================================
 */
package entity;

import com.google.common.base.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Rust
 */
@Entity
@Table(name = "cf1")
/** 对应列簇 column_family */ public class T1 {

	/**
	 * 这个是rowkey
	 */
	@Id
	private String rowKey;
	@Column
	private String name;
	@Column
	private int age;
	@Column
	private String no;

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getNo() {
		return no;
	}

	public void setNo(String no) {
		this.no = no;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("rowKey", rowKey).add("name", name).add("age", age).add("no", no).toString();
	}
}
