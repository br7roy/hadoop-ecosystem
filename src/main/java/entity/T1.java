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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Rust
 */
@Entity
@Table(name = "t1")
public class T1 {
	@Id
	private String no;
	@Column
	private String name;
	@Column
	private int age;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNo() {
		return no;
	}

	public void setNo(String no) {
		this.no = no;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
}
