package tk.tak.hadoop.myhadoop.serialize;

import java.io.Serializable;

public class Dog implements Serializable {
	/**
	 * 反串行
	 */
	private static final long serialVersionUID = 3621048420579402762L;
	private String name;

	public Dog(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}


}
