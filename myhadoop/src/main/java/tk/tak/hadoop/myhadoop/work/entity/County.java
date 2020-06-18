 /*
  * Package tk.tak.hadoop.myhadoop.work.entity
  * FileName: County
  * Author:   Tak
  * Date:     2018/10/9 23:36
  */
 package tk.tak.hadoop.myhadoop.work.entity;

 /**
  * Author:      Tak
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
