 /*
  * Package tk.tak.hadoop.myhadoop.work.entity
  * FileName: Big
  * Author:   Tak
  * Date:     2018/10/9 23:38
  */
 package tk.tak.hadoop.myhadoop.work.entity;

 import java.util.List;

 /**
  * Author:      Tak
  * Date:        2018/10/9 23:38
  */
 public class Big {

	 private String name;
	 private String code;
	 private List<City> city;

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

	 public void setCity(List<City> city) {
		 this.city = city;
	 }

	 public List<City> getCity() {
		 return city;
	 }

	 @Override
	 public String toString() {
		 return "Big{" +
				 "name='" + name + '\'' +
				 ", code='" + code + '\'' +
				 ", city=" + city +
				 '}';
	 }

 }
