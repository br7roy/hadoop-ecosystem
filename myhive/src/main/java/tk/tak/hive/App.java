package tk.tak.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 通过JDBC访问HIVE
 */
public class App {
	public static void main(String[] args) throws Exception{
		Class.forName("org.apache.hive.jdbc.HiveDriver");

		Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.231.100:10000/hive1", "ubuntu", "123456");
		PreparedStatement preparedStatement = connection.prepareStatement("select * from t");
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {

			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println(id + "," + name + "," + age);
		}
		rs.close();
		preparedStatement.close();
		connection.close();

	}
}
