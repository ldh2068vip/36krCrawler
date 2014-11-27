package org.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectionFactory {
	// 创建一个自身的实例
	private static ConnectionFactory connectionFactory = new ConnectionFactory();

	// 注册驱动程序
	private ConnectionFactory()// 使用private而不是public
	{
		try {
			Class.forName("org.h2.Driver");// 如果是其他的数据库则做相应的更改
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	// 返回一个Connection
	public static Connection getConnection() throws SQLException {
		String url = "jdbc:h2:./crawler";// 如果是其他的数据库则做相应的更改
		return DriverManager.getConnection(url, "sa", "");
	}

	// 关闭Connection
	public static void close(Connection connection) throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	// 关闭Statement
	public static void close(Statement statement) throws SQLException {
		if (statement != null && !statement.isClosed()) {
			statement.close();
		}
	}

	// 关闭ResultSet
	public static void close(ResultSet resultSet) throws SQLException {
		if (resultSet != null && !resultSet.isClosed()) {
			resultSet.close();
		}
	}
}