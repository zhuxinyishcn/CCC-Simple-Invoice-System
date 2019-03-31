/**
 * Author : Xinyi Zhu, Jin Seng Cheng
 * Date : 03/21/2019
 * Cinco Computer Consultants (CCC) project
 * 
 * This class helps me to set up the connection between Mysql database 
 * and my java program
 */
package databaseConnect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class DataConnection {
	// use the log4j to help me track the error layer
	public static Logger log = Logger.getLogger(DataConnection.class);

	// Load the JDBC Driver
	public static Connection dataConnectionFunction() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			System.out.println("InstantiationException: ");
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			System.out.println("IllegalAccessException: ");
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException: ");
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		// Create a connection to my database
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(DatabaseInfo.url, DatabaseInfo.username, DatabaseInfo.password);
		} catch (SQLException e) {
			// log information here!
			log.error("Cannot connect to server, Please double check your DatabaseInfo class", e);
			throw new RuntimeException(e);
		}

		// return Connection to future reference
		return conn;
	}
}
