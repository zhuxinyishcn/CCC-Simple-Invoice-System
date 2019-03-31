/**
 * Author : Xinyi Zhu, Jin Seng Cheng
 * Date : 02/20/2019
 *Cinco Computer Consultants (CCC) project
 * 
 * This is the class helps me to close the connection from the database  
 * it will take Connection, PreparedStatement,ResultSet check if it is 
 * still connect then close it
 */
package databaseConnect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

public class CloseConnection {
	// use the log4j to help me track the error layer
	public static Logger log = Logger.getLogger(CloseConnection.class);

	public static void closeConnectionFunction(Connection conn, PreparedStatement ps, ResultSet rs) {
		try {
			// check ResultSet if still on
			if (rs != null && !rs.isClosed())
				rs.close();
			// check PreparedStatement if still on
			if (ps != null && !ps.isClosed())
				ps.close();
			// check Connection if still on
			if (conn != null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			log.info("Closeing all the Connection, PreparedStatement, ResultSet....");
			log.warn("Please check Connection, PreparedStatement, ResultSet.... ");
			log.debug("Something wrong when Closeing all the Connection, PreparedStatement, ResultSet this process");
			throw new RuntimeException(e);
		}
	}
}
