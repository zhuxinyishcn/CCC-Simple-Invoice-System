package com.cinco;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

import databaseConnect.CloseConnection;
import databaseConnect.DataConnection;

/**
 * This is a collection of utility methods that define a general API for
 * interacting with the database supporting this application.
 *
 */
public class InvoiceData {
	// use the log4j to help me track the error layer
	public static Logger log = Logger.getLogger(InvoiceData.class);

	/**
	 * Method that removes every person record from the database
	 */
	public static void removeAllPersons() {
		Connection conn = DataConnection.dataConnectionFunction();
		String query = "SELECT personCode FROM Person";
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				String personCode = rs.getString("personCode");
				removePerson(personCode);
			}
		} catch (SQLException e) {
			log.info("Deleteing all the Person data");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when Deleteing all the Person data this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Removes the person record from the database corresponding to the provided
	 * <code>personCode</code>
	 * 
	 * @param personCode
	 */
	public static void removePerson(String personCode) {
		removeAllCustomers();
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		int personId = getPersonId(personCode);
		int addressId = getAddressId(personCode);
		String query = "delete from EmailAddress where personId=?";

		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, personId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		query = "DELETE" + " FROM Person" + " WHERE personId = ?";
		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, personId);
			ps.executeUpdate();
		} catch (SQLException e) {
			log.info("DELETE PersonData info.....");
			log.debug("Something wrong when DELETE PersonData info process");
			throw new RuntimeException(e);
		}

//		query = "DELETE" + " FROM Address" + " WHERE addressId = ?";
//		try {
//			ps = conn.prepareStatement(query);
//			ps.setInt(1, addressId);
//			ps.executeUpdate();
//		} catch (SQLException e) {
//			log.info("DELETE Address info.....");
//			log.debug("Something wrong when DELETE Address info this process");
//			throw new RuntimeException(e);
//		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Method to add a person record to the database with the provided data.
	 * 
	 * @param personCode
	 * @param firstName
	 * @param lastName
	 * @param street
	 * @param city
	 * @param state
	 * @param zip
	 * @param country
	 */
	public static void addPerson(String personCode, String firstName, String lastName, String street, String city,
			String state, String zip, String country) {

		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		int AddressForeignKey = addAddress(street, city, state, zip, country);

		String query = "INSERT INTO Person (personCode, firstName, lastName, addressId) VALUES (?, ?, ?, ?)";
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, personCode);
			ps.setString(2, firstName);
			ps.setString(3, lastName);
			ps.setInt(4, AddressForeignKey);
			ps.executeUpdate();
		} catch (SQLException e) {
			log.info("Trying to set the Person");
			log.debug("Something Wrong with the insert Person");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Adds an email record corresponding person record corresponding to the
	 * provided <code>personCode</code>
	 * 
	 * @param personCode
	 * @param email
	 */
	public static void addEmail(String personCode, String email) {
		Connection conn = DataConnection.dataConnectionFunction();
		int personId = getPersonId(personCode);
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "INSERT INTO EmailAddress (email, personId) VALUES (?, ?)";
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, email);
			ps.setInt(2, personId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Method that removes every customer record from the database
	 */
	public static void removeAllCustomers() {

		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;

		String query = "select c.customerId from Customer c";
		try {
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				int customerId = rs.getInt("customerId");
				removeCustomers(customerId);
			}

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	public static void removeCustomers(int customerId) {
		removeAllInvoices();
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "delete from Customer where customerId=?";
		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, customerId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	public static void addCustomer(String customerCode, String type, String primaryContactPersonCode, String name,
			String street, String city, String state, String zip, String country) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		int AddressForeignKey = addAddress(street, city, state, zip, country);
		int personId = getPersonId(primaryContactPersonCode);
		String query = "INSERT INTO Customer (customerCode, type, primaryContact, fullName, addressId, personId) VALUES (?, ?, ?, ?, ?, ?)";
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, customerCode);
			ps.setString(2, type);
			ps.setString(3, primaryContactPersonCode);
			ps.setString(4, name);
			ps.setInt(5, AddressForeignKey);
			ps.setInt(6, personId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Removes all product records from the database
	 */
	public static void removeAllProducts() {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "select  p.productCode from Product p";
		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			// load all the Person detail information
			while (rs.next()) {
				String productCode = rs.getString("productCode");
				removeProduct(productCode);
			}
		} catch (SQLException e) {
			log.info("Retrieving all major field the PersonData info.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when getting (personCode,personCode, firstName, lastName,address) this process");

		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Removes a particular product record from the database corresponding to the
	 * provided <code>productCode</code>
	 * 
	 * @param assetCode
	 */
	public static void removeProduct(String productCode) {
		int productId = getProductId(productCode);
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;

		String query = "delete from ProductOrder where productId=?";
		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, productId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		query = "delete from Product where productId=?";
		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, productId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Adds an equipment record to the database with the provided data.
	 */
	public static void addEquipment(String productCode, String name, Double pricePerUnit) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "INSERT INTO Product (productCode, type, name, pricePerUnit) VALUES (?, ?, ?, ?)";
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, productCode);
			ps.setString(2, "E");
			ps.setString(3, name);
			ps.setDouble(4, pricePerUnit);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Adds an license record to the database with the provided data.
	 */
	public static void addLicense(String productCode, String name, double serviceFee, double annualFee) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "INSERT INTO Product (productCode, type, name, serviceFee, annualLicenseFee) VALUES (?, ?, ? ,?, ?)";
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, productCode);
			ps.setString(2, "L");
			ps.setString(3, name);
			ps.setDouble(4, serviceFee);
			ps.setDouble(5, annualFee);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Adds an consultation record to the database with the provided data.
	 */
	public static void addConsultation(String productCode, String name, String consultantPersonCode, Double hourlyFee) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;

		int consultantId = getPersonId(consultantPersonCode);

		String query = "INSERT INTO Product ( productCode, type, name,  consultantPersonCode, hourlyFee, consultantId) VALUES ( ?, ?, ?, ?, ?, ?)";
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, productCode);
			ps.setString(2, "C");
			ps.setString(3, name);
			ps.setString(4, consultantPersonCode);
			ps.setDouble(5, hourlyFee);
			ps.setInt(6, consultantId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Removes all invoice records from the database
	 */
	public static void removeAllInvoices() {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "select invoiceCode from Invoice";
		try {
			ps = conn.prepareStatement(query);
			rs = ps.executeQuery();
			while (rs.next()) {
				String invoiceCode = rs.getString("invoiceCode");
				removeInvoice(invoiceCode);
			}

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Removes the invoice record from the database corresponding to the provided
	 * <code>invoiceCode</code>
	 * 
	 * @param invoiceCode
	 */
	public static void removeInvoice(String invoiceCode) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		int invoiceId = getInvoiceId(invoiceCode);
		removeAllProducts();
		String query = "delete FROM Invoice WHERE invoiceId = ?";
		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, invoiceId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Adds an invoice record to the database with the given data.
	 */
	public static void addInvoice(String invoiceCode, String customerCode, String salesPersonCode) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;

		int salesPersonId = getPersonId(salesPersonCode);

		String query = "SELECT c.customerId FROM Customer c WHERE c.customerCode = ?";
		int customerId = 0;
		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			ps.setString(1, customerCode);
			rs = ps.executeQuery();
			// load all the Person detail information
			if (rs.next()) {
				customerId = rs.getInt("customerId");
			} else {
				log.info("it seems we don't have this person in database");
			}
		} catch (SQLException e) {
			log.info("Retrieving get personId PersonData info.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when get personId this process");
			throw new RuntimeException(e);
		}

		query = "INSERT INTO Invoice (invoiceCode, customerId, personId) VALUES (?, ?, ?)";

		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, invoiceCode);
			ps.setInt(2, customerId);
			ps.setInt(3, salesPersonId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);

	}

	/**
	 * Adds a particular equipment (corresponding to <code>productCode</code> to an
	 * invoice corresponding to the provided <code>invoiceCode</code> with the given
	 * number of units
	 */
	public static void addEquipmentToInvoice(String invoiceCode, String productCode, int numUnits) {
		addConsultationToInvoice(invoiceCode, productCode, numUnits);
	}

	/**
	 * Adds a particular equipment (corresponding to <code>productCode</code> to an
	 * invoice corresponding to the provided <code>invoiceCode</code> with the given
	 * begin/end dates
	 */
	public static void addLicenseToInvoice(String invoiceCode, String productCode, String startDate, String endDate) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		int productId = getProductId(productCode);
		int invoiceId = getInvoiceId(invoiceCode);

		String query = "INSERT INTO ProductOrder (productId, startDate, endDate, invoiceId) VALUES ( ?, ?, ?, ?);";

		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, productId);
			ps.setString(2, startDate);
			ps.setString(3, endDate);
			ps.setInt(4, invoiceId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	/**
	 * Adds a particular equipment (corresponding to <code>productCode</code> to an
	 * invoice corresponding to the provided <code>invoiceCode</code> with the given
	 * number of billable hours.
	 */
	public static void addConsultationToInvoice(String invoiceCode, String productCode, double numHours) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		int productId = getProductId(productCode);
		int invoiceId = getInvoiceId(invoiceCode);

		String query = "INSERT INTO ProductOrder (productId, units, invoiceId) VALUES ( ?, ?, ?)";

		try {
			ps = conn.prepareStatement(query);
			ps.setInt(1, productId);
			ps.setDouble(2, numHours);
			ps.setInt(3, invoiceId);
			ps.executeUpdate();

		} catch (SQLException e) {
			log.info("Retrieving all Email Address field each Person.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when retrieve email address this process");
			throw new RuntimeException(e);
		}

		CloseConnection.closeConnectionFunction(conn, ps, rs);
	}

	public static int addAddress(String street, String city, String state, String zip, String country) {
		Connection conn = DataConnection.dataConnectionFunction();

		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "INSERT INTO Address (street, city, STATE, zip, country) VALUES (?, ?, ?, ?, ?)";

		int foreignKeyId;
		try {
			ps = conn.prepareStatement(query);
			ps.setString(1, street);
			ps.setString(2, city);
			ps.setString(3, state);
			ps.setString(4, zip);
			ps.setString(5, country);
			ps.executeUpdate();
			ps = conn.prepareStatement("SELECT LAST_INSERT_ID()");
			rs = ps.executeQuery();
			rs.next();
			foreignKeyId = rs.getInt("LAST_INSERT_ID()");
		} catch (SQLException sqle) {
			log.info("Trying to set the Address");
			log.debug("Something Wrong with the Address");
			throw new RuntimeException(sqle);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
		return foreignKeyId;
	}

	public static int getPersonId(String personCode) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "SELECT p.personId FROM Person p WHERE p.personCode = ?";
		int personId = 0;
		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			ps.setString(1, personCode);
			rs = ps.executeQuery();
			// load all the Person detail information
			if (rs.next()) {
				personId = rs.getInt("personId");
			} else {
				log.info("it seems we don't have this person in database");
			}
		} catch (SQLException e) {
			log.info("Retrieving get personId PersonData info.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when get personId this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
		return personId;
	}

	public static int getInvoiceId(String invoiceCode) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "SELECT i.invoiceId FROM Invoice i WHERE i.invoiceCode =?";
		int invoiceId = 0;
		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			ps.setString(1, invoiceCode);
			rs = ps.executeQuery();
			// load all the Person detail information
			if (rs.next()) {
				invoiceId = rs.getInt("invoiceId");
			} else {
				log.info("it seems we don't have this invoice in database");
			}
		} catch (SQLException e) {
			log.info("Retrieving get personId PersonData info.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when get personId this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
		return invoiceId;
	}

	public static int getProductId(String productCode) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "SELECT p.productId FROM Product p WHERE p.productCode =?";
		int productId = 0;
		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			ps.setString(1, productCode);
			rs = ps.executeQuery();
			// load all the Person detail information
			if (rs.next()) {
				productId = rs.getInt("productId");
			} else {
				log.info("it seems we don't have this product in database");
			}
		} catch (SQLException e) {
			log.info("Retrieving get personId PersonData info.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when get personId this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
		return productId;
	}

	public static int getAddressId(String personCode) {
		Connection conn = DataConnection.dataConnectionFunction();
		PreparedStatement ps = null;
		ResultSet rs = null;
		String query = "SELECT p.addressId FROM Person p WHERE p.personCode =?";
		int addressId = 0;
		try {
			// use PreparedStatement to get the data
			ps = conn.prepareStatement(query);
			ps.setString(1, personCode);
			rs = ps.executeQuery();
			// load all the Person detail information
			if (rs.next()) {
				addressId = rs.getInt("addressId");
			} else {
				log.info("it seems we don't have this person in database");
			}
		} catch (SQLException e) {
			log.info("Retrieving get personId PersonData info.....");
			log.warn("Please check SQL query ");
			log.debug("Something wrong when get personId this process");
			throw new RuntimeException(e);
		}
		CloseConnection.closeConnectionFunction(conn, ps, rs);
		return addressId;
	}

//	public static void main(String[] args) {
//		BasicConfigurator.configure();
//		//removePerson("652md");
//		// removeAllCustomers();
//		removeAllPersons();
//	}

}
