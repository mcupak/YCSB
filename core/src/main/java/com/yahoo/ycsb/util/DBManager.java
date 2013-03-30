package com.yahoo.ycsb.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBManager {
	// JDBC driver name and database URL
	public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public static final String DB_URL = "jdbc:mysql://localhost/test";

	// Database credentials
	public static final String USER = "";
	public static final String PASS = "";

	// queries
	private static final String DROP_RUNS_QUERY = "DROP TABLE IF EXISTS runs;";
	private static final String DROP_WORKLOADS_QUERY = "DROP TABLE IF EXISTS workloads;";
	private static final String DROP_OPERATIONS_QUERY = "DROP TABLE IF EXISTS operations;";
	private static final String CREATE_RUNS_QUERY = "CREATE TABLE runs (id BIGINT NOT NULL AUTO_INCREMENT, runtime DOUBLE PRECISION, throughput DOUBLE PRECISION, t TIMESTAMP, PRIMARY KEY (id));";
	private static final String CREATE_WORKLOADS_QUERY = "CREATE TABLE workloads (id BIGINT NOT NULL AUTO_INCREMENT, run_id BIGINT NOT NULL, type INT NOT NULL, operations INT, avglat DOUBLE PRECISION, minlat INT, maxlat INT, 95lat INT, 99lat INT, 0return INT, PRIMARY KEY (id), FOREIGN KEY(run_id) REFERENCES runs(id));";
	private static final String CREATE_OPERATIONS_QUERY = "CREATE TABLE operations (id BIGINT NOT NULL AUTO_INCREMENT, workload_id BIGINT NOT NULL, tm INT, ct INT, PRIMARY KEY (id), FOREIGN KEY(workload_id) REFERENCES workloads(id));";

	private Connection conn = null;

	public boolean isConnected() {
		if (conn == null)
			return false;
		try {
			return conn.isValid(0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void initDB() {
		PreparedStatement s;
		try {
			s = conn.prepareStatement(DROP_RUNS_QUERY);
			s.execute();
			s = conn.prepareStatement(DROP_WORKLOADS_QUERY);
			s.execute();
			s = conn.prepareStatement(DROP_OPERATIONS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_RUNS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_WORKLOADS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_OPERATIONS_QUERY);
			s.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void connect() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		}
	}

	public void disconnect() {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	public Long executeUpdate(String q) {
		try {
			PreparedStatement s = conn.prepareStatement(q,
					Statement.RETURN_GENERATED_KEYS);
			int count = s.executeUpdate();
			if (count == 1) {
				// 1 value inserted
				ResultSet results = s.getGeneratedKeys();
				if (results.next()) {
					return results.getLong(1);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
