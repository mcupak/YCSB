package com.yahoo.ycsb.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBManager {
	// queries
	private static final String DROP_RUNS_QUERY = "DROP TABLE IF EXISTS runs;";
	private static final String DROP_WORKLOADS_QUERY = "DROP TABLE IF EXISTS workloads;";
	private static final String DROP_OPERATIONS_QUERY = "DROP TABLE IF EXISTS operations;";
	private static final String DROP_CONFIGURATIONS_QUERY = "DROP TABLE IF EXISTS configurations;";
	private static final String CREATE_RUNS_QUERY = "CREATE TABLE runs (id BIGINT NOT NULL AUTO_INCREMENT, runtime DOUBLE PRECISION, throughput DOUBLE PRECISION, t TIMESTAMP, PRIMARY KEY (id));";
	private static final String CREATE_WORKLOADS_QUERY = "CREATE TABLE workloads (id BIGINT NOT NULL AUTO_INCREMENT, run_id BIGINT NOT NULL, type INT NOT NULL, operations INT, avglat DOUBLE PRECISION, minlat INT, maxlat INT, 95lat INT, 99lat INT, 0return INT, PRIMARY KEY (id), FOREIGN KEY(run_id) REFERENCES runs(id));";
	private static final String CREATE_OPERATIONS_QUERY = "CREATE TABLE operations (id BIGINT NOT NULL AUTO_INCREMENT, workload_id BIGINT NOT NULL, tm INT, ct INT, PRIMARY KEY (id), FOREIGN KEY(workload_id) REFERENCES workloads(id));";
	private static final String CREATE_CONFIGURATIONS_QUERY = "CREATE TABLE configurations (id BIGINT NOT NULL AUTO_INCREMENT, run_id BIGINT NOT NULL, arg VARCHAR(500), PRIMARY KEY (id), FOREIGN KEY(run_id) REFERENCES runs(id));";

	private Connection conn = null;
	Properties prop = null;

	public DBManager() {
		prop = new Properties();
		InputStream in = null;
		try {
			in = this.getClass().getResourceAsStream("/db.properties");
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

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
			s = conn.prepareStatement(DROP_CONFIGURATIONS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_RUNS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_WORKLOADS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_OPERATIONS_QUERY);
			s.execute();
			s = conn.prepareStatement(CREATE_CONFIGURATIONS_QUERY);
			s.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void connect() {
		try {
			Class.forName(prop.getProperty("jdbc.driver"));
			conn = DriverManager.getConnection(prop.getProperty("db.url"),
					prop.getProperty("db.user"), prop.getProperty("db.passwd"));
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

	public void executePreparedUpdate(String q, String args) {
		try {
			PreparedStatement s = conn.prepareStatement(q);
			s.setString(1, args);
			s.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
