package com.yahoo.ycsb.measurements.exporter;

import java.io.IOException;
import java.io.OutputStream;

import com.yahoo.ycsb.util.DBManager;
import com.yahoo.ycsb.util.WorkloadType;

/**
 * Write human readable text. Tries to emulate the previous print report method.
 */
public class JDBCMeasurementsExporter implements MeasurementsExporter {

	private DBManager db;
	private Long runId = null;
	private Long tempId = null;

	public JDBCMeasurementsExporter(OutputStream os) {
		this.db = new DBManager();
		db.connect();
		db.initDB();
	}

	private void execute(String metric, String measurement, Object o) {
		String q = "";
		if ("OVERALL".equals(metric)) {
			if (measurement.startsWith("RunTime")) {
				q = "UPDATE runs SET runtime=" + (Double) o + " WHERE id="
						+ runId + ";";
			} else if (measurement.startsWith("Throughput")) {
				q = "UPDATE runs SET throughput=" + (Double) o + " WHERE id="
						+ runId + ";";
			}
			db.executeUpdate(q);
		} else if ("UPDATE".equals(metric) || "READ".equals(metric)
				|| "CLEANUP".equals(metric)) {
			if (measurement.startsWith("Operations")) {
				WorkloadType type = null;
				if ("UPDATE".equals(metric))
					type = WorkloadType.UPDATE;
				if ("READ".equals(metric))
					type = WorkloadType.READ;
				if ("CLEANUP".equals(metric))
					type = WorkloadType.CLEANUP;
				q = "INSERT INTO workloads (run_id, type, operations) VALUES ("
						+ runId + ", " + type.ordinal() + ", " + (Integer) o
						+ ");";
				tempId = db.executeUpdate(q);
			} else {
				if (measurement.startsWith("Average")) {
					q = "UPDATE workloads SET avglat=" + (Double) o
							+ " WHERE id=" + tempId + ";";
				} else if (measurement.startsWith("Min")) {
					q = "UPDATE workloads SET minlat=" + (Integer) o
							+ " WHERE id=" + tempId + ";";
				} else if (measurement.startsWith("Max")) {
					q = "UPDATE workloads SET maxlat=" + (Integer) o
							+ " WHERE id=" + tempId + ";";
				} else if (measurement.startsWith("95th")) {
					q = "UPDATE workloads SET 95lat=" + (Integer) o
							+ " WHERE id=" + tempId + ";";
				} else if (measurement.startsWith("99th")) {
					q = "UPDATE workloads SET 99lat=" + (Integer) o
							+ " WHERE id=" + tempId + ";";
				} else if (measurement.startsWith("Return")) {
					q = "UPDATE workloads SET 0return=" + (Integer) o
							+ " WHERE id=" + tempId + ";";
				} else {
					Integer m = null;
					try {
						m = Integer.parseInt(measurement);
					} catch (NumberFormatException ex) {
						// last operation starts with <
						m = Integer.parseInt(measurement.substring(1));
					}
					q = "INSERT INTO operations (workload_id, tm, ct) VALUES ("
							+ tempId + ", " + m + ", " + (Integer) o + ");";
				}
				db.executeUpdate(q);
			}
		} else {
			// should not occur
		}
	}

	public void write(String metric, String measurement, int i)
			throws IOException {
		execute(metric, measurement, (Integer) i);
		System.out.println("[" + metric + "], " + measurement + ", " + i);
	}

	public void write(String metric, String measurement, double d)
			throws IOException {
		execute(metric, measurement, (Double) d);
		System.out.println("[" + metric + "], " + measurement + ", " + d);
	}

	public void writeConfig(String arg) {
		// is called before exportMeasurements from the client
		String q = null;
		if (runId == null) {
			q = "INSERT INTO runs (runtime) VALUES (" + Double.valueOf(0)
					+ ");";
			runId = db.executeUpdate(q);
		}
		q = "INSERT INTO configurations (run_id, arg) VALUES (" + runId
				+ ", ?);";
		db.executePreparedUpdate(q, arg);
	}

	public void close() throws IOException {
		db.disconnect();
	}

}
