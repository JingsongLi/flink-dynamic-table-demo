package org.apache.datagen;

import org.apache.commons.lang3.time.DateUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataGen {
	private static Map<String, String> productCateInfo = new HashMap<String, String>();
	private static long orderCnt = 0;
	private static final String RETAIL = "retail";

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			throw new IllegalArgumentException("Two arguments are required: Hostname Port");
		}
		String host = args[0];
		String port = args[1];

		Class.forName("com.mysql.jdbc.Driver");
		String url = String.format("jdbc:mysql://%s:%s/", host, port);
		// wait until mysql is ready
		int retries = 0;
		while (retries++ < 20) {
			try {
				DriverManager.getConnection(url, "root", "123456");
				break;
			} catch (SQLException c) {
				Thread.sleep(3000);
			}
		}
		System.out.println("### Mysql is alive...");
		createDbAndTable(url);
		insertData(url);
	}

	private static void createDbAndTable(String url) throws Exception {
		Connection conn = DriverManager.getConnection(url, "root", "123456");
		Statement stmt = conn.createStatement();
		// create db
		String sql = String.format("CREATE DATABASE %s;", RETAIL);
		stmt.executeUpdate(sql);
		stmt.executeUpdate("USE " + RETAIL);

		// create table orders
		String createOrder = "CREATE TABLE orders (\n" +
				"   order_id VARCHAR(255) NOT NULL PRIMARY KEY,\n" +
				"   product_id VARCHAR(255) COMMENT '000~499',\n" +
				"   cate_id VARCHAR(255) COMMENT '00~99',\n" +
				"   trans_amount BIGINT COMMENT '10000~20000',\n" +
				"   gmt_create VARCHAR(255) NOT NULL\n" +
				");";
		stmt.executeUpdate(createOrder);

		// create tb category
		String createCate = "CREATE TABLE category (\n" +
				"  cate_id VARCHAR(255) NOT NULL PRIMARY KEY COMMENT '00~99',\n" +
				"  parent_cate_id VARCHAR(255) COMMENT '0~9'\n" +
				");";
		stmt.executeUpdate(createCate);
		conn.close();
	}

	private static void insertData(String url) throws Exception {
		Connection conn = DriverManager.getConnection(url + RETAIL, "root", "123456");
		conn.setAutoCommit(false);
		String cateSql = "INSERT INTO category (cate_id, parent_cate_id) VALUES (?, ?)";
		PreparedStatement pStmt1 = conn.prepareStatement(cateSql);

		// insert data into `category`
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 10; j++) {
				pStmt1.setString(1, String.format("%03d", i * 10 + j));
				pStmt1.setString(2, String.format("%03d", i));
				pStmt1.addBatch();
			}
		}
		pStmt1.executeBatch();

		// order insert statement
		String orderSql = "INSERT INTO orders (order_id, product_id, cate_id, trans_amount, gmt_create)"
				+ " VALUES (?, ?, ?, ?, ?)";
		PreparedStatement pStmt2 = conn.prepareStatement(orderSql);

		// create orders for last 5 days
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Random rnd = new Random();
		for (int i = 0; i < 5; i++) {
			// 1000 data for each data
			for (int j = 0; j < 1000; j++) {
				pStmt2.setString(1, (++orderCnt) + ""); // order_id
				String productId = String.format("%03d", rnd.nextInt(500));
				pStmt2.setString(2, productId);
				String cateId = productCateInfo.computeIfAbsent(
						productId, s -> String.format("%03d", rnd.nextInt(100)));
				pStmt2.setString(3, cateId);

				// insert some invalid data: trans_amount < 0
				if (rnd.nextInt(10) < 1) {
					pStmt2.setLong(4, -10000 - rnd.nextInt(10000));
				} else {
					pStmt2.setLong(4, 10000 + rnd.nextInt(10000));
				}

				Date date = DateUtils.addDays(new Date(), -i-1);
				date.setHours(0);
				date.setMinutes(0);
				date = DateUtils.addMinutes(date, rnd.nextInt(1000));
				String ts = sdf.format(date);
				pStmt2.setString(5, ts);
				pStmt2.addBatch();
			}
			pStmt2.executeBatch();
		}
		conn.commit();

		conn.setAutoCommit(true);
		// continous insert data for today...
		try {
			while (true) {
				pStmt2.setString(1, (++orderCnt) + ""); // order_id
				String productId = String.format("%03d", rnd.nextInt(500));
				pStmt2.setString(2, productId);
				String cateId = productCateInfo.computeIfAbsent(
						productId, s -> String.format("%03d", rnd.nextInt(100)));
				pStmt2.setString(3, cateId);
				pStmt2.setLong(4, 10000 + rnd.nextInt(10000));

				Date date = new Date();
				int elapsedMinutes = date.getHours() * 60 + date.getMinutes();
				date.setHours(0);
				date.setMinutes(0);
				date = DateUtils.addMinutes(date, rnd.nextInt(elapsedMinutes));
				String ts = sdf.format(date);
				pStmt2.setString(5, ts);
				pStmt2.execute();

				System.out.println("insert with oder_id: " + orderCnt);

				Thread.sleep(5000);
			}
		} finally {
			conn.close();
		}

	}
}
