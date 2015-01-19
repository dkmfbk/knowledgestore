package eu.fbk.knowledgestore.datastore;

import com.google.common.collect.Iterables;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.data.XPath;
import eu.fbk.knowledgestore.runtime.DataCorruptedException;
import eu.fbk.knowledgestore.vocabulary.KS;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: alessio
 * Date: 08/09/14
 * Time: 17:57
 * To change this template use File | Settings | File Templates.
 */

public class MySQLDataStore implements DataStore {

//	private String connectionString;
//	private String dbUser;
//	private String dbPass;

	static Logger logger = LoggerFactory.getLogger(MySQLDataStore.class);
	public HikariDataSource dataSource;
	final HikariConfig config = new HikariConfig();

	public MySQLDataStore(String host, String username, String password, String databaseName) {
		config.setMinimumIdle(2); // default = max
		config.setMaximumPoolSize(10); // default = 10
		config.setConnectionTimeout(30000); // default 30000 ms (30 s)
		config.setIdleTimeout(600000); // default 600000 ms (10 m)
		config.setMaxLifetime(1800000); // default 1800000 ms (30 m)
		config.setLeakDetectionThreshold(600000); // default 0 ms = disabled

		config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		config.addDataSourceProperty("serverName", host);
		config.addDataSourceProperty("port", "3306");
		config.addDataSourceProperty("databaseName", databaseName);
		config.addDataSourceProperty("user", username);
		config.addDataSourceProperty("password", password);

		config.addDataSourceProperty("cachePrepStmts", true);
		config.addDataSourceProperty("prepStmtCacheSize", 250);
		config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
		config.addDataSourceProperty("useServerPrepStmts", true);

//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//		} catch (Exception e) {
//			throw new Error(e);
//		}
//		connectionString = "jdbc:mysql://" + host + ":3306/" + databaseName;
//		logger.debug(String.format("Connection string: %s", connectionString));
//		dbUser = username;
//		dbPass = password;
	}

	public class MySQLTransaction implements DataTransaction {

		private Connection con;
		boolean readOnly;

		private final static String insertQuery = "INSERT INTO $tableName (`key`, `value`) VALUES (MD5(?), ?) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)";
		private final static String selectQuery = "SELECT `value` FROM $tableName WHERE `key` = MD5(?)";
		private final static String deleteQuery = "DELETE FROM $tableName WHERE `key` = MD5(?)";
		private final static String countQuery = "SELECT COUNT(*) FROM $tableName";
		private final static String selectAllQuery = "SELECT `value` FROM $tableName";

		HashMap<URI, PreparedStatement> insertBatchStatements = new HashMap<>();

		public MySQLTransaction(boolean readOnly) throws SQLException {
			this.readOnly = readOnly;
			this.con = dataSource.getConnection();
			this.con.setAutoCommit(false);

			insertBatchStatements.put(KS.MENTION, con.prepareStatement(insertQuery.replace("$tableName", "mentions")));
			insertBatchStatements.put(KS.RESOURCE, con.prepareStatement(insertQuery.replace("$tableName", "resources")));

//			this.connect(dbUser, dbPass);
		}

		private void connect(String dbUser, String dbPass) throws SQLException {
//			con = DriverManager.getConnection(connectionString, dbUser, dbPass);
		}

		private String getTableName(URI type) throws IOException {
			if (type.equals(KS.MENTION)) {
				return "mentions";
			}
			else if (type.equals(KS.RESOURCE)) {
				return "resources";
			}
			throw new IOException(String.format("Unknown URI: %s", type));
		}

		private byte[] serializeRecord(Record record) throws IOException {
			ObjectOutput out = null;
			byte[] returnBytes;

			try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
				out = new ObjectOutputStream(bos);
				out.writeObject(record);
				returnBytes = bos.toByteArray();
			} finally {
				if (out != null) {
					out.close();
				}
			}

			return returnBytes;
		}

		private Record unserializeRecord(byte[] bytes) throws IOException {
			ObjectInput in = null;
			Record returnRecord;

			try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
				in = new ObjectInputStream(bis);
				try {
					returnRecord = (Record) in.readObject();
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			return returnRecord;
		}

		@Override
		public Stream<Record> lookup(URI type, Set<? extends URI> ids, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {
			String tableName = getTableName(type);
			List<Record> returns = new ArrayList<>();

			for (URI id : ids) {
				String uri;
				try {
					uri = id.toString();
				} catch (NullPointerException e) {
					throw new IOException(e);
				}

				logger.debug(String.format("Selecting %s", uri));
				String query = selectQuery.replace("$tableName", tableName);
				try {
					PreparedStatement stmt = con.prepareStatement(query);
					stmt.setString(1, uri);

					ResultSet set = stmt.executeQuery();

					while (set.next()) {
						Record r = unserializeRecord(set.getBytes("value"));
						if (properties != null) {
							r.retain(Iterables.toArray(properties, URI.class));
						}
						returns.add(r);
					}
				} catch (SQLException e) {
					throw new IOException(e);
				}

			}

			return Stream.create(returns);
		}

		@Override
		public Stream<Record> retrieve(URI type, @Nullable XPath condition, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {
			String tableName = getTableName(type);
			List<Record> returns = new ArrayList<>();

			logger.debug("Retrieving all lines");
			String query = selectAllQuery.replace("$tableName", tableName);

			try {
				Statement statement = con.createStatement();
				ResultSet resultSet = statement.executeQuery(query);

				while (resultSet.next()) {
					Record r = unserializeRecord(resultSet.getBytes("value"));
					if (condition != null && !condition.evalBoolean(r)) {
						continue;
					}

					if (properties != null) {
						r.retain(Iterables.toArray(properties, URI.class));
					}
					returns.add(r);
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}

			return Stream.create(returns);
		}

		@Override
		public long count(URI type, @Nullable XPath condition) throws IOException, IllegalArgumentException, IllegalStateException {
			String tableName = getTableName(type);
			logger.debug("Counting rows");
			String query = countQuery.replace("$tableName", tableName);

			try {
				Statement statement = con.createStatement();
				ResultSet resultSet = statement.executeQuery(query);

				if (resultSet.next()) {
					return resultSet.getLong(1);
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}

			throw new IOException();
		}

		@Override
		public Stream<Record> match(Map<URI, XPath> conditions, Map<URI, Set<URI>> ids, Map<URI, Set<URI>> properties) throws IOException, IllegalStateException {
			return null;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public void store(URI type, Record record) throws IOException, IllegalStateException {
//			String tableName = getTableName(type);
//			String query = insertQuery.replace("$tableName", tableName);

			String uri;
			try {
				uri = record.getID().toString();
			} catch (NullPointerException e) {
				throw new IOException(e);
			}

			logger.debug(String.format("Inserting %s", uri));
			try {
//				PreparedStatement stmt = con.prepareStatement(query);
//				stmt.setString(1, uri);
//				stmt.setBytes(2, serializeRecord(record));
//				stmt.executeUpdate();
				insertBatchStatements.get(type).setString(1, uri);
				insertBatchStatements.get(type).setBytes(2, serializeRecord(record));
				insertBatchStatements.get(type).addBatch();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void delete(URI type, URI id) throws IOException, IllegalStateException {
			String tableName = getTableName(type);

			String uri;
			try {
				uri = id.toString();
			} catch (NullPointerException e) {
				throw new IOException(e);
			}

			logger.debug(String.format("Deleting %s", uri));
			String query = deleteQuery.replace("$tableName", tableName);
			try {
				PreparedStatement stmt = con.prepareStatement(query);
				stmt.setString(1, uri);
				stmt.executeUpdate();
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void end(boolean commit) throws DataCorruptedException, IOException, IllegalStateException {
			try {
				if (commit) {
					for (URI type : insertBatchStatements.keySet()) {
						insertBatchStatements.get(type).executeBatch();
					}
					con.commit();
				}
				else {
					con.rollback();
				}
				con.close();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public DataTransaction begin(boolean readOnly) throws DataCorruptedException, IOException, IllegalStateException {

		MySQLTransaction ret = null;
		try {
			ret = new MySQLTransaction(readOnly);
		} catch (Exception e) {
			throw new IOException(e);
		}

		return ret;
	}

	@Override
	public void init() throws IOException, IllegalStateException {
		dataSource = new HikariDataSource(config);
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public void close() {
		dataSource.close();
		//To change body of implemented methods use File | Settings | File Templates.
	}
}
