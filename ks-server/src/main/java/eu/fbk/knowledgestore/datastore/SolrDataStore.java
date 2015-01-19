package eu.fbk.knowledgestore.datastore;

import com.google.common.collect.Iterables;
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

public class SolrDataStore implements DataStore {

	static Logger logger = LoggerFactory.getLogger(SolrDataStore.class);
	public HikariDataSource dataSource;

	public SolrDataStore() {

	}

	public class SolrTransaction implements DataTransaction {

		private Connection con;
		boolean readOnly;

		public SolrTransaction(boolean readOnly) throws SQLException {
			this.readOnly = readOnly;

			// stuff here
		}

		private void connect(String dbUser, String dbPass) throws SQLException {
			// stuff here
		}

		@Override
		public Stream<Record> lookup(URI type, Set<? extends URI> ids, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {
//			String tableName = getTableName(type);
//			List<Record> returns = new ArrayList<>();
//
//			for (URI id : ids) {
//				String uri;
//				try {
//					uri = id.toString();
//				} catch (NullPointerException e) {
//					throw new IOException(e);
//				}
//
//				logger.debug(String.format("Selecting %s", uri));
//				String query = selectQuery.replace("$tableName", tableName);
//				try {
//					PreparedStatement stmt = con.prepareStatement(query);
//					stmt.setString(1, uri);
//
//					ResultSet set = stmt.executeQuery();
//
//					while (set.next()) {
//						Record r = unserializeRecord(set.getBytes("value"));
//						if (properties != null) {
//							r.retain(Iterables.toArray(properties, URI.class));
//						}
//						returns.add(r);
//					}
//				} catch (SQLException e) {
//					throw new IOException(e);
//				}
//
//			}
//
//			return Stream.create(returns);
			return Stream.create();
		}

		@Override
		public Stream<Record> retrieve(URI type, @Nullable XPath condition, @Nullable Set<? extends URI> properties) throws IOException, IllegalArgumentException, IllegalStateException {
//			String tableName = getTableName(type);
//			List<Record> returns = new ArrayList<>();
//
//			logger.debug("Retrieving all lines");
//			String query = selectAllQuery.replace("$tableName", tableName);
//
//			try {
//				Statement statement = con.createStatement();
//				ResultSet resultSet = statement.executeQuery(query);
//
//				while (resultSet.next()) {
//					Record r = unserializeRecord(resultSet.getBytes("value"));
//					if (condition != null && !condition.evalBoolean(r)) {
//						continue;
//					}
//
//					if (properties != null) {
//						r.retain(Iterables.toArray(properties, URI.class));
//					}
//					returns.add(r);
//				}
//			} catch (SQLException e) {
//				throw new IOException(e);
//			}
//
//			return Stream.create(returns);
			return Stream.create();
		}

		@Override
		public long count(URI type, @Nullable XPath condition) throws IOException, IllegalArgumentException, IllegalStateException {
//			String tableName = getTableName(type);
//			logger.debug("Counting rows");
//			String query = countQuery.replace("$tableName", tableName);
//
//			try {
//				Statement statement = con.createStatement();
//				ResultSet resultSet = statement.executeQuery(query);
//
//				if (resultSet.next()) {
//					return resultSet.getLong(1);
//				}
//			} catch (SQLException e) {
//				throw new IOException(e);
//			}
//
//			throw new IOException();
			return 0;
		}

		@Override
		public Stream<Record> match(Map<URI, XPath> conditions, Map<URI, Set<URI>> ids, Map<URI, Set<URI>> properties) throws IOException, IllegalStateException {
			return null;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public void store(URI type, Record record) throws IOException, IllegalStateException {

			System.out.println(type);
			System.out.println(record);
		}

		@Override
		public void delete(URI type, URI id) throws IOException, IllegalStateException {
			// stuff here
		}

		@Override
		public void end(boolean commit) throws DataCorruptedException, IOException, IllegalStateException {
			// stuff here
		}
	}

	@Override
	public DataTransaction begin(boolean readOnly) throws DataCorruptedException, IOException, IllegalStateException {

		SolrTransaction ret = null;
		try {
			ret = new SolrTransaction(readOnly);
		} catch (Exception e) {
			throw new IOException(e);
		}

		return ret;
	}

	@Override
	public void init() throws IOException, IllegalStateException {
	}

	@Override
	public void close() {
	}
}
