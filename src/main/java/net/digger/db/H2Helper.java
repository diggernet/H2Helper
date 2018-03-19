package net.digger.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Copyright © 2017  David Walton
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Utility class for H2 Database.
 * Provides table versioning and reduces boilerplate code.
 * 
 * @author walton
 */
public class H2Helper {
	/**
	 * Name of the table version table.
	 */
	private static final String VERSION_TABLE_NAME = "table_version";
	/**
	 * Version of the table version table.
	 */
	private static final int VERSION_TABLE_VERSION = 1;
	private final String connUrl;
	private String user;
	private String password;
	
	/**
	 * Callback used for creating and upgrading database tables.
	 */
	public interface UpgradeCallback {
		public void upgrade(Connection conn, Integer current) throws SQLException;
	}
	
	/**
	 * Callback used for working with PreparedStatement before executing query.
	 */
	public interface PrepareCallback {
		public void prepare(PreparedStatement ps) throws SQLException;
	}

	/**
	 * Callback used for processing query ResultSet.
	 * 
	 * @param <T> Type of data object to return.
	 */
	public interface ResultCallback<T> {
		public T process(ResultSet rs) throws SQLException;
	}

	/**
	 * Callback used for processing update generated keys ResultSet.
	 * 
	 * @param <T> Type of data object to return.
	 */
	public interface GeneratedKeysCallback<T> {
		public T process(int rowCount, ResultSet rs) throws SQLException;
	}

	/**
	 * Create instance of H2DB using given connection URL, and initialize table version table.
	 * 
	 * @param connUrl H2 JDBC connection URL.
	 * @throws ClassNotFoundException If error loading database driver.
	 * @throws SQLException If database error occurs.
	 */
	public H2Helper(String connUrl) throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		this.connUrl = connUrl;
		
		initVersionTable();
	}

	/**
	 * Create instance of H2DB using given database, and initialize table version table.
	 * 
	 * @param datafile Path to database.
	 * @throws IOException If error creating database directory.
	 * @throws ClassNotFoundException If error loading database driver.
	 * @throws SQLException If database error occurs.
	 */
	public H2Helper(Path datafile) throws IOException, ClassNotFoundException, SQLException {
		this(datafile, false);
	}

	/**
	 * Create instance of H2DB using given database, and initialize table version table.
	 * 
	 * @param datafile Path to database.
	 * @param autoServer If true, use AUTO_SERVER=TRUE.
	 * @throws IOException If error creating database directory.
	 * @throws ClassNotFoundException If error loading database driver.
	 * @throws SQLException If database error occurs.
	 */
	public H2Helper(Path datafile, boolean autoServer) throws IOException, ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		Files.createDirectories(datafile.getParent());
		String connUrl = "jdbc:h2:" + datafile.toString();
		if (autoServer) {
			connUrl += ";AUTO_SERVER=TRUE";
		}
		this.connUrl = connUrl;
		
		initVersionTable();
	}

	/**
	 * Create instance of H2DB using given database, and initialize table version table.
	 * 
	 * @param datafile Path to database.
	 * @param autoServerPort Use AUTO_SERVER mode with this port.
	 * @throws IOException If error creating database directory.
	 * @throws ClassNotFoundException If error loading database driver.
	 * @throws SQLException If database error occurs.
	 */
	public H2Helper(Path datafile, int autoServerPort) throws IOException, ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		Files.createDirectories(datafile.getParent());
		connUrl = "jdbc:h2:" + datafile.toString() + ";AUTO_SERVER=TRUE;AUTO_SERVER_PORT=" + autoServerPort;
		
		initVersionTable();
	}

	/**
	 * Sets login credentials to be used by connect() and other methods which do not have a Connection argument.
	 * <p>
	 * If these have not been set, the H2 default ("sa"/"") is used.  For any usage where security matters that
	 * should have been changed, and you will need to provide credentials here.
	 * 
	 * @param user Username to use for connections.
	 * @param password Password to use for connections.
	 */
	public void setCredentials(String user, String password) {
		this.user = user;
		this.password = password;
	}
	
	/**
	 * Opens and returns a connection to the database.
	 * <p>
	 * Uses credentials set by setCredentials().  
	 * If those haven't been set, attempts to use the H2 default values.
	 * 
	 * @return Database connection.
	 * @throws SQLException If database error occurs.
	 */
	public Connection connect() throws SQLException {
		if ((user == null) || (password == null)) {
			return connect("sa", "");
		}
		return connect(user, password);
	}

	/**
	 * Opens and returns a connection to the database as given user.
	 * 
	 * @param user Username for connection.
	 * @param password Password for connection.
	 * @return Database connection.
	 * @throws SQLException If database error occurs.
	 */
	public Connection connect(String user, String password) throws SQLException {
		return DriverManager.getConnection(connUrl, user, password);
	}

	/**
	 * Returns the version of the given table, or null if not exists.
	 * 
	 * @param name Name of table to look up.
	 * @return Version number of table.
	 * @throws SQLException If database error occurs.
	 */
	private Integer getTableVersion(String name) throws SQLException {
		Connection conn = null;
		ResultSet result = null;
		try {
			conn = connect();
			result = conn.getMetaData().getTables(conn.getCatalog(), null, VERSION_TABLE_NAME.toUpperCase(), null);
			if (!result.next()) {
				// version table doesn't exist
				return null;
			}
			result.close();
			
			result = conn.getMetaData().getTables(conn.getCatalog(), null, name.toUpperCase(), null);
			if (!result.next()) {
				// given table doesn't exist
				return null;
			}
			result.close();
			
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT version from ").append(VERSION_TABLE_NAME);
			sql.append(" WHERE name = ?");
			return doQuery(conn, sql.toString(), (ps) -> {
				ps.setString(1, name);
			}, (rs) -> {
				if (rs.next()) {
					return rs.getInt(1);
				}
				return null;
			});
		} finally {
			try {
				if (result != null) {
					result.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {}
		}
	}

	/**
	 * Updates or adds table version in db using the given connection.
	 * Meant to be used inside a transaction.
	 * 
	 * @param conn Database connection to use.
	 * @param name Name of table to update.
	 * @param version Version number to set.
	 * @throws SQLException If database error occurs.
	 */
	private void updateTableVersion(Connection conn, String name, int version) throws SQLException {
		StringBuilder sql = new StringBuilder();
		sql.append("MERGE INTO ").append(VERSION_TABLE_NAME);
		sql.append(" (name, version)");
		sql.append(" KEY (name)");
		sql.append(" VALUES (?, ?)");
		doUpdate(conn, sql.toString(), (ps) -> {
			ps.setString(1, name);
			ps.setInt(2, version);
		});
	}

	/**
	 * Creates or upgrades table version table if necessary.
	 * Always call this before any other DB calls.
	 * 
	 * @throws SQLException If database error occurs.
	 */
	private void initVersionTable() throws SQLException {
		initTable(VERSION_TABLE_NAME, VERSION_TABLE_VERSION, (conn, current) -> {
			StringBuilder sql = new StringBuilder();

			// put alter table queries here, depending on from and to values
			if (current == null) {
				sql.setLength(0);
				sql.append("CREATE TABLE ").append(VERSION_TABLE_NAME).append(" (");
				sql.append(" name VARCHAR_IGNORECASE(255) NOT NULL PRIMARY KEY,");
				sql.append(" version INTEGER NOT NULL");
				sql.append(")");
				doUpdate(conn, sql.toString(), null);
			}
		});
	}

	/**
	 * Checks the version of the given table against the given version, and calls the upgrade callback if necessary.
	 * After successful completion of the upgrade callback, updates table to the given version.
	 * 
	 * @param name Name of table to check.
	 * @param version Desired table version.
	 * @param upgrade Callback to use if the current version is less than desired version.
	 * @throws SQLException If database error occurs.
	 */
	public void initTable(String name, int version, UpgradeCallback upgrade) throws SQLException {
		Integer current = getTableVersion(name);
		if (current != null) {
			// table exists
			if (current == version) {
				// nothing to do
				return;
			}
			if (current > version) {
				// current table is newer than calling code
				throw new SQLException("Required version (" + version + ") for table " + name + " is older than current version (" + current + ").");
			}
		}
		// current table needs updating
		Connection conn = null;
		try {
			conn = connect();
			conn.setAutoCommit(false);

			upgrade.upgrade(conn, current);

			updateTableVersion(conn, name, version);
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	/**
	 * Perform database query, doing Connection, PreparedStatement and ResultSet setup and cleanup.
	 * 
	 * @param <T> Type of data object to return.
	 * @param sql Query SQL.
	 * @param pc Callback for preparing query.  Can be null.
	 * @param rc Callback for processing result set.
	 * @return Return value from rc.
	 * @throws SQLException If database error occurs.
	 */
	public <T> T doQuery(String sql, PrepareCallback pc, ResultCallback<T> rc) throws SQLException {
		Connection conn = null;
		try {
			conn = connect();
			return doQuery(conn, sql, pc, rc);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	/**
	 * Perform database query, doing PreparedStatement and ResultSet setup and cleanup.
	 * <p>
	 * Synchronizes on conn, to avoid concurrency problems if the caller is using multiple threads.
	 * 
	 * @param <T> Type of data object to return.
	 * @param conn Database connection to use.
	 * @param sql Query SQL.
	 * @param pc Callback for preparing query.  Can be null.
	 * @param rc Callback for processing result set.
	 * @return Return value from rc.
	 * @throws SQLException If database error occurs.
	 */
	public <T> T doQuery(Connection conn, String sql, PrepareCallback pc, ResultCallback<T> rc) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			synchronized (conn) {
				conn.setAutoCommit(true);
				ps = conn.prepareStatement(sql);
				if (pc != null) {
					pc.prepare(ps);
				}
				rs = ps.executeQuery();
				return rc.process(rs);
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}
	
	/**
	 * Perform database update, doing Connection and PreparedStatement setup and cleanup.
	 * 
	 * @param sql Update SQL.
	 * @param pc Callback for preparing update.  Can be null.
	 * @return Row count.
	 * @throws SQLException If database error occurs.
	 */
	public int doUpdate(String sql, PrepareCallback pc) throws SQLException {
		Connection conn = null;
		try {
			conn = connect();
			return doUpdate(conn, sql, pc);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	/**
	 * Perform database update, doing PreparedStatement setup and cleanup.
	 * <p>
	 * Synchronizes on conn, to avoid concurrency problems if the caller is using multiple threads.
	 * 
	 * @param conn Database connection to use.
	 * @param sql Update SQL.
	 * @param pc Callback for preparing update.  Can be null.
	 * @return Row count.
	 * @throws SQLException If database error occurs.
	 */
	public int doUpdate(Connection conn, String sql, PrepareCallback pc) throws SQLException {
		PreparedStatement ps = null;
		try {
			synchronized (conn) {
				conn.setAutoCommit(true);
				ps = conn.prepareStatement(sql);
				if (pc != null) {
					pc.prepare(ps);
				}
				return ps.executeUpdate();
			}
		} finally {
			if (ps != null) {
				ps.close();
			}
		}
	}
	
	/**
	 * Perform database update, doing Connection, PreparedStatement and ResultSet setup and cleanup.
	 * 
	 * @param <T> Type of data object to return.
	 * @param sql Update SQL.
	 * @param pc Callback for preparing update.  Can be null.
	 * @param gkc Callback for processing generated keys result set.
	 * @return Return value from gkc.
	 * @throws SQLException If database error occurs.
	 */
	public <T> T doUpdate(String sql, PrepareCallback pc, GeneratedKeysCallback<T> gkc) throws SQLException {
		Connection conn = null;
		try {
			conn = connect();
			return doUpdate(conn, sql, pc, gkc);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	/**
	 * Perform database update, doing PreparedStatement and ResultSet setup and cleanup.
	 * <p>
	 * Synchronizes on conn, to avoid concurrency problems if the caller is using multiple threads.
	 * 
	 * @param <T> Type of data object to return.
	 * @param conn Database connection to use.
	 * @param sql Update SQL.
	 * @param pc Callback for preparing update.  Can be null.
	 * @param gkc Callback for processing generated keys result set.
	 * @return Return value from gkc.
	 * @throws SQLException If database error occurs.
	 */
	public <T> T doUpdate(Connection conn, String sql, PrepareCallback pc, GeneratedKeysCallback<T> gkc) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			synchronized (conn) {
				conn.setAutoCommit(true);
				ps = conn.prepareStatement(sql);
				if (pc != null) {
					pc.prepare(ps);
				}
				int count = ps.executeUpdate();
				rs = ps.getGeneratedKeys();
				return gkc.process(count, rs);
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}
	
	/**
	 * Perform database batch update transaction, doing Connection and PreparedStatement setup and cleanup.
	 * 
	 * @param sql Batch update SQL.
	 * @param pc Callback for preparing batch update.  Can be null.
	 * @return Array of row counts.
	 * @throws SQLException If database error occurs.
	 */
	public int[] doBatchUpdate(String sql, PrepareCallback pc) throws SQLException {
		Connection conn = null;
		try {
			conn = connect();
			return doBatchUpdate(conn, sql, pc);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	/**
	 * Perform database batch update transaction, doing PreparedStatement setup and cleanup.
	 * <p>
	 * Synchronizes on conn, to avoid concurrency problems if the caller is using multiple threads.
	 * 
	 * @param conn Database connection to use.
	 * @param sql Batch update SQL.
	 * @param pc Callback for preparing batch update.  Can be null.
	 * @return Array of row counts.
	 * @throws SQLException If database error occurs.
	 */
	public int[] doBatchUpdate(Connection conn, String sql, PrepareCallback pc) throws SQLException {
		PreparedStatement ps = null;
		synchronized (conn) {
			try {
				conn.setAutoCommit(false);
				ps = conn.prepareStatement(sql);
				if (pc != null) {
					pc.prepare(ps);
				}
				int[] result = ps.executeBatch();
				conn.commit();
				return result;
			} catch (SQLException e) {
				try {
					conn.rollback();
				} catch (SQLException e1) {}
				throw e;
			} finally {
				if (ps != null) {
					ps.close();
				}
			}
		}
	}
}
