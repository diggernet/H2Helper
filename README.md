# H2Helper
Utility class for H2 database, which provides table versioning, and methods to simplify database calls. 
Intended primarily to simplify using H2 as an application's embedded data store (similar to common usage of SQLite).

## Maven configuration

		<dependency>
			<groupId>net.digger</groupId>
			<artifactId>h2-helper</artifactId>
			<version>1.1.0</version>
		</dependency>

## Usage
When your database access class is instantiated, it should create an instance of H2Helper, which will create
the database if necessary set up the table where H2Helper stores the table versions.
There are a few different ways to do this:

		String connectionUrl = "...";	// JDBC connection URL for database
		H2Helper h2 = new H2Helper(connectionUrl);

		Path databasePath = ...;			// Path to database file base name (H2 will append ".mv.db" to this).
		H2Helper h2 = new H2Helper(databasePath);

		Path databasePath = ...;			// Path to database file base name (H2 will append ".mv.db" to this).
		Map<String, String> options = new HashMap<>();
		options.put(key, value);			// Key/value pairs to add to constructed connection URL.
		H2Helper h2 = new H2Helper(databasePath, options);

		Path databasePath = ...;			// Path to database file base name (H2 will append ".mv.db" to this).
		Map<String, String> options = new HashMap<>();
		options.put(key, value);			// Key/value pairs to add to constructed connection URL.
		String user = "...";				// Username for admin account for this database.
		String password = "...";			// Password for admin account for this database.
		H2Helper h2 = new H2Helper(databasePath, options, user, password);

Once that is done, your class should then call H2Helper.initTable() for each database table, to create the
table if it is a new database, or upgrade the table if its schema has changed:

		int version = ...;	// Table schema version.
		String name = "...";	// Table name.
		h2.initTable(tableName, version, (conn, current) -> {
			// This is an UpgradeCallback implementation, which is called if the table doesn't yet exist
			// or if the current table version is less than the provided version.
			// If called, conn is a Connection to the database, and current is the version of the table
			// as it currently exists in the database (null, if the table doesn't exist).
			if (current == null) {
				// Put code to create the table here.
				// You can see H2Helper.initVersionTable() for a simple example.
				// Upon return, the table version will be set to the version provided in the initTable() call.
				// If an exception is thrown, all changes are rolled back.
				return;
			}
			// If this point is reached, the table exists and is older than the current version.
			// Put code here to modify the table schema and/or data, depending on what is needed
			// to bring the table up to the current version.  For example, you could do something like:
			if (current == 1) {
				upgradeV1toV2(h2, conn);
				current = 2;
			}
			if (current == 2) {
				upgradeV2toV3(h2, conn);
				current = 3;
			}
			// Upon return, the table version will be set to the version provided in the initTable() call.
			// If an exception is thrown, all changes are rolled back.
		});

Once the table has been initialized, it is ready for use.

You can call setCredentials() to access a database with a username/password other than the admin account used to
initialize the database and table(s).  And connect() will set up and return a connection to the database.

H2Helper provides a number of methods, including doQuery(), doUpdate() and doBatchUpdate(), which handle a lot of
the boilerplate necessary for database calls, in order to simplify your code.  They have variations which will
accept an existing database connection, and variations which create and close their own connection automatically.

The doQuery() methods accept a SQL string to execute, an optional PrepareCallback implementation which is called
with a PreparedStatement to assign values to parameters before the call is made, and a ResultCallback implementation
which is called with a ResultSet containing the results of the query.  The final return value is the data object
built and returned by the ResultCallback.

The doUpdate() methods are similar to doQuery(), except they do not accept a ResultCallback, and return the number
of modified rows.  There is also a variation of doUpdate() which accepts a GeneratedKeysCallback implementation.
In that case, the GeneratedKeysCallback is called with the number of modified rows, and a ResultSet containing
data about any auto-generated keys which were created by this query.

Finally, the doBatchUpdate() methods are similar to doUpdate(), except they expect the PrepareCallback to create
one or more sets of parameters (using PreparedStatement.addBatch() for each one).  They return an array of
modified row counts, one for each set of parameters in the batch.

## Dependencies
* [H2 Database](http://www.h2database.com)

## License
H2Helper is provided under the terms of the GNU LGPLv3.
