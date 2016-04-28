var config = {
	// optional parameter.  database connection timeout in milliseconds.  default value:  15000.
	connectTimeout: 10000,
	// Postgresql database server connection parameters
	connectionParams: {
		host: 'localhost',
		// optional parameter.  connection attempt will fail if missing and needed by postgres database.  must have database creation privileges.
		user: 'user1',
		// optional parameter.  connection attempt will fail if missing and needed by postgres database.
		password: 'password1'
	}
};

module.exports = config;