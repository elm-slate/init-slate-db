var config = {
	// optional parameter.  database connection timeout in milliseconds.  default value:  15000.
	connectTimeout: 10000,
	// connection parameters to Postgresql server postgres database
	connectionParams: {
		host: 'localhost',
		// optional parameter.  connection attempt will fail if missing and needed by postgres database.  must have database creation privileges.
		user: 'user1',
		// optional parameter.  connection attempt will fail if missing and needed by postgres database.
		password: 'password1'
	}
};

module.exports = config;