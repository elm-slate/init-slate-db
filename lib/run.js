const path = require('path');
const program = require('commander');
const R = require('ramda');
const is = require('is_js');
const co = require('co');
const bunyan = require('bunyan');
const dbUtils = require('@panosoft/slate-db-utils');

var logger = bunyan.createLogger({
	name: 'slate-init-db',
	serializers: bunyan.stdSerializers
});

process.on('uncaughtException', err => {
	logger.error({err: err}, `Uncaught exception:`);
	process.exit(1);
});
process.on('unhandledRejection', (reason, p) => {
	logger.error("Unhandled Rejection at: Promise ", p, " reason: ", reason);
	process.exit(1);
});
process.on('SIGINT', () => {
	logger.info(`SIGINT received.`);
	process.exit(0);
});
process.on('SIGTERM', () => {
	logger.info(`SIGTERM received.`);
	process.exit(0);
});

program
	.option('-c, --config-filename <s>', 'configuration file name')
	.option('-n, --new-database <s>', 'name of database to create')
	.option('-t, --table-type <s>', 'type of events table to create in new database:  must be "source"  or "destination"')
	.option('--dry-run', 'if specified, display run parameters and end program without performing database initialization')
	.parse(process.argv);

const validateArguments = arguments => {
	var errors = [];
	if (!arguments.configFilename || is.not.string(arguments.configFilename))
		errors = R.append('config-filename is invalid:  ' + arguments.configFilename, errors);
	// non-quoted Postgresql identifier must begin with 'a-z' or '_' and remaining characters must be 'a-z', '0-9' or '_'
	if (!arguments.newDatabase || !(/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(arguments.newDatabase))) {
		errors = R.append(`new-database is invalid:  "${arguments.newDatabase}"`, errors);
	}
	if (arguments.tableType !== 'source' && arguments.tableType !== 'destination') {
		errors = R.append(`table-type is invalid:  "${arguments.tableType}"`, errors);
	}
	return errors;
};

const validateConnectionParameters = (parameters, parametersName) => {
	var errors = [];
	if (parameters) {
		if (!parameters.host || is.not.string(parameters.host)) {
			errors = R.append(`${parametersName}.host is missing or invalid:  ${parameters.host}`, errors);
		}
		if (parameters.userName && is.not.string(parameters.userName)) {
			errors = R.append(`${parametersName}.userName is invalid:  ${parameters.userName}`, errors);
		}
		if (parameters.password && is.not.string(parameters.password)) {
			errors = R.append(`${parametersName}.password is invalid:  ${parameters.password}`, errors);
		}
	}
	else {
		errors = R.append(`connection parameters for ${parametersName} are missing or invalid`, errors);
	}
	return errors;
};

const logConfig = (config, arguments) => {
	logger.info(`Connection Params:`, R.pick(['host', 'user'], config.connectionParams));
	logger.info(`Database to create:  "${arguments.newDatabase}"`);
	logger.info(`Type of events table to create:  "${arguments.tableType}"`);
	if (config.connectTimeout)
		logger.info(`Database Connection Timeout (millisecs):`, config.connectTimeout);
};

const argumentErrors = validateArguments(program);

if (argumentErrors.length > 0) {
	logger.error(`Invalid command line arguments:${'\n' + R.join('\n', argumentErrors)}`);
	program.help();
	process.exit(2);
}
// get absolute name so logs will display absolute path
const configFilename = path.isAbsolute(program.configFilename) ? program.configFilename : path.resolve('.', program.configFilename);

try {
	logger.info(`${'\n'}Config File Name:  "${configFilename}"${'\n'}`);
	const config = require(configFilename);
}
catch (err) {
	logger.error({err: err}, `Exception detected processing configuration file:`);
	process.exit(1);
}

var configErrors = [];
configErrors = R.concat(validateConnectionParameters(config.connectionParams, 'config.connectionParams'), configErrors);
if (config.connectTimeout) {
	if (is.not.integer(config.connectTimeout) || is.not.positive(config.connectTimeout)) {
		configErrors = R.append(`config.connectTimeout is invalid:  "${config.connectTimeout}"`, configErrors);
	}
}
if (configErrors.length > 0) {
	logger.error(`Invalid configuration parameters:${'\n' + R.join('\n', configErrors)}`);
	program.help();
	process.exit(2);
}

logConfig(config, program);
if (program.dryRun) {
	logger.info(`--dry-run specified, ending program`);
	process.exit(2);
}
/////////////////////////////////////////////////////////////////////////////////////
//  configuration has been processed
/////////////////////////////////////////////////////////////////////////////////////
const doesDatabaseExist = co.wrap(function *(databaseName, connectionString) {
	try {
		const dbClient = yield dbUtils.createClient(connectionString);
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database ${dbClientDatabase}`);
			throw err;
		});
		const selectStatement = `SELECT 1 as "dbexists" FROM pg_database WHERE datname = $1`;
		const result = yield dbUtils.executeSQLStatement(dbClient, selectStatement, [databaseName]);
		if (result.rows.length === 0) {
			return false;
		}
		else if (result.rows.length === 1 && result.rows[0].dbexists === 1) {
			return true;
		}
		else {
			logger.error({rows: result.rows},  'Invalid result from SELECT statement');
			throw new Error(`Result rows returned from SELECT statement is invalid.  Database ${dbClientDatabase}`);
		}
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const createDatabase = co.wrap(function *(databaseName, connectionString) {
	try {
		const dbClient = yield dbUtils.createClient(connectionString);
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database ${dbClientDatabase}`);
			throw err;
		});
		const sqlStatement = `CREATE DATABASE ${databaseName}`;
		yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	}
	catch(err) {
		logger.error({err: err}, `Create database failed for database "${databaseName}"`);
		throw err;
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const createSourceTable = co.wrap(function *(connectionString) {
	try {
		const dbClient = yield dbUtils.createClient(connectionString);
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database ${dbClientDatabase}`);
			throw err;
		});
		var sqlStatement =
			`CREATE TABLE events (` +
			`id bigserial NOT NULL, ` +
			`eventTimestamp timestamp with time zone NOT NULL, ` +
			`event jsonb NOT NULL, ` +
			`CONSTRAINT events_pkey PRIMARY KEY (id)) ` +
			`WITH (OIDS=FALSE)`;
		yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`events table created in database "${dbClientDatabase}"`);
		sqlStatement =
			`CREATE FUNCTION events_notify_trigger() RETURNS trigger AS $$${'\n'}` +
			`DECLARE${'\n'}` +
			`BEGIN${'\n'}` +
			`    PERFORM pg_notify('eventsinsert', json_build_object('table', TG_TABLE_NAME, 'id', NEW.id )::text);${'\n'}` +
			`    RETURN new;${'\n'}` +
			`END;${'\n'}` +
			`$$ LANGUAGE plpgsql`;
		result = yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`events_notify_trigger function created in database "${dbClientDatabase}"`);
		sqlStatement = `CREATE TRIGGER events_table_trigger AFTER INSERT ON events FOR EACH ROW EXECUTE PROCEDURE events_notify_trigger()`;
		result = yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`event_table_trigger created in database "${dbClientDatabase}"`);
	}
	catch(err) {
		logger.error({err: err}, `Create source table failed for database "${dbClientDatabase}"`);
		throw err;
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const createDestinationTable = co.wrap(function *(connectionString) {
	try {
		const dbClient = yield dbUtils.createClient(connectionString);
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database ${dbClientDatabase}`);
			throw err;
		});
		var sqlStatement =
			`CREATE TABLE events (` +
			`id bigint NOT NULL, ` +
			`eventTimestamp timestamp with time zone NOT NULL, ` +
			`event jsonb NOT NULL, ` +
			`CONSTRAINT events_pkey PRIMARY KEY (id)) ` +
			`WITH (OIDS=FALSE)`;
		yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`events table created in database "${dbClientDatabase}"`);
		sqlStatement = `CREATE INDEX events_event_name on events ((event #>> '{name}'))`;
		yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`events_event_name index created in database "${dbClientDatabase}"`);
		sqlStatement = `CREATE INDEX events_eventtimestamp on events (eventTimestamp)`;
		yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`events_eventtimestamp index created in database "${dbClientDatabase}"`);
	}
	catch(err) {
		logger.error({err: err}, `Create destination table failed for database "${dbClientDatabase}"`);
		throw err;
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const main = co.wrap(function * (newDatabase, tableType) {
	dbUtils.setDefaultOptions({logger: logger, connectTimeout: config.connectTimeout});
	const masterConnectionString = dbUtils.createConnectionUrl(R.merge(R.pick(['host', 'user', 'password'], config.connectionParams), {databaseName: 'postgres'}));
	const result = yield doesDatabaseExist(newDatabase, masterConnectionString);
	if (result) {
		return {err: true, message: `Database "${newDatabase}" already exists.  Processing ended with errors.`};
	}
	else {
		yield createDatabase(newDatabase, masterConnectionString, config);
		const newDbConnectionString = dbUtils.createConnectionUrl(R.merge(R.pick(['host', 'user', 'password'], config.connectionParams), {databaseName: newDatabase}));
		if (tableType === 'source') {
			yield createSourceTable(newDbConnectionString);
		}
		else if (tableType === 'destination') {
			yield createDestinationTable(newDbConnectionString);
		}
		else {
			throw new Error(`Program logic error.  events tableType not = "source" or "destination":  "${tableType}"`);
		}
		return {message: 'Processing completed successfully'};
	}
});

main(program.newDatabase, program.tableType)
.then(result =>  {
	if (result.err)
		logger.error(result.message);
	else
		logger.info(result.message);
})
.catch(err => {
	logger.error({err: err}, `Exception in init-db.  Processing ended with errors.`);
	process.exit(1);
});
