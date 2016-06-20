const path = require('path');
const fs = require('fs');
const program = require('commander');
const R = require('ramda');
const is = require('is_js');
const co = require('co');
const bunyan = require('bunyan');
const dbUtils = require('@panosoft/slate-db-utils');

const logger = bunyan.createLogger({
	name: 'slate-init-db',
	serializers: bunyan.stdSerializers
});

const exit = exitCode => setTimeout(() => process.exit(exitCode), 1000);

process.on('uncaughtException', err => {
	logger.error({err: err}, `Uncaught exception:`);
	exit(1);
});
process.on('unhandledRejection', (reason, p) => {
	logger.error("Unhandled Rejection at: Promise ", p, " reason: ", reason);
	exit(1);
});
process.on('SIGINT', () => {
	logger.info(`SIGINT received.`);
	exit(0);
});
process.on('SIGTERM', () => {
	logger.info(`SIGTERM received.`);
	exit(0);
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
	if (arguments.args.length > 0)
		errors = R.append(`Some command arguments exist after processing command options.  There may be command options after " -- " in the command line.  Unprocessed Command Arguments:  ${program.args}`, errors);
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
/////////////////////////////////////////////////////////////////////////////////////
//  validate configuration
/////////////////////////////////////////////////////////////////////////////////////
const argumentErrors = validateArguments(program);

if (argumentErrors.length > 0) {
	logger.error(`Invalid command line arguments:${'\n' + R.join('\n', argumentErrors)}`);
	program.help();
	process.exit(2);
}
// get absolute name so logs will display absolute path
const configFilename = path.isAbsolute(program.configFilename) ? program.configFilename : path.resolve('.', program.configFilename);

let config;

try {
	logger.info(`${'\n'}Config File Name:  "${configFilename}"${'\n'}`);
	config = require(configFilename);
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

const createEventsNotifyTriggerFunctionString = fs.readFileSync('sql/eventsNotifyTriggerFunction.sql', 'utf8');
const createInsertEventsFunctionString = fs.readFileSync('sql/insertEventsFunction.sql', 'utf8');

logConfig(config, program);
if (program.dryRun) {
	logger.info(`--dry-run specified, ending program`);
	process.exit(2);
}
/////////////////////////////////////////////////////////////////////////////////////
//  to reach this point, configuration must be valid and --dry-run was not specified
/////////////////////////////////////////////////////////////////////////////////////
const doesDatabaseExist = co.wrap(function *(databaseName, connectionString) {
	let dbClient;
	try {
		dbClient = yield dbUtils.createClient(connectionString);
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
			throw new Error(`Result rows returned from SELECT statement is invalid.  Database "${dbClientDatabase}"`);
		}
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const createDatabase = co.wrap(function *(databaseName, connectionString) {
	let dbClient;
	try {
		dbClient = yield dbUtils.createClient(connectionString);
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database "${dbClientDatabase}"`);
			throw err;
		});
		const sqlStatement = `CREATE DATABASE "${databaseName}"`;
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

const createAndInitializeIdTable = co.wrap(function *(dbClient, dbClientDatabase) {
	var sqlStatement =
		`CREATE TABLE id (` +
		`	id bigint NOT NULL, ` +
		`CONSTRAINT id_pkey PRIMARY KEY (id)) ` +
		`WITH (OIDS=FALSE)`;
	yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	logger.info(`id table created in database "${dbClientDatabase}"`);
	sqlStatement = `INSERT INTO id (id) VALUES (1)`;
	yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	logger.info(`id table initialized in database "${dbClientDatabase}"`);
});

const createEventsTable = co.wrap(function *(dbClient, dbClientDatabase) {
	var sqlStatement =
		`CREATE TABLE events (` +
		`	id bigint NOT NULL, ` +
		`	ts timestamp with time zone NOT NULL, ` +
		`	entity_id uuid NOT NULL, ` +
		`	event jsonb NOT NULL, ` +
		`CONSTRAINT events_pkey PRIMARY KEY (id)) ` +
		`WITH (OIDS=FALSE)`;
	yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	logger.info(`events table created in database "${dbClientDatabase}"`);
	sqlStatement = `CREATE INDEX events_event_name on events ((event #>> '{name}'))`;
	yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	logger.info(`events_event_name index created in database "${dbClientDatabase}"`);
	sqlStatement = `CREATE INDEX events_ts on events (ts)`;
	yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	logger.info(`events_ts index created in database "${dbClientDatabase}"`);
	sqlStatement = `CREATE INDEX events_entity_id on events (entity_id)`;
	yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
	logger.info(`events_entity_id index created in database "${dbClientDatabase}"`);

});

const createSourceDatabaseFunctions = co.wrap(function *(dbClient, dbClientDatabase) {
	result = yield dbUtils.executeSQLStatement(dbClient, createEventsNotifyTriggerFunctionString);
	logger.info(`events_notify_trigger function created in database "${dbClientDatabase}"`);
	result = yield dbUtils.executeSQLStatement(dbClient, createInsertEventsFunctionString);
	logger.info(`insert_events function created in database "${dbClientDatabase}"`);
});

const createSourceTable = co.wrap(function *(dbClient, dbClientDatabase) {
	try {
		yield createEventsTable(dbClient, dbClientDatabase);
		yield createSourceDatabaseFunctions(dbClient, dbClientDatabase);
		var sqlStatement = `CREATE TRIGGER events_table_trigger AFTER INSERT ON events FOR EACH ROW EXECUTE PROCEDURE events_notify_trigger()`;
		result = yield dbUtils.executeSQLStatement(dbClient, sqlStatement);
		logger.info(`event_table_trigger created in database "${dbClientDatabase}"`);
		yield createAndInitializeIdTable(dbClient, dbClientDatabase);
	}
	catch(err) {
		logger.error({err: err}, `Create source table failed for database "${dbClientDatabase}"`);
		throw err;
	}
});

const createDestinationTable = co.wrap(function *(dbClient, dbClientDatabase) {
	try {
		yield createEventsTable(dbClient, dbClientDatabase);
	}
	catch(err) {
		logger.error({err: err}, `Create destination table failed for database "${dbClientDatabase}"`);
		throw err;
	}
});

const initializeDatabase = co.wrap(function *(newDatabaseName, tableType) {
	let dbClient;
	try {
		dbClient = yield dbUtils.createClient(dbUtils.createConnectionUrl(R.merge(R.pick(['host', 'user', 'password'], config.connectionParams),
													{databaseName: newDatabaseName})));
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database "${dbClientDatabase}"`);
			throw err;
		});
		if (tableType === 'source') {
			yield createSourceTable(dbClient, dbClientDatabase);
		}
		else if (tableType === 'destination') {
			yield createDestinationTable(dbClient, dbClientDatabase);
		}
		else {
			throw new Error(`Program logic error.  events tableType not = "source" or "destination":  "${tableType}"`);
		}
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const initDb = co.wrap(function * (newDatabaseName, tableType) {
	dbUtils.setDefaultOptions({logger: logger, connectTimeout: config.connectTimeout});
	const masterConnectionString = dbUtils.createConnectionUrl(R.merge(R.pick(['host', 'user', 'password'], config.connectionParams), {databaseName: 'postgres'}));
	const result = yield doesDatabaseExist(newDatabaseName, masterConnectionString);
	if (result) {
		return {err: true, message: `Database "${newDatabaseName}" already exists.  Processing ended with errors.`};
	}
	else {
		yield createDatabase(newDatabaseName, masterConnectionString);
		yield initializeDatabase(newDatabaseName, tableType);
		return {message: 'Processing completed successfully'};
	}
});

initDb(program.newDatabase, program.tableType)
.then(result =>  {
	if (result.err) {
		logger.error(result.message);
		exit(0);
	}
	else {
		logger.info(result.message);
		exit(1);
	}
})
.catch(err => {
	logger.error({err: err}, `Exception in init-db.  Processing ended with errors.`);
	exit(1);
});
