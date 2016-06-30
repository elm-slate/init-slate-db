const path = require('path');
const fs = require('fs');
const commander = require('commander');
const prompt = require('prompt');
const R = require('ramda');
const Ru = require('@panosoft/ramda-utils'); // this may not be used in the code base but is here for production patches/tests
const is = require('is_js');
const co = require('co');
const bunyan = require('bunyan');
const bformat = require('bunyan-format');
const {dasherize} = require('underscore.string');
const dbUtils = require('@panosoft/slate-db-utils');

const formatOut = bformat({ outputMode: 'long' });

const logger = bunyan.createLogger({
	name: 'slate-init-db',
	stream: formatOut,
	serializers: bunyan.stdSerializers
});

const exit = exitCode => setTimeout(_ => process.exit(exitCode), 1000);

process.on('uncaughtException', err => {
	logger.error({err: err}, `Uncaught exception:`);
	exit(1);
});
process.on('unhandledRejection', (reason, p) => {
	logger.error("Unhandled Rejection at: Promise ", p, " reason: ", reason);
	exit(1);
});
const handleSignal = signal => process.on(signal, _ => {
	logger.info(`${signal} received.`);
	exit(0);
});
R.forEach(handleSignal, ['SIGINT', 'SIGTERM']);

commander
	.option('--host <name>', 'database server name')
	.option('--user <name>', 'database user name.   must have database creation privileges.  if not specified, prompt for user name.')
	.option('--password <password>', 'database password.  if not specified, prompt for password.')
	.option('--connect-timeout <millisecs>', 'database connection timeout.  if not specified, defaults to 15000 millisecs.')
	.option('-n, --new-database <name>', 'name of database to create')
	.option('-t, --table-type <source | destination>', 'type of events table to create in new database:  must be "source"  or "destination"')
	.option('--dry-run', 'if specified, display run parameters and end program without performing database initialization')
	.parse(process.argv);

const validator = validation => R.compose(R.filter(e => e.length), R.map(v => validation(v) ? '' : v.errMsg(v.param)));
const isValidOptionString = s => s && s.length && is.string(s) && !R.test(/^-/, s);
const isPositiveInteger = n => n && is.integer(n) && is.positive(n);
const isBoolean = b => b === true || b === false;
const isOptional = x => x === undefined;
const validateParameters = args => {
	return validator(v => v.validIf(args, v.param)) ([
		{param: 'user', validIf: (args, param) => isOptional(args[param]) || isValidOptionString(args[param]), errMsg: param => `${param} is invalid:  ${JSON.stringify(args[param])}`},
		{param: 'password', validIf: (args, param) => isOptional(args[param]) || isValidOptionString(args[param]), errMsg: param => `${param} is invalid:  "${args[parm]}"`},
		{param: 'connectTimeout', validIf: (args, param) => isOptional(args[param]) || isPositiveInteger(args[param]), errMsg: param => `${dasherize(param)} is not a positive integer:  ${JSON.stringify(args.__connectTimeout)}`},
		{param: 'host', validIf: (args, param) => isValidOptionString(args[param]), errMsg: param => `${param} is missing or invalid:  ${JSON.stringify(args[param])}`},
		{param: 'newDatabase', validIf: (args, param) => isValidOptionString(args[param]) && R.test(/^[a-zA-Z_][a-zA-Z0-9_]*$/, args[param]), errMsg: param => `${dasherize(param)} is missing or invalid:  ${JSON.stringify(args[param])}`},
		{param: 'tableType', validIf: (args, param) => isValidOptionString(args[param]) && (args[param] === 'source' || args[param] === 'destination'), errMsg: param => `${dasherize(param)} is missing or invalid:  ${JSON.stringify(args[param])}`},
		{param: 'dryRun', validIf: (args, param) => isOptional(args[param]) || isBoolean(args[param]), errMsg: param => `${dasherize(param)} is not valid:  ${JSON.stringify(args[param])}`},
		{param: 'args', validIf: (args, param) => args[param].length === 0, errMsg: param => `Some command arguments were unrecognized.  There may be command arguments after " -- " or syntax errors in the command line.  Unrecognized Command Arguments:  ${args[param]}`}
	]);
};

const logConfig = (args) => {
	logger.info(`Connection Params:  ${JSON.stringify(R.pick(['host', 'user'], args))}`);
	logger.info(`Database to create:  ${JSON.stringify(args.newDatabase)}`);
	logger.info(`Type of events table to create:  ${JSON.stringify(args.tableType)}`);
	if (args.connectTimeout)
		logger.info(`Database Connection Timeout (millisecs):  ${JSON.stringify(args.connectTimeout)}`);
};

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
	yield dbUtils.executeSQLStatement(dbClient, `
		CREATE TABLE id (
			id bigint NOT NULL,
		CONSTRAINT id_pkey PRIMARY KEY (id))
		WITH (OIDS=FALSE)`);
	logger.info(`id table created in database "${dbClientDatabase}"`);
	yield dbUtils.executeSQLStatement(dbClient, `INSERT INTO id (id) VALUES (1)`);
	logger.info(`id table initialized in database "${dbClientDatabase}"`);
});

const createEventsTable = co.wrap(function *(dbClient, dbClientDatabase) {
	yield dbUtils.executeSQLStatement(dbClient, `
		CREATE TABLE events (
			id bigint NOT NULL,
			ts timestamp with time zone NOT NULL,
			entity_id uuid NOT NULL,
			event jsonb NOT NULL,
		CONSTRAINT events_pkey PRIMARY KEY (id))
		WITH (OIDS=FALSE)`
	);
	logger.info(`events table created in database "${dbClientDatabase}"`);
	yield dbUtils.executeSQLStatement(dbClient, `CREATE INDEX events_event_name on events ((event #>> '{name}'))`);
	logger.info(`events_event_name index created in database "${dbClientDatabase}"`);
	yield dbUtils.executeSQLStatement(dbClient, `CREATE INDEX events_ts on events (ts)`);
	logger.info(`events_ts index created in database "${dbClientDatabase}"`);
	yield dbUtils.executeSQLStatement(dbClient, `CREATE INDEX events_entity_id on events (entity_id)`);
	logger.info(`events_entity_id index created in database "${dbClientDatabase}"`);
});

const createSourceDatabaseFunctions = co.wrap(function *(dbClient, dbClientDatabase, sqlStatements) {
	result = yield dbUtils.executeSQLStatement(dbClient, sqlStatements.notifyTriggerFunction);
	logger.info(`events_notify_trigger function created in database "${dbClientDatabase}"`);
	result = yield dbUtils.executeSQLStatement(dbClient, sqlStatements.insertEventsFunction);
	logger.info(`insert_events function created in database "${dbClientDatabase}"`);
});

const createSourceTable = co.wrap(function *(dbClient, dbClientDatabase, sqlStatements) {
	try {
		yield createEventsTable(dbClient, dbClientDatabase);
		yield createSourceDatabaseFunctions(dbClient, dbClientDatabase, sqlStatements);
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

const initializeDatabase = co.wrap(function *(args, sqlStatements) {
	let dbClient;
	try {
		dbClient = yield dbUtils.createClient(dbUtils.createConnectionUrl(R.merge(R.pick(['host', 'user', 'password'], args), {databaseName: args.newDatabase})));
		const dbClientDatabase = dbClient.database;
		dbClient.on('error', function(err) {
			logger.error({err: err}, `Error detected for database "${dbClientDatabase}"`);
			throw err;
		});
		yield R.cond([
			[R.equals('source'), _ => createSourceTable(dbClient, dbClientDatabase, sqlStatements)],
			[R.equals('destination'), _=> createDestinationTable(dbClient, dbClientDatabase)],
			[R.T, _ => {throw new Error(`Program logic error.  events tableType not = "source" or "destination":  ${JSON.stringify(args.tableType)}`)}]
		])(args.tableType);
	}
	finally {
		if (dbClient) {
			dbUtils.close(dbClient);
		}
	}
});

const getCredentialsFromPrompt = options => {
	const schema = {
		properties: {
			user: {
				description: 'Database User',
				type: 'string',
				required: false
			},
			password: {
				description: 'Database Password',
				hidden: true,
				type: 'string',
				required: false,
				// only ask for password if a valid user value was on the command line or was input when prompted
				ask: _ => options.user || (prompt.history('user') && prompt.history('user').value.length > 0) || false
			}
		}
	};
	return new Promise((resolve, reject) => {
		prompt.get(schema, (err, result) => {
			if (err) {
				logger.error(err);
				resolve(err);
			}
			else {
				resolve(result);
			}
		});
	});
};

const getCredentials = co.wrap(function *(options) {
	// use user and password values from command line if present otherwise prompt
	prompt.override = options;
	prompt.start();
	return yield getCredentialsFromPrompt(options);
});

const getParametersToValidate = co.wrap(function *(commander) {
	// change '' to undefined.  the value '' is returned for user and password from getCredentials if user hits only the <enter> key when prompted for value.
	const credentialArgs = R.map(s => s === '' ? undefined : s, yield getCredentials(commander.opts()));
	const toInteger = s => s ? (R.test(/^[0-9]+$/, s) ? Number(s) : NaN) : undefined;
	const getIntegerValues = R.compose(R.map(toInteger), R.pickAll(['connectTimeout']));
	const integerArgs = getIntegerValues(commander.opts());
	const integerArgsAsString = {__connectTimeout: commander.opts()['connectTimeout']};
	// user and password can be specified in the command line.
	const commandLineArgs = R.merge(R.pick(['host', 'user', 'password', 'newDatabase', 'tableType', 'dryRun', 'args'], commander.opts()), R.pick(['args'], commander));
	return R.mergeAll([commandLineArgs, integerArgs, integerArgsAsString, credentialArgs]);
});

const initDb = co.wrap(function * (commander) {
	const parametersToValidate = yield getParametersToValidate(commander);
	const runParameters = R.pickBy((value, key) => value && key !== 'args' && key.substring(0, 2) !== '__', parametersToValidate);
	const errors = yield validateParameters(parametersToValidate);
	if (errors.length > 0) {
		return {err: true, displayHelp: true, message: `Invalid command line arguments:${'\n' + R.join('\n', errors)}`};
	}
	logConfig(runParameters);
	const sqlStatements = {
		notifyTriggerFunction: fs.readFileSync('sql/eventsNotifyTriggerFunction.sql', 'utf8'),
		insertEventsFunction: fs.readFileSync('sql/insertEventsFunction.sql', 'utf8')
	};
	if (runParameters.dryRun === true) {
		return ({message: `--dry-run specified, ending program`});
	}
	dbUtils.setDefaultOptions({logger: logger, connectTimeout: runParameters.connectTimeout});
	const masterConnectionString = dbUtils.createConnectionUrl(R.merge(R.pick(['host', 'user', 'password'], runParameters), {databaseName: 'postgres'}));
	const result = yield doesDatabaseExist(runParameters.newDatabase, masterConnectionString);
	if (result) {
		return {err: true, message: `Database "${runParameters.newDatabase}" already exists.  Processing ended with errors.`};
	}
	yield createDatabase(runParameters.newDatabase, masterConnectionString);
	yield initializeDatabase(runParameters, sqlStatements);
	return {message: 'Processing completed successfully'};
});

initDb(commander)
.then(result =>  {
	if (result.err) {
		logger.error(result.message);
		if (result.displayHelp) {
			commander.help();
		}
		exit(1);
	}
	else {
		logger.info(result.message);
		exit(0);
	}
})
.catch(err => {
	logger.error({err: err}, `Exception in init-db.  Processing ended with errors.`);
	exit(1);
});
