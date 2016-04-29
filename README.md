# slate-init-db
Creates and initializes a Postgresql database for use by the [`slate-replicator`](https://github.com/panosoft/slate-replicator)

The purpose of slate-init-db is to create and initialize a new `Postgresql` database with either a `source` or `destination` events table to be used by the slate-replicator.

# Installation
> npm install @panosoft/slate-init-db

# Usage

#### Run slate-init-db

    node run.js [options]

  Options:

    -h, --help                 output usage information
    -c, --config-filename <s>  configuration file name
    -n, --new-database <s>'    name of database to create
    -t, --table-type <s>'      type of events table to create in new database:  must be "source"  or "destination"
    --dry-run                  if specified, display run parameters and end program without performing database initialization

### Sample configuration file

```javascript
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
```

#### connectTimeout
> An optional parameter that specifies the maximum number of milliseconds to wait to connect to a database before throwing an Error.  Default value is `15000` milliseconds.

#### connectionParams
  > Parameters used to connect to the `Postgresql` database server

| Field         | Required | Description
| ------------- |:--------:| :---------------------------------------
| host          | Yes      | database server name
| user          | No       | database user name.  connection attempt will fail if missing and required by database.
| password      | No       | database user password.  connection attempt will fail if missing and required by database.

# Operations
### Start up validations
- Configuration parameters are validated
- Database to be created must not exist and its name must be a valid `Postgresql` identifier
- If `slate-init-db` is started in `--dry-run` mode then it will validate and display configuration parameters without performing database initialization
- All start up information and any configuration errors are logged

### Error Recovery
- All configuration and operational errors will be logged
- If any errors are reported when running `slate-init-db` then the new database was not initialized properly
- Before restarting `slate-init-db`, all errors should be corrected, and the new database should be manually deleted if it was created

# Events table
The events table in the new database is initialized differently depending on whether it is a `source` or `destination` events table.

## Source Events Table Initialization

```sql
--source events table
CREATE TABLE events
(
  id bigserial NOT NULL,
  eventTimestamp timestamp with time zone NOT NULL,
  event jsonb NOT NULL,
  CONSTRAINT events_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

--NOTIFY function

CREATE FUNCTION events_notify_trigger() RETURNS trigger AS $$
DECLARE
BEGIN
  PERFORM pg_notify('eventsinsert', json_build_object('table', TG_TABLE_NAME, 'id', NEW.id )::text);
  RETURN new;
END;
$$ LANGUAGE plpgsql;

--NOTIFY trigger

CREATE TRIGGER events_table_trigger AFTER INSERT ON events
FOR EACH ROW EXECUTE PROCEDURE events_notify_trigger();
```

## Destination Events Table Initialization

```sql
--destination events table
CREATE TABLE events
(
  id bigint NOT NULL,
  eventTimestamp timestamp with time zone NOT NULL,
  event jsonb NOT NULL,
  CONSTRAINT events_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX events_event_name on events ((event #>> '{name}'));

CREATE INDEX events_eventtimestamp on events (eventTimestamp);

```
