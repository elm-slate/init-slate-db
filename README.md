# slate-init-db
Creates and initializes a Postgresql database for use by the [`slate-replicator`](https://github.com/panosoft/slate-replicator)

The purpose of slate-init-db is to create and initialize a new `Postgresql` database to contain either a `source` or `destination` events table to be used by the slate-replicator.

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

# Database Initialization
A new database is initialized differently depending on whether it contains a `source` or `destination` events table.

The `id` table in a `source` database is used to assign consecutive integer ids starting at 1 to the rows inserted into the `events` table.

The `id` column and `ts` column values for a `source` events table are generated by the `insert_events` function.

## Database with a Source Events table

### Source Events Table Initialization

```sql
--create source events table

CREATE TABLE events
(
  id bigint NOT NULL,
  ts timestamp with time zone NOT NULL,
  entity_id uuid NOT NULL,
  event jsonb NOT NULL,
  CONSTRAINT events_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

--create source events table indexes

CREATE INDEX events_event_name on events ((event #>> '{name}'));

CREATE INDEX events_ts on events (ts);

CREATE INDEX events_entity_id on events (entity_id);

--create NOTIFY trigger function

CREATE FUNCTION events_notify_trigger() RETURNS trigger AS $$
DECLARE
BEGIN
  PERFORM pg_notify('eventsinsert', json_build_object('table', TG_TABLE_NAME, 'id', NEW.id )::text);
  RETURN new;
END;
$$ LANGUAGE plpgsql;

--create NOTIFY trigger

CREATE TRIGGER events_table_trigger AFTER INSERT ON events
FOR EACH ROW EXECUTE PROCEDURE events_notify_trigger();
```

### ID Table Initialization

```sql
CREATE TABLE id (
  id bigint NOT NULL,
  CONSTRAINT id_pkey PRIMARY KEY (id))
WITH (
  OIDS=FALSE
);
```
### insert_events function

```sql
CREATE FUNCTION insert_events(insertValuesList text[])
	RETURNS integer AS $$
DECLARE
	insertValues text;
	ts timestamp with time zone;
	startId bigint;
	nextStartId bigint;
	idxs bigint[];
	ids bigint[];
	lastIdx integer;
	idx bigint;
	maxIndex bigint;
	tsMatches text[];
	countRows bigint;
	countAllRows bigint DEFAULT 0;
	countRowsByInsert bigint[];
	countAllRowsInserted bigint DEFAULT 0;
BEGIN
	LOCK TABLE ID IN ACCESS EXCLUSIVE MODE;
	-- get timestamp to insert in each ts column
	ts := transaction_timestamp();
	-- insertValuesList is an array of string values.  Each string value in the array represents the column values to insert for one or more rows for one INSERT statement.
	-- Each string value is formatted to follow the VALUES keyword of the statement "INSERT INTO events (id, ts, entity_id, event) VALUES "
	-- (e.g. '($1[1], $2, '<entity_id literal>', '<json string for event>'),($1[2], $2, '<entity_id literal>', '<json string for event>')'.
	-- The id column for each row to insert is formatted as a substitution parameter, $1[x], where x is an index value.
	-- index values start at 1 for the first row to insert and must be a consecutive positive integer for each additional row.
	-- The ts (event timestamp) column is generated by this function and is represented  by the parameter $2.

	FOREACH insertValues IN ARRAY insertValuesList
	LOOP
		SELECT ARRAY(SELECT unnest(regexp_matches(insertValues, '\$1\[([0-9]+)\]', 'g'))) into idxs;
		SELECT ARRAY(SELECT unnest(regexp_matches(insertValues, '(\$2),', 'g'))) into tsMatches;
		countRows := 0;
		lastIdx := 0;
		-- find the row count, maximum index for a row, and check that the indices are consecutive positive integers.
		FOREACH idx IN ARRAY idxs
		LOOP
			countRows := countRows + 1;
			IF lastIdx = 0 THEN
				lastIdx := idx;
			ELSE
				IF idx = lastIdx + 1 THEN
					lastIdx := idx;
					maxindex := idx;
				ELSE
					RAISE EXCEPTION 'Parameter index is not consecutive at ------> %,  previous index ------> %', idx, lastIdx
						USING HINT = 'Parameter for id column of the form "$1[x]" where x is the 1-based index for the row to be inserted is not greater than the previous row''s index';
				END IF;
			END IF;
		END LOOP;
		IF countRows < 1 THEN
			RAISE EXCEPTION 'No inserted rows found with id substitution parameters' USING HINT = 'id column substitution parameter value for row to be inserted must be of the form "$1[x]" where x is the 1-indexed based index of the row';
		END IF;
		IF countRows != maxIndex THEN
			RAISE EXCEPTION 'Number of rows to be inserted (%) does not match the highest row index (%)', countRows, maxIndex USING HINT = 'The highest id column parameter substitution parameter index value does not match the number of rows to be inserted';
		END IF;
		IF countRows != coalesce(array_length(tsMatches, 1), 0) THEN
			RAISE EXCEPTION 'Number of rows to be inserted (%) does not match number of rows with a ts substitution parameter (%)', countRows, coalesce(array_length(tsMatches, 1), 0) USING HINT = 'ts column parameter substitution value for row to be inserted must be "$2"';
		END IF;
		countRowsByInsert := array_append(countRowsByInsert, countRows);
		countAllRows := countAllRows + countRows;
	END LOOP;
	-- update id table to point to the next starting id value to use
	UPDATE id SET id = id + countAllRows RETURNING id INTO nextStartId;
	-- start id for first insert statement
	startId := nextStartId - countAllRows;
	idx := 1;
	FOREACH insertValues IN ARRAY insertValuesList
	LOOP
		-- get ids to use for each inserted row's id column
		SELECT into ids ARRAY(SELECT generate_series(startId, startId + countRowsByInsert[idx] - 1));
		-- RAISE NOTICE 'ids ----> %', ids;
		EXECUTE 'INSERT INTO events (id, ts, entity_id, event) VALUES ' || insertValues USING ids, ts;
		GET DIAGNOSTICS countRows = ROW_COUNT;
		countAllRowsInserted := countAllRowsInserted + countRows;
		startId := startId + countRowsByInsert[idx];
		idx := idx + 1;
	END LOOP;
	RETURN countAllRowsInserted;
END;
$$ LANGUAGE plpgsql;
```

## Database with a Destination Events table

### Destination Events Table Initialization

```
--create destination events table

Same as create source events table script

--create destination events table indexes

Same as create source events indexes script
```
