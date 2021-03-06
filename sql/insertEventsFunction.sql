CREATE FUNCTION insert_events(insertValues text)
	RETURNS integer AS $$
DECLARE
	ts timestamp with time zone;
	startId bigint;
	nextStartId bigint;
	idxs bigint[];
	ids bigint[];
	lastIdx integer;
	idx bigint;
	rowsInserted bigint;
	countRows bigint;
	maxIndex bigint;
	tsMatches text[];
BEGIN
	LOCK TABLE ID IN ACCESS EXCLUSIVE MODE;
	-- get timestamp to insert in each ts column
	ts := transaction_timestamp();
	-- insertValues is a string value that represents the column values to insert for one or more rows for one INSERT statement.
	-- insertValues is formatted to follow the VALUES keyword of the statement "INSERT INTO events (id, ts, event) VALUES "
	-- (e.g. '($1[1], $2, '<json string for event>'), ($1[2], $2, '<json string for event>')'.
	-- The id column for each row to insert is formatted as a substitution parameter, $1[x], where x is an index value.
	-- index values start at 1 for the first row to insert and must be a consecutive positive integer for each additional row.
	-- The ts (event timestamp) column is generated by this function and is represented  by the parameter $2.

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
				maxIndex := idx;
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

	-- update id table to point to the next starting id value to use
	UPDATE id SET id = id + countRows RETURNING id INTO nextStartId;
	-- start id for first insert statement
	startId := nextStartId - countRows;
	-- get ids to use for each inserted row's id column
	SELECT into ids ARRAY(SELECT generate_series(startId, nextStartId - 1));
	-- RAISE NOTICE 'ids ----> %', ids;
	EXECUTE 'INSERT INTO events (id, ts, event) VALUES ' || insertValues USING ids, ts;
	GET DIAGNOSTICS rowsInserted = ROW_COUNT;
	RETURN rowsInserted;
END;
$$ LANGUAGE plpgsql;
