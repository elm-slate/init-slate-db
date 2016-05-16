CREATE FUNCTION insert_events(insertValues text)
	RETURNS integer AS $$
DECLARE
	startId bigint;
	nextStartId bigint;
	idxs bigint[];
	ids bigint[];
	lastIdx integer DEFAULT 0;
	idx bigint;
	rowsInserted bigint;
	countRows bigint;
	maxIndex bigint;
BEGIN
	LOCK TABLE ID IN ACCESS EXCLUSIVE MODE;
	-- RAISE NOTICE 'insertValues----> %', insertValues;
	-- get index values for id column for each row to insert.  id column substitution parameter format is $1[x] where x is the index for each row to insert.
	-- index values start at 1 for the first row to insert and must be a consecutive positive integer for each additional row.
	-- insertValues is a string representing the column values to insert for one or more rows which follows the VALUES keyword of the INSERT statement
	-- (e.g. '($1[1], '<eventtimestamp literal>', '<json string for event>'),($1[2], ''<eventtimestamp literal>', '<json string for event>')'
	SELECT ARRAY(SELECT unnest(regexp_matches(insertValues, '\$1\[([0-9]+)\]', 'g'))) into idxs;
	-- RAISE NOTICE 'row idxs ----> %', idxs;
	countRows := 0;
	-- find the row count, maximum index for a row, and check that the indices are consecutive positive integers.
	FOREACH idx IN ARRAY idxs
	LOOP
		-- RAISE NOTICE 'lastIdx ----> %  idx ----> %', lastIdx, idx;
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
	-- RAISE NOTICE 'countRows ----> %  maxIndex ----> %', countRows, maxIndex;
	IF countRows < 1 THEN
		RAISE EXCEPTION 'No inserted rows found with parameters' USING HINT = 'id column for row to be inserted must be a parameter of the form "$1[x]" where x is the 1-indexed based index of the row';
	END IF;
	IF countRows != maxIndex THEN
		RAISE EXCEPTION 'Number of rows to be inserted % does not match the highest row index %', countRows, maxIndex USING HINT = 'The highest id column parameter index does not match the number of rows to be inserted';
	END IF;
	-- update id table to point to the next starting id value to use if this function returns successfully
	UPDATE id SET id = id + countRows RETURNING id INTO nextStartId;
	startId := nextStartId - countRows;
	-- RAISE NOTICE 'startId ----> %  nextStartId ----> %', startId, nextStartId;
	-- get ids to use for each inserted row's id column
	SELECT into ids ARRAY(SELECT generate_series(startId, nextStartId - 1));
	-- RAISE NOTICE 'ids ----> %', ids;
	EXECUTE 'INSERT INTO events (id, ts, entity_id, event) VALUES ' || insertValues USING ids;
	GET DIAGNOSTICS rowsInserted = ROW_COUNT;
	RETURN rowsInserted;
END;
$$ LANGUAGE plpgsql;
