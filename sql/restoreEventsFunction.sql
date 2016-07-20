CREATE FUNCTION restore_events(fromHost text, fromDatabase text, "user" text, "password" text, OUT rowsRestored bigint, OUT nextInsertId bigint)
	AS $$
DECLARE
	getEventsStmt CONSTANT text = 'SELECT id, ts, entity_id, event FROM events ORDER BY id';
	getCountMaxIdStmt CONSTANT text = 'SELECT MAX(id) AS maxid, count(*) AS count FROM events';
	connectionInfo text;
	sourceEventsCount bigint;
	sourceNextIdValue bigint;
	fromEventsCount bigint;
	fromEventsMaxId bigint;
BEGIN
	connectionInfo := 'host=' || fromHost || ' dbname=' || fromDatabase || ' user=' || "user" || ' password=' || "password";
	RAISE NOTICE 'connection string %', connectionInfo;
	-- get row count from Source events table.  must be 0 after being created by slate-init-db.
	SELECT count(*) from events into sourceEventsCount;
	IF sourceEventsCount != 0 THEN
		RAISE EXCEPTION 'Source events table row count (%) is not 0', sourceEventsCount USING HINT = 'The Source events database must be initialized with slate-init-db';
	END IF;
	-- get next event id from id table.  must be 1 after being created by slate-init-db.
	SELECT id from id into sourceNextIdValue;
	IF sourceNextIdValue != 1 THEN
		RAISE EXCEPTION 'Source id table id value (%) is not 1', sourceNextIdValue USING HINT = 'The Source events database must be initialized with slate-init-db';
	END IF;
	-- get maximum event id and row count from events table in remote database being used to restore the Source events table.
	-- maximum event id must be 1 or greater and equal to the row count.
	SELECT fe.maxid, fe.count FROM dblink(connectionInfo, getCountMaxIdStmt) AS fe(maxid bigint, count bigint) INTO fromEventsMaxId, fromEventsCount;
	IF fromEventsMaxId IS NULL OR fromEventsMaxId < 1 THEN
		RAISE EXCEPTION 'The from events table maximum id value (%) is not 1 or greater', fromEventsMaxId USING HINT = 'The events table used to restore the Source events table must have a maximum id value of 1 or greater';
	END IF;
	IF fromEventsCount != fromEventsMaxId THEN
		RAISE EXCEPTION 'The from events table row count (%) is not equal to the from events table maximum id (%)', fromEventsCount, fromEventsMaxId USING HINT = 'The events table used to restore the Source events table is not valid';
	END IF;
	-- copy the events in order by id from the remote events table to the Source events table.
	INSERT INTO events (id, ts, entity_id, event)
		SELECT fe.id, fe.ts, fe.entity_id, fe.event
			FROM dblink(connectionInfo, getEventsStmt)
			AS fe(id bigint, ts timestamp with time zone, entity_id uuid, event jsonb);
	GET DIAGNOSTICS rowsRestored = ROW_COUNT;
	-- update the id value in the Source id table to the maximum id value + 1 from the remote events table used to restore the Source events table.
	UPDATE id SET id = fromEventsMaxId + 1 RETURNING id INTO nextInsertId;
	RAISE NOTICE 'rows restored %  next start id %', rowsRestored, nextInsertId;
	-- the count of events copied to the Source events table and the next events table id value to be used for the next event inserted into the Source events table are returned.
END;
$$ LANGUAGE plpgsql;
