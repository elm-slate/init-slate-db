CREATE FUNCTION notify_event_insert()
	RETURNS TRIGGER AS $$
DECLARE
BEGIN
	PERFORM pg_notify('eventsinsert', json_build_object('table', TG_TABLE_NAME, 'id', NEW.id, 'event', NEW.event )::text);
	RETURN new;
END;
$$ LANGUAGE plpgsql;
