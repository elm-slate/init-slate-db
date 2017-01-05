CREATE FUNCTION filter_sql_command()
	RETURNS TRIGGER as $$
BEGIN
	RAISE EXCEPTION 'cannot perform SQL command on events table:  %', TG_OP;
END;
$$ LANGUAGE plpgsql;
