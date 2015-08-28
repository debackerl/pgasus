CREATE SCHEMA pgasus;

SET search_path = pgasus, pg_catalog;

CREATE TYPE http_method AS ENUM (
	'get',
	'post',
	'put',
	'patch',
	'delete'
);

CREATE TYPE object_type AS ENUM (
	'relation',
	'procedure'
);

CREATE TABLE routes (
	route_id serial NOT NULL,
	method admin.http_method NOT NULL,
	url_path text NOT NULL, -- URL route, e.g. /users/:user_id/files/*path, where user_id and path are variables
	object_name text NOT NULL, -- name of relation or procedure
	object_type admin.object_type NOT NULL,
	ttl integer NOT NULL DEFAULT 0, -- cache-control setting used for responses, in seconds
	is_public boolean NOT NULL, -- cache-control setting used for responses
	constants jsonb NOT NULL DEFAULT '{}'::jsonb, -- set of contstants to use in where clauses or as procedure arguments
	context_mapped_headers hstore NOT NULL DEFAULT ''::hstore, -- context variables copied from HTTP header using specified mapping, where keys are HTTP header keys, and values are variable names
	context_mapped_variables text[] NOT NULL DEFAULT ARRAY[]::text[], -- context variables copied from URL if available, or from cookie otherwise
	max_limit integer NOT NULL DEFAULT 0, -- maximum number of records that can be requested when using a select statement
	hidden_fields text[] NOT NULL DEFAULT ARRAY[]::text[], -- used for searchable fields which should not be displayed
	readonly_fields text[] NOT NULL DEFAULT ARRAY[]::text[], -- fields that could not be saved via insert or update statements
	CONSTRAINT rules_rule_id_pkey PRIMARY KEY (route_id)
);

COMMENT ON COLUMN routes.url_path IS 'URL route, e.g. /users/:user_id/files/*path, where user_id and path are variables';
COMMENT ON COLUMN routes.object_name IS 'name of relation or procedure';
COMMENT ON COLUMN routes.ttl IS 'cache-control setting used for responses, in seconds';
COMMENT ON COLUMN routes.is_public IS 'cache-control setting used for responses';
COMMENT ON COLUMN routes.constants IS 'set of contstants to use in where clauses or as procedure arguments';
COMMENT ON COLUMN routes.context_mapped_headers IS 'context variables copied from HTTP header using specified mapping, where keys are HTTP header keys, and values are variable names';
COMMENT ON COLUMN routes.context_mapped_variables IS 'context variables copied from URL if available, or from cookie otherwise';
COMMENT ON COLUMN routes.max_limit IS 'maximum number of records that can be requested when using a select statement';
COMMENT ON COLUMN routes.hidden_fields IS 'used for searchable fields which should not be displayed';
COMMENT ON COLUMN routes.readonly_fields IS 'fields that could not be saved via insert or update statements';

CREATE OR REPLACE FUNCTION routes_notify_trigger()
	RETURNS trigger AS
$BODY$
begin
	NOTIFY "goml.reload_routes";
	RETURN NULL;
end
$BODY$
	LANGUAGE plpgsql VOLATILE
	COST 100;

CREATE TRIGGER routes_updated_trigger
	AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
	ON routes
	FOR EACH STATEMENT
	EXECUTE PROCEDURE routes_notify_trigger();
