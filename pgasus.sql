--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.1
-- Dumped by pg_dump version 9.4.0
-- Started on 2015-04-11 00:58:53

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 7 (class 2615 OID 16395)
-- Name: pgasus; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA pgasus;


SET search_path = pgasus, pg_catalog;

--
-- TOC entry 1864 (class 1247 OID 16598)
-- Name: http_method; Type: TYPE; Schema: pgasus; Owner: laurent
--

CREATE TYPE http_method AS ENUM (
    'get',
    'post',
    'put',
    'patch',
    'delete'
);


--
-- TOC entry 1867 (class 1247 OID 16610)
-- Name: object_type; Type: TYPE; Schema: pgasus; Owner: laurent
--

CREATE TYPE object_type AS ENUM (
    'relation',
    'procedure'
);


SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 175 (class 1259 OID 16620)
-- Name: routes; Type: TABLE; Schema: pgasus; Owner: laurent; Tablespace: 
--

CREATE TABLE routes (
    route_id integer NOT NULL,
    method http_method NOT NULL,
    url_path text NOT NULL,
    object_name text NOT NULL,
    object_type object_type NOT NULL,
    ttl integer DEFAULT 0 NOT NULL,
    is_public boolean NOT NULL,
    hidden_fields text[] DEFAULT ARRAY[]::text[] NOT NULL,
    readonly_fields text[] DEFAULT ARRAY[]::text[] NOT NULL,
    constants jsonb DEFAULT 'null'::jsonb NOT NULL,
    context_mapped_headers public.hstore DEFAULT ''::public.hstore NOT NULL,
    context_mapped_variables text[] DEFAULT ARRAY[]::text[] NOT NULL,
    max_limit integer NOT NULL DEFAULT 0
);


--
-- TOC entry 174 (class 1259 OID 16618)
-- Name: routes_rule_id_seq; Type: SEQUENCE; Schema: pgasus; Owner: laurent
--

CREATE SEQUENCE routes_rule_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4270 (class 0 OID 0)
-- Dependencies: 174
-- Name: routes_rule_id_seq; Type: SEQUENCE OWNED BY; Schema: pgasus; Owner: laurent
--

ALTER SEQUENCE routes_rule_id_seq OWNED BY routes.route_id;


--
-- TOC entry 4136 (class 2604 OID 16623)
-- Name: route_id; Type: DEFAULT; Schema: pgasus; Owner: laurent
--

ALTER TABLE ONLY routes ALTER COLUMN route_id SET DEFAULT nextval('routes_rule_id_seq'::regclass);


--
-- TOC entry 4143 (class 2606 OID 16630)
-- Name: rules_rule_id_pkey; Type: CONSTRAINT; Schema: pgasus; Owner: laurent; Tablespace: 
--

ALTER TABLE ONLY routes
    ADD CONSTRAINT rules_rule_id_pkey PRIMARY KEY (route_id);


-- Completed on 2015-04-11 00:58:57

--
-- PostgreSQL database dump complete
--

