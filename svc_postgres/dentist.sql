--
-- Dentist's Clinic Database MasterData Init
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;



SET default_tablespace = '';

SET default_with_oids = false;


---
--- ensure if tables doesnt exist
---

DROP TABLE IF EXISTS dentists;

--
-- create tables 
--

CREATE TABLE dentists (
    dentist_id smallint PRIMARY KEY,
    first_name varchar(15),
    last_name varchar(15),
    licence varchar(20),
    workdays varchar(20),
    shift varchar(20),
    created_at date,
    updated_at date
);

--
-- data for dentists

INSERT INTO dentists VALUES (1, 'Aslijan', 'Rahmawati', 'STR-2007-30350', 'Senin - Kamis', 'Pagi', '2022-03-23');
INSERT INTO dentists VALUES (2, 'Silvia', 'Riyanti', 'STR-2010-61056', 'Senin - Kamis', 'Sore', '2023-05-06');
INSERT INTO dentists VALUES (3, 'Wardi', 'Damanik', 'STR-2000-93455', 'Jumat - Minggu', 'Pagi', '2024-03-11');
INSERT INTO dentists VALUES (4, 'Cagak', 'Suwarno', 'STR-2002-55577', 'Jumat - Minggu', 'Sore', '2024-11-09');

--
-- PostgreSQL database dump complete
--

