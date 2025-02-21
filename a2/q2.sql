-- Airport type

-- You must not change the next 3 lines or the table or type definition.
SET SEARCH_PATH TO AirTravel;
DROP TABLE IF EXISTS q2 CASCADE;
DROP TYPE IF EXISTS AirportType CASCADE;

CREATE TYPE AirportType AS ENUM (
	'heavy-inbound traffic', 
	'heavy-outbound traffic', 
	'hub',
	'N/A'
);

CREATE TABLE q2 (
    code CHAR(3) NOT NULL,
    name TEXT NOT NULL,
	category AirportType NOT NULL,
	inbound_traffic INT NOT NULL,
	outbound_traffic INT NOT NULL
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
-- If you do not define any views, you can delete the lines about views.
DROP VIEW IF EXISTS intermediate_step CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW intermediate_step AS ... ;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q2
