-- Airline information

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO AirTravel;
DROP TABLE IF EXISTS q4 CASCADE;

CREATE TABLE q4 (
    airline CHAR(2) NOT NULL,
    num_planes INT NOT NULL,
    num_routes INT NOT NULL,
    avg_flights REAL DEFAULT NULL,
    avg_delay INTERVAL DEFAULT NULL,
    avg_first_price REAL DEFAULT NULL,
    avg_business_price REAL DEFAULT NULL,
    avg_economy_price REAL DEFAULT NULL   
);

--- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
-- If you do not define any views, you can delete the lines about views.
DROP VIEW IF EXISTS intermediate_step CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW intermediate_step AS ... ;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q4
