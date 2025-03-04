-- Recommendation

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO AirTravel;
DROP TABLE IF EXISTS q3 CASCADE;

CREATE TABLE q3 (
    pid INT NOT NULL,
    name VARCHAR(410) NOT NULL,
	city VARCHAR(410) NOT NULL
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
-- If you do not define any views, you can delete the lines about views.
DROP VIEW IF EXISTS intermediate_step CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW intermediate_step AS ... ;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q3
