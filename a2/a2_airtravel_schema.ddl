-- NOTE: we have included some constraints in a comment in cases where
-- enforcing the constraint using SQL would be costly. For all parts of A2,
-- you may assume that these constraints hold, unless we explicitly specify
-- otherwise. Don't make any additional assumptions, regarding constraints
-- not enforced by the schema, or provided in the comments (even if the
-- constraint was specified in A1).

DROP SCHEMA IF EXISTS AirTravel CASCADE;
CREATE SCHEMA AirTravel;
SET SEARCH_PATH TO AirTravel;


-- Possible values for a seating class.
CREATE TYPE Class AS ENUM ('first', 'business', 'economy');

CREATE DOMAIN NonNegReal AS REAL CHECK (VALUE >= 0.0);


-- A city, its name <name>, and the name of the country <country>
-- where it is located.
CREATE TABLE City (
	cid INT PRIMARY KEY,
	name VARCHAR(200) NOT NULL,
	country VARCHAR(200) NOT NULL,
	UNIQUE (name, country)
);


-- An airline, its two-character IATA code <code> e.g. "AC", 
-- and its name e.g. "Air Canada".
CREATE TABLE Airline (
	code CHAR(2) PRIMARY KEY,
	name TEXT NOT NULL
);


-- A passenger, their first name <first_name> and last name
-- <last_name>.
CREATE TABLE Passenger (
	pid INT PRIMARY KEY,
	first_name VARCHAR(200) NOT NULL,
	last_name VARCHAR(200) NOT NULL,
	email VARCHAR(500) NOT NULL UNIQUE
);


-- A plane, its tail number <tail_number>, model <model> e.g. "Boeing 777",
-- and the code of the airline that owns the plane.
CREATE TABLE Plane (
	tail_number VARCHAR(6) PRIMARY KEY,
	model VARCHAR(20) NOT NULL,
	airline CHAR(2) NOT NULL REFERENCES Airline (code)
);


-- A seat on the plane identified by <plane>, located in row <row>,
-- column <letter>, and has the seating class <class>.
-- You may assume:
--	* Each plane includes at least 1 seat.
CREATE TABLE Seat (
	sid INT PRIMARY KEY,
	plane VARCHAR(6) NOT NULL REFERENCES Plane(tail_number),
	row INT NOT NULL,
	letter CHAR(1) NOT NULL,
	class CLASS NOT NULL,
	UNIQUE (plane, row, letter)
);


-- An airport, its identifying IATA code <code>, its name <name>,
-- and the id of the city where it is located <city>. 
CREATE TABLE Airport (
	code CHAR(3) PRIMARY KEY,
	name TEXT NOT NULL,
	city INT NOT NULL REFERENCES City (cid),
	UNIQUE (name, city)
);


-- A route operated by the airline <airline> between the <source>
-- and <destination> airports. <flight_num> is the flight designator
-- e.g., "WS579".
CREATE TABLE Route (
	flight_num VARCHAR(6) PRIMARY KEY,
	airline CHAR(2) NOT NULL REFERENCES Airline (code),
	source CHAR(3) NOT NULL REFERENCES Airport (code),
	destination CHAR(3) NOT NULL REFERENCES Airport (code),
	CHECK (source <> destination)
);


-- A scheduled flight that follows the route <route>, and uses plane <plane>.
-- The flight is scheduled to depart the source airport at <sched_dept>, and 
-- arrive at its destination at <sched_arrival>.
-- You may assume:
--	* A plane has no overlapping flights.
--  * The plane is owned by the same airline as the one operating this flight.
CREATE TABLE Flight (
	fid INT PRIMARY KEY,
	route VARCHAR(6) NOT NULL REFERENCES Route(flight_num),
	plane VARCHAR(6) NOT NULL REFERENCES Plane(tail_number),
	sched_dept TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	sched_arrival TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	UNIQUE (plane, sched_dept),
	CHECK (sched_arrival > sched_dept)  
);


-- The flight identified with <fid> departed its source airport
-- at <date_time>.
CREATE TABLE Departure (
	fid INT PRIMARY KEY REFERENCES Flight (fid),
	date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL
);


-- The flight identified with <fid> arrived at its destination
-- airport at <date_time>.
-- You may assume:
-- 	* The flight's actual arrival time occurred after its actual
--	  departure time.
CREATE TABLE Arrival (
	fid INT PRIMARY KEY REFERENCES Departure (fid),
	date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL
);


-- The flight identified with <fid> costs <price> for the seating class
-- <class>.
-- Note: The price recorded in FlightPrice changes over time.
-- You may assume:
-- 	* A seating class is included for the flight identified by <fid> in 
--    FlightPrice IFF it is available on the plane used for this flight.
CREATE TABLE FlightPrice (
	fid INT REFERENCES Flight (fid),
	class CLASS NOT NULL,
	price NonNegReal NOT NULL,
	PRIMARY KEY (fid, class)
);


-- A booking made by the passenger identified by <passenger> for 
-- seat <seat> on flight <flight>. The passenger paid <price>, which 
-- might be different from the price recorded in FlightPrice.
-- <date_time> is the timestamp at which the booking was made.
-- You may assume:
--	* The seat <seat> exists on the plane used for flight <flight>.
--	* The booking timestamp is before the scheduled departure time
--    of <flight>. 
CREATE TABLE Booking (
	bid INT PRIMARY KEY,
	passenger INT NOT NULL REFERENCES Passenger (pid),
	seat INT NOT NULL REFERENCES Seat (sid),
	flight INT NOT NULL REFERENCES Flight (fid),
	price NonNegReal NOT NULL,
	date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	UNIQUE (seat, flight)
);
