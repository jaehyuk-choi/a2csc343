-- u1.sql
-- Big sale! Update the FlightPrice table for Air Canada flights to US/Canada
-- between 2025-06-01 and 2025-08-31.

UPDATE FlightPrice AS fp
SET price = CASE
    WHEN fp.seat_class = 'F' THEN fp.price * 0.8
    WHEN fp.seat_class = 'B' THEN fp.price * 0.7
    WHEN fp.seat_class = 'E' THEN fp.price * 0.6
    ELSE fp.price  -- 혹시 다른 클래스가 있다면 변경 없음
END
FROM Flight f
JOIN Airport a ON f.scheduled_arrival_airport = a.code
WHERE fp.flight_id = f.flight_id
  AND f.airline = 'AC'
  AND a.country IN ('United States', 'Canada')
  AND f.scheduled_departure_time::date BETWEEN '2025-06-01' AND '2025-08-31';
