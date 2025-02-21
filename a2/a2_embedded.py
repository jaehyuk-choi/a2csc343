"""CSC343 Assignment 2

=== CSC343 Winter 2025 ===
Department of Computer Science,
University of Toronto

This code is provided solely for the personal and private use of
students taking the CSC343 course at the University of Toronto.
Copying for purposes other than this use is expressly prohibited.
All forms of distribution of this code, whether as given or with
any changes, are expressly prohibited.

Authors: Jacqueline Smith, Marina Tawfik and Akshay Arun Bapat

All of the files in this directory and all subdirectories are:
Copyright (c) 2025

=== Module Description ===

This file contains the AirTravel class and some simple testing functions.
"""
from typing import Optional
from datetime import date, datetime
import psycopg2 as pg
import psycopg2.extensions as pg_ext


class AirTravel:
    """A class that can work with data conforming to the schema used in A2.

    === Instance Attributes ===
    connection: connection to a PostgreSQL database of Markus-related
        information.

    Representation invariants:
    - The database to which <connection> holds a reference conforms to the
      schema used in A2.
    """
    connection: Optional[pg_ext.connection]

    def __init__(self) -> None:
        """Initialize this VetClinic instance, with no database connection
        yet.
        """
        self.connection = None

    def connect(self, dbname: str, username: str, password: str) -> bool:
        """Establish a connection to the database <dbname> using the
        username <username> and password <password>, and assign it to the
        instance attribute <connection>. In addition, set the search path
        to AirTravel.

        Return True if the connection was made successfully, False otherwise.
        I.e., do NOT throw an error if making the connection fails.

        >>> a2 = AirTravel()
        >>> # The following example will only work if you change the dbname
        >>> # and password to your own credentials.
        >>> a2.connect("csc343h-bapataks", "bapataks", "")
        True
        >>> # In this example, the connection cannot be made.
        >>> a2.connect("invalid", "nonsense", "incorrect")
        False
        """
        try:
            self.connection = pg.connect(
                dbname=dbname, user=username, password=password,
                options="-c search_path=AirTravel"
            )
            self.connection.set_client_encoding("UTF8")
            return True
        except pg.Error:
            return False

    def disconnect(self) -> bool:
        """Close this instance's connection to the database.

        Return True if closing the connection was successful, False otherwise.
        I.e., do NOT throw an error if closing the connection fails.

        >>> a2 = AirTravel()
        >>> # The following example will only work if you change the dbname
        >>> # and password to your own credentials.
        >>> a2.connect("csc343h-bapataks", "bapataks", "")
        True
        >>> a2.disconnect()
        True
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            return True
        except pg.Error:
            return False

    def make_booking(self, pid, seat, fid, timestamp):
        """Create a booking for the passenger identified by <pid> for the
        flight identified by <fid>. <seat> is a tuple of the row and letter of
        the seat selected by the passenger. <timestamp> is the timestamp of the
        booking.

        Set the booking's bid to be the maximum current existing bid + 1.
        Set the price paid by the passenger to be the current price recorded
        in FlightPrice for the seating class for <seat>.

        Return True if the booking is successful, and False otherwise i.e.,
        your method should NOT throw an exception.

        A booking is not successful if any of the following is True:
            * <pid> is invalid.
            * <fid> is invalid.
            * <seat> doesn't exist on the plane used by <fid> or is already
              booked.
            * <timestamp> is later than 1 hour before <fid>'s scheduled
              departure.
        """
        # TODO: Write the function definition according to the defined criteria
        if self.connection is None:
            return False
        try:
            cursor = self.connection.cursor()
            # 1. 확인: Passenger 존재 여부
            cursor.execute("SELECT 1 FROM Passenger WHERE passenger_id = %s", (pid,))
            if cursor.fetchone() is None:
                cursor.close()
                return False

            # 2. 확인: Flight 존재 및 scheduled_departure, plane_id (또는 tail_number) 가져오기
            cursor.execute(
                "SELECT scheduled_departure, tail_number FROM Flight WHERE flight_id = %s",
                (fid,)
            )
            flight_row = cursor.fetchone()
            if flight_row is None:
                cursor.close()
                return False
            scheduled_departure, plane_identifier = flight_row

            # 3. 예약 시각 검사: 예약 timestamp는 scheduled_departure 1시간 전보다 이전이어야 함.
            if timestamp > (scheduled_departure - timedelta(hours=1)):
                cursor.close()
                return False

            # 4. 좌석 확인: Seat 테이블에서 plane의 좌석 구성 확인 (가정: Plane 식별은 tail_number로)
            row_val, letter = seat
            cursor.execute(
                "SELECT seat_class FROM Seat WHERE tail_number = %s AND row = %s AND letter = %s",
                (plane_identifier, row_val, letter)
            )
            seat_info = cursor.fetchone()
            if seat_info is None:
                cursor.close()
                return False
            seat_class = seat_info[0]

            # 5. 해당 항공편에 대해 해당 좌석이 이미 예약되었는지 검사
            cursor.execute(
                "SELECT 1 FROM Booking WHERE flight_id = %s AND row = %s AND seat_letter = %s",
                (fid, row_val, letter)
            )
            if cursor.fetchone() is not None:
                cursor.close()
                return False

            # 6. FlightPrice 테이블에서 해당 항공편, 좌석 클래스의 가격 조회
            cursor.execute(
                "SELECT price FROM FlightPrice WHERE flight_id = %s AND seat_class = %s",
                (fid, seat_class)
            )
            price_info = cursor.fetchone()
            if price_info is None:
                cursor.close()
                return False
            price = price_info[0]

            # 7. 새 예약 id(bid)를 max(bid) + 1 로 설정
            cursor.execute("SELECT COALESCE(MAX(bid), 0) FROM Booking")
            max_bid = cursor.fetchone()[0]
            new_bid = max_bid + 1

            # 8. Booking 삽입
            cursor.execute(
                "INSERT INTO Booking (bid, passenger_id, flight_id, row, seat_letter, booking_time, price) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (new_bid, pid, fid, row_val, letter, timestamp, price)
            )
            self.connection.commit()
            cursor.close()
            return True

        except Exception:
            if self.connection:
                self.connection.rollback()
            return False

    def find_unreachable_from(self, airport: str):
        """Return a list of unique airport IATA code(s) that are not
        reachable from the airport identified by the IATA code <airport>.
        Don't include <airport> in the result.

        An airport A2 is considered to be reachable from another airport A1 if:
            * There is a route from A1 to A2.
            * There is a route from a third airport A3 to A2, where A3 is
              reachable from A1.

        Note that the above specification uses "route" and not "flight".

        If <airport> is not a valid IATA code, return None i.e., your method
        should NOT throw an exception.
        If <airport> is a valid IATA code, but is reachable from all airports,
        you should return an empty list.
        """
        # TODO: Write the function definition according to the defined criteria
        # NOTE: Check the 'WITH RECURSIVE' clause of Postgres.
        #       You do not have to necessarily use it but it can be helpful.
        if self.connection is None:
            return None
        try:
            cursor = self.connection.cursor()
            # 유효한 공항 코드인지 확인
            cursor.execute("SELECT 1 FROM Airport WHERE code = %s", (airport,))
            if cursor.fetchone() is None:
                cursor.close()
                return None

            # WITH RECURSIVE를 이용하여 <airport>로부터 도달 가능한 공항들을 찾음
            recursive_query = (
                "WITH RECURSIVE reachable(dest) AS ("
                "    SELECT destination FROM Route WHERE source = %s "
                "    UNION "
                "    SELECT r.destination FROM Route r JOIN reachable re ON r.source = re.dest"
                ") "
                "SELECT code FROM Airport WHERE code <> %s AND code NOT IN (SELECT dest FROM reachable)"
            )
            cursor.execute(recursive_query, (airport, airport))
            results = cursor.fetchall()
            cursor.close()
            # results는 [(code,), ...] 형태이므로 list comprehension 사용
            return [row[0] for row in results]
        except Exception:
            if self.connection:
                self.connection.rollback()
            return None

    def reassign_plane(self, tail_number: str, start: date, end: date):
        """Reassign planes to flights scheduled to depart between the
        <start> and <end> dates (inclusive), that are currently using the plane
        identified by <tail_number> as indicated below.

        Consider flights with scheduled departures between <start> and <end>
        days (inclusive) that use the plane <tail_number> i.e., you should
        account for flights departing on <start> and <end>, as well as any day
        in between regardless of the exact time of departure.

        Consider the target flights in order of departure date, giving higher
        priority to flights that are scheduled to depart earlier.

        For each flight, pick a replacement plane that satisfies the following
        criteria:
            * It is not the original plane <tail_number>.
            * The plane must be owned by the same airline as the original
              plane <tail_number>.
            * The plane can't have a flight that overlaps with the current
              flight, accounting for a 2 hours gap between flights i.e.,
              for a flight scheduled from 13:00 to 14:00, you should select a
              plane with no flights in the interval (11:00, 16:00).
            * The plane must have enough seating to satisfy the current number
              of bookings for the flight i.e., if the current flight has 5
              bookings for economy seats, and 10 booking for first class, then
              the replacement plane must have at least 5 economy seats and 10
              first class seats.
        From all candidate planes, give priority to planes that have fewer trips
        during <start> to <finish>, and break possible ties by ordering by
        tail_number in ascending order.

        Update the Flight relation accordingly for flights where a replacement
        plane was found, but make sure NOT to make changes for flights where
        a replacement is not available.

        Return a list of the flight ids where a replacement was NOT found.

        Your method should NOT throw an error. For cases such as an empty
        interval or no flights within interval, simply return an empty list.

        Note: You may assume that planes do not have overlapping flights.
        In your implementation, you make sure that this continues to hold
        (as specified above).

        Note: While it does not make sense in real life, we may call your
        method with dates in the past. You may assume however that the flights
        in the range from <start> to <end> have not departed.
        """
        # TODO: Write the function definition according to the defined criteria
        unscheduled = []
        if self.connection is None:
            return unscheduled
        try:
            cursor = self.connection.cursor()
            # 1. 원래 plane의 항공사 조회 (Plane 테이블에서 tail_number 기준)
            cursor.execute(
                "SELECT airline FROM Plane WHERE tail_number = %s", (tail_number,)
            )
            orig = cursor.fetchone()
            if orig is None:
                cursor.close()
                return unscheduled
            original_airline = orig[0]

            # 2. 대상 항공편 조회: 지정 tail_number를 사용하며, scheduled_departure의 날짜가 start~end 사이인 항공편
            cursor.execute(
                "SELECT flight_id, scheduled_departure, scheduled_arrival "
                "FROM Flight "
                "WHERE tail_number = %s AND DATE(scheduled_departure) BETWEEN %s AND %s "
                "ORDER BY scheduled_departure ASC",
                (tail_number, start, end)
            )
            flights = cursor.fetchall()

            for flight in flights:
                fid, sched_dep, sched_arr = flight
                # 3. 현재 항공편의 예약 건수를 좌석 클래스별로 집계
                cursor.execute(
                    "SELECT seat_class, COUNT(*) FROM Booking WHERE flight_id = %s GROUP BY seat_class",
                    (fid,)
                )
                bookings = cursor.fetchall()
                booking_counts = {cls: count for cls, count in bookings}

                # 4. 후보 plane 선택
                #    - 같은 항공사 소유, 다른 tail_number
                #    - 해당 항공편의 안전 구간: (sched_dep - 2시간, sched_arr + 2시간)
                interval_start = sched_dep - timedelta(hours=2)
                interval_end = sched_arr + timedelta(hours=2)
                # 후보 plane 중, 동일 시간대에 겹치는 항공편이 없는 plane 선택
                candidate_query = (
                    "SELECT p.tail_number FROM Plane p "
                    "WHERE p.airline = %s AND p.tail_number <> %s "
                    "AND NOT EXISTS ("
                    "    SELECT 1 FROM Flight f "
                    "    WHERE f.tail_number = p.tail_number "
                    "      AND f.scheduled_departure < %s "
                    "      AND f.scheduled_arrival > %s"
                    ") "
                    "ORDER BY p.tail_number ASC"
                )
                cursor.execute(candidate_query, (original_airline, tail_number, interval_end, interval_start))
                candidate_planes = [row[0] for row in cursor.fetchall()]

                replacement_found = False
                for candidate in candidate_planes:
                    # 5. 후보 plane의 좌석 용량 확인 (PlaneSeat 테이블 사용)
                    #    가정: PlaneSeat 테이블은 각 plane의 (tail_number, seat_class, capacity)를 제공
                    cursor.execute(
                        "SELECT seat_class, capacity FROM PlaneSeat WHERE tail_number = %s",
                        (candidate,)
                    )
                    capacities = {cls: cap for cls, cap in cursor.fetchall()}
                    meets_capacity = True
                    for seat_cls, needed in booking_counts.items():
                        if capacities.get(seat_cls, 0) < needed:
                            meets_capacity = False
                            break
                    if meets_capacity:
                        # 후보 plane 만족 → 해당 Flight의 tail_number를 업데이트
                        cursor.execute(
                            "UPDATE Flight SET tail_number = %s WHERE flight_id = %s",
                            (candidate, fid)
                        )
                        replacement_found = True
                        break

                if not replacement_found:
                    unscheduled.append(fid)
            self.connection.commit()
            cursor.close()
            return unscheduled
        except Exception:
            if self.connection:
                self.connection.rollback()
            return unscheduled

def setup(
        dbname: str, username: str, password: str, schema_path: str,
        data_path: str
) -> None:
    """Set up the testing environment for the database <dbname> using the
    username <username> and password <password> by importing the schema file
    at <schema_path> and the file containing the data at <data_path>.

    <schema_path> and <data_path> are the relative/absolute paths to the files
    containing the schema and the data respectively.
    """
    connection, cursor, schema_file, data_file = None, None, None, None
    try:
        connection = pg.connect(
            dbname=dbname, user=username, password=password,
            options="-c search_path=AirTravel"
        )
        connection.set_client_encoding("UTF8")
        cursor = connection.cursor()

        with open(schema_path, "r") as schema_file:
            cursor.execute(schema_file.read())

        with open(data_path, "r") as info_file:
            for line in info_file:
                line_elems = line.split()
                table_name = line_elems[1].lower()
                file_path = line_elems[3].strip("'")
                with open(file_path, "r", encoding='utf-8') as data_file:
                    cursor.copy_from(data_file, table_name, sep=",")
        connection.commit()
    except Exception as ex:
        connection.rollback()
        raise Exception(f"Couldn't set up environment for tests: \n{ex}")
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if connection and not connection.closed:
            connection.close()


def test_basics() -> None:
    """Test basic aspects of the A2 methods.
    """
    # TODO: Change the values of the following variables to connect to your
    #  own database:
    dbname = "csc343h-choija45"
    user = "choija45"
    password = "Jh07104455!!"

    # The following uses the relative paths to the schema file and the data file
    # we have provided. For your own tests, you will want to make your own data
    # files to use for testing.
    schema_file = "./a2_airtravel_schema.ddl"
    data_file = "./populate_data.sql"

    a2 = AirTravel()
    try:
        connected = a2.connect(dbname, user, password)

        # The following is an assert statement. It checks that the value for
        # connected is True. The message after the comma will be printed if
        # that is not the case (that is, if connected is False).
        # Use the same notation throughout your testing.
        assert connected, f"[Connect] Expected True | Got {connected}."

        # The following function call will set up the testing environment by
        # loading a fresh copy of the schema and the sample data we have
        # provided into your database. You can create more sample data files
        # and call the same function to load them into your database.
        setup(dbname, user, password, schema_file, data_file)

        # [TODO] Note: these results are for the provided generated data -
        #   Remember to change if you end up modifying the data.

        # ----------------------- Testing make_booking ----------------------- #

        # Note: These results assume that the instance has already been
        # populated with the provided data e.g., using the setup function.
        # You will also need to manually check the contents of your instance to
        # make sure it was updated correctly.

        # Invalid pid
        expected = False
        booked = a2.make_booking(39, (6, 'A'), 8, datetime(2025, 1, 15, 10, 0))
        assert booked == expected, \
            f"[make_booking] Expected {expected} - Got {booked}."

        # Valid booking
        expected = True
        booked = a2.make_booking(17, (6, 'A'), 8, datetime(2025, 1, 15, 10, 0))
        assert booked == expected, \
            f"[make_booking] Expected {expected} - Got {booked}."

        # Seat is already occupied
        expected = False
        booked = a2.make_booking(7, (6, 'A'), 8, datetime(2025, 3, 2, 10, 0))
        assert booked == expected, \
            f"[make_booking] Expected {expected} - Got {booked}"

        # ------------------ Testing find_unreachable_from ------------------- #

        # Note: These results assume that the instance has already been
        # populated with the provided data e.g., using the setup function.
        # Since we run all tests in the same function, the instance has been
        # changed by the above tests.

        # invalid IATA code.
        expected = None
        unreachable = a2.find_unreachable_from("ABC")
        assert unreachable is expected, \
            f"[find_unreachable_from] Expected {expected} - Got {unreachable}"

        # valid IATA code, not reachable from some airports.
        # Note that we use sorted, since we are not enforcing a specific
        # order on the return value.
        expected = sorted([
            "YTZ", "ATL", "LAX", "DFW", "DEN", "JFK",
            "SFO", "SEA", "LAS", "MIA", "AMS", "DXB", "SIN",
            "HKG", "ICN", "SYD", "PEK", "DEL", "GRU", "MEX", "JNB", "BKK",
            "KUL", "IST"
        ])
        unreachable = sorted(a2.find_unreachable_from("YYZ"))
        assert unreachable == expected, \
            f"[find_unreachable_from] Expected {expected} - Got {unreachable}"

        # ---------------------- Testing reassign_plane ---------------------- #

        # Note: These results assume that the instance has already been
        # populated with the provided data e.g., using the setup function.
        # Since we run all tests in the same function, the instance has been
        # changed by the above tests.
        # Note that you will still need to inspect the database to ensure that
        # the changes are reflected there.

        # Two flights to re-schedule.
        expected = []
        unscheduled = sorted(a2.reassign_plane(
            'D84KL', date(2024, 1, 1), date(2024, 8, 12)))
        assert unscheduled == expected, \
            f"[reassign_plane] Expected {expected} - Got {unscheduled}"
    finally:
        a2.disconnect()


if __name__ == "__main__":
    # Un comment-out the next two lines if you would like to run the doctest
    # examples (see ">>>" in the methods connect and disconnect)
    # import doctest
    # doctest.testmod()

    # TODO: Put your testing code here, or call testing functions such as
    #   this one which has been provided as a sample for you to write more tests:
    test_basics()
