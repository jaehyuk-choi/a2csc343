-- u2.sql
-- Decommissioned planes: Delete all info about planes that have not been used
-- since 2021-12-31 (i.e., last actual arrival was on or before 2021-12-31).

-- 1) 먼저, 삭제 대상 기체(tail_number) 목록을 CTE로 추출
WITH to_decommission AS (
    SELECT p.tail_number
    FROM Plane p
    -- "가장 최근 actual_arrival_time"이 2021-12-31 이하인 기체만 찾는다.
    --  -> "해당 기체로 운항한 Flight 중 actual_arrival_time이 가장 큰 값"이 2021-12-31 이하
    LEFT JOIN (
        SELECT tail_number, MAX(actual_arrival_time) AS last_arrival
        FROM Flight
        WHERE actual_arrival_time IS NOT NULL
        GROUP BY tail_number
    ) AS fmax
    ON p.tail_number = fmax.tail_number
    WHERE (fmax.last_arrival <= '2021-12-31' OR fmax.last_arrival IS NULL)
)
-- 2) 자식 테이블(Seat, PlaneSeat 등)부터 삭제
--    (만약 FK가 ON DELETE CASCADE 라면 이 단계는 생략 가능)
DELETE FROM Seat
WHERE plane IN (SELECT tail_number FROM to_decommission);

DELETE FROM Flight
WHERE plane IN (SELECT tail_number FROM to_decommission);

-- (다른 관련 테이블이 있다면 추가로 DELETE)

-- 3) Plane 테이블에서 삭제
DELETE FROM Plane
WHERE tail_number IN (SELECT tail_number FROM to_decommission);
