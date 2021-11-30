--- Team #7:
---     Zhen Wu (zhw87)
---     Sushruti Bansod (sdb88)

SELECT *
FROM FOREST;

SELECT *
FROM STATE;

SELECT *
FROM ROAD;

SELECT *
FROM COVERAGE;

SELECT *
FROM INTERSECTION;

SELECT *
FROM SENSOR;

SELECT *
FROM REPORT;

SELECT *
FROM WORKER;

-------- Question 2
--(a)
-- note: road_no is primary key, which is NOT DEFERRED in our case. So it cannot be changed during execution.
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO ROAD VALUES (105,'Route Five',426);
INSERT INTO INTERSECTION VALUES (1, 105);
COMMIT;

--(b)
-- the initial constraint for maintainer is immediate deferrable. I think it's fine to keep it as immediate.
BEGIN;
UPDATE SENSOR
SET maintainer = 'xxxxxxxxx'
WHERE maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'Jason');
COMMIT;
BEGIN;
UPDATE SENSOR
SET maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'Jason')
WHERE maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'John');
COMMIT;
BEGIN;
UPDATE SENSOR
SET maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'John')
WHERE maintainer = 'xxxxxxxxx';
COMMIT;
--(c)
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO WORKER VALUES ('105588973', 'Natalia', 1, 'OH');
UPDATE SENSOR
SET maintainer = '105588973'
WHERE sensor_id = 2;
COMMIT;

-------- Question 3
--(a)
SELECT sensor_id, count(report_time)
FROM REPORT
GROUP BY sensor_id
HAVING count(report_time) IS NOT NULL
ORDER BY count(report_time) DESC
FETCH FIRST 3 ROWS ONLY;

--(b)
SELECT sensor_id, count(report_time)
FROM REPORT
GROUP BY sensor_id
HAVING count(report_time) IS NOT NULL
ORDER BY count(report_time) DESC
FETCH FIRST 2 ROWS ONLY OFFSET 3;

--(c)
SELECT t1.state, sum(t1.area)
FROM COVERAGE t1
GROUP BY t1.state
HAVING sum(t1.area) > (
    SELECT sum(t2.area)
    FROM COVERAGE t2 JOIN STATE t on t2.state = t.abbreviation
    WHERE t.name='Pennsylvania'
    )
ORDER BY sum(t1.area) DESC;

--(d)
SELECT DISTINCT r.name
FROM ROAD r JOIN INTERSECTION i1 on r.road_no = i1.road_no
WHERE r.road_no IN (
    SELECT i2.road_no
    FROM FOREST f JOIN INTERSECTION i2 on f.forest_no = i2.forest_no
    WHERE f.name = 'Stone Valley'
    );

--(e)
SELECT DISTINCT ssn, name, RANK() over (ORDER BY rank DESC) AS rank
FROM (
    SELECT w.ssn, w.name, w.rank
    FROM WORKER w JOIN SENSOR s ON w.ssn = s.maintainer
    WHERE s.energy <= 2
    ) AS R;

--(f)
SELECT DISTINCT f.name
FROM FOREST f JOIN COVERAGE c1 on f.forest_no = c1.forest_no
WHERE f.forest_no IN (
    SELECT c2.forest_no
    FROM COVERAGE c2 JOIN STATE t on c2.state = t.abbreviation
    WHERE t.name = 'Pennsylvania'
    ) AND acid_level > 0.6;

--(g)
SELECT f.name
FROM FOREST f NATURAL JOIN (
SELECT forest_no
FROM (SELECT forest_no
      FROM INTERSECTION i
      WHERE i.road_no IN (
        SELECT i1.road_no
        FROM FOREST f1 JOIN INTERSECTION i1 on f1.forest_no = i1.forest_no
        WHERE f1.name = 'Big Woods')) AS "extracted",
        (SELECT COUNT(*) as count
         FROM (SELECT i2.road_no
        FROM FOREST f2 JOIN INTERSECTION i2 on f2.forest_no = i2.forest_no
        WHERE f2.name = 'Big Woods') AS "bigwoods") AS "bigwoods_count"
WHERE forest_no <> (SELECT f3.forest_no
                    FROM FOREST f3
                    WHERE f3.name = 'Big Woods')
GROUP BY forest_no, bigwoods_count.count
HAVING COUNT(forest_no) = bigwoods_count.count) AS "final_forest_no";





-------- Question 4
--(a)
DROP VIEW IF EXISTS DUTIES;
CREATE OR REPLACE VIEW DUTIES AS
SELECT maintainer, COUNT(sensor_id) AS num_sensors
FROM SENSOR
WHERE maintainer IS NOT NULL
GROUP BY maintainer;

--(b)
DROP MATERIALIZED VIEW IF EXISTS DUTIES_MV;
CREATE MATERIALIZED VIEW DUTIES_MV AS
SELECT maintainer, COUNT(sensor_id) AS num_sensors_mv
FROM SENSOR
WHERE maintainer IS NOT NULL
GROUP BY maintainer;

--(c)
DROP VIEW IF EXISTS FOREST_SENSOR;
CREATE OR REPLACE VIEW FOREST_SENSOR AS
SELECT f.forest_no, f.name, s.sensor_id
FROM FOREST f, SENSOR s
WHERE s.x BETWEEN f.mbr_xmin AND f.mbr_xmax AND s.y BETWEEN f.mbr_ymin AND f.mbr_ymax;

--(d)
DROP VIEW IF EXISTS FOREST_ROAD;
CREATE OR REPLACE VIEW FOREST_ROAD AS
SELECT forest_no, COUNT(road_no) as count
FROM INTERSECTION
GROUP BY forest_no;


-------- Question 5
REFRESH MATERIALIZED VIEW DUTIES_MV;
--(a)

SELECT forest_no
FROM (SELECT forest_no, RANK() over (ORDER BY count DESC) as rank
       FROM FOREST_ROAD) AS rank_view
WHERE rank = 2;

--(b)
SELECT maintainer, name, employing_state, area
FROM COVERAGE C JOIN (SELECT maintainer, name, employing_state
                        FROM (SELECT maintainer, RANK() OVER (ORDER BY num_sensors DESC) as rank
                                FROM DUTIES) AS workers
                        JOIN WORKER w on workers.maintainer = w.ssn
                        WHERE workers.rank = 1) AS get_state on get_state.employing_state = c.state;

--(c)

SELECT F.name
FROM FOREST_SENSOR AS F
WHERE F.NAME NOT IN (SELECT F2.name
                        FROM REPORT R JOIN FOREST_SENSOR F2 on R.sensor_id = F2.sensor_id
                        WHERE R.report_time BETWEEN '2020-10-10 00:00:00' AND '2020-10-11 00:00:00');
--(d)

SELECT maintainer, name, employing_state, area
FROM COVERAGE C JOIN (SELECT maintainer, name, employing_state
                        FROM (SELECT maintainer, RANK() OVER (ORDER BY num_sensors_mv DESC) as rank
                                FROM DUTIES_MV) AS workers
                        JOIN WORKER w on workers.maintainer = w.ssn
                        WHERE workers.rank = 1) AS get_state on get_state.employing_state = c.state;

--(e)
---i really dont understand the question


