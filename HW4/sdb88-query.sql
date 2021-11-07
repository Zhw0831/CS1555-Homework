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
INSERT INTO ROAD VALUES (105,'Route Five',426);
INSERT INTO INTERSECTION VALUES (1, 105);

--(b)
begin;
UPDATE SENSOR
SET maintainer = 'xxxxxxxxx'
WHERE maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'Jason');

UPDATE SENSOR
SET maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'Jason')
WHERE maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'John');

UPDATE SENSOR
SET maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'John')
WHERE maintainer = 'xxxxxxxxx';
end;

--(c)
-- begin end?
INSERT INTO WORKER VALUES ('105588973', 'Natalia', 1, 'OH');
UPDATE SENSOR
SET maintainer = (SELECT w.ssn
                  FROM WORKER w
                  WHERE w.name = 'Natalia')
WHERE sensor_id = 2;

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
--SELECT *
--FROM INTERSECTION i RIGHT OUTER JOIN(
--          SELECT i2.road_no
--          FROM FOREST f2 JOIN INTERSECTION i2 on f2.forest_no = i2.forest_no
--          WHERE f2.name = 'Big Woods') AS i3 on i.road_no=i3.road_no;
--WHERE i3.road_no IS NOT NULL;


--(h)
--SELECT
--FROM REPORT
--WHERE



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
SELECT COUNT(sensor_id) AS num_sensors_mv
FROM SENSOR
WHERE maintainer IS NOT NULL
GROUP BY maintainer;

--(c)
CREATE OR REPLACE VIEW FOREST_SENSOR AS
SELECT f.forest_no, f.name, s.sensor_id
FROM FOREST f, SENSOR s
WHERE s.x BETWEEN f.mbr_xmin AND f.mbr_xmax AND s.y BETWEEN f.mbr_ymin AND f.mbr_ymax;

--(d)
CREATE OR REPLACE VIEW FOREST_ROAD AS
SELECT forest_no, COUNT(road_no)
FROM INTERSECTION
GROUP BY forest_no;

