----CS1555/2055 - DATABASE MANAGEMENT SYSTEMS (FALL 2021)
----DEPT. OF COMPUTER SCIENCE, UNIVERSITY OF PITTSBURGH
----ASSIGNMENT #4: 



-----------------------------------------------
-- Q2
-----------------------------------------------
--Q2.a:
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

INSERT INTO ROAD VALUES (105, 'Route Five', 426);
INSERT INTO INTERSECTION (SELECT forest_no, 105 FROM FOREST WHERE name = 'Allegheny National Forest');

COMMIT;



--Q2.b:
-- RECALL: worker's name is unique.
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

UPDATE SENSOR
SET maintainer = 'temp'
WHERE maintainer = (SELECT SSN FROM Worker WHERE Name = 'Jason');

UPDATE SENSOR
SET maintainer = (SELECT ssn FROM Worker WHERE Name = 'Jason')
WHERE maintainer = (SELECT ssn FROM Worker WHERE Name = 'John');

UPDATE SENSOR
SET maintainer = (SELECT ssn FROM Worker WHERE Name = 'John')
WHERE maintainer = 'temp';

COMMIT;


--Note: in PostgreSQL and Oracle (the multi-version concurrency control DBMSs), this solution works:
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

UPDATE SENSOR
SET maintainer=
  CASE
    WHEN (maintainer IN (SELECT ssn FROM WORKER WHERE name='Jason'))
    THEN
      (SELECT ssn FROM WORKER WHERE name='John')
    WHEN (maintainer IN (SELECT ssn FROM WORKER WHERE name='John'))
    THEN
      (SELECT ssn FROM WORKER WHERE name='Jason')
    ELSE maintainer
  END;

COMMIT;


-- Same but more efficient:
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

UPDATE SENSOR
SET maintainer=
  CASE
    WHEN (maintainer IN (SELECT ssn FROM WORKER WHERE name='Jason'))
    THEN
      (SELECT ssn FROM WORKER WHERE name='John')
    WHEN (maintainer IN (SELECT ssn FROM WORKER WHERE name='John'))
    THEN
      (SELECT ssn FROM WORKER WHERE name='Jason')
  END;
WHERE name IN ('Jason', 'John');

COMMIT;


--Q2.c:
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

INSERT INTO WORKER
VALUES ('105588973', 'Natalia', 1, 'OH');

UPDATE SENSOR
SET maintainer='105588973'
WHERE sensor_id = 2;

COMMIT;


-----------------------------------------------
-- Q3
-----------------------------------------------
--Q3.a:
SELECT sensor_id
FROM REPORT
group by sensor_id
order by count(*) desc
FETCH FIRST 3 ROWS ONLY;

--Q3.b:
SELECT sensor_Id
FROM REPORT
group by sensor_Id
order by count(*) desc
FETCH FIRST 2 ROW ONLY OFFSET 3; 

--Q3.c:
SELECT State
FROM COVERAGE
GROUP BY state
HAVING sum(area) > (SELECT sum(c.area)
                    FROM STATE S JOIN COVERAGE C on S.abbreviation = C.state
                    WHERE S.name = 'Pennsylvania');


--Q3.d:
SELECT name
FROM ROAD
WHERE road_no IN
    (SELECT road_no FROM INTERSECTION WHERE forest_no IN
    (SELECT forest_no FROM FOREST WHERE name='Stone Valley'));

-- another way:
SELECT RI.name
FROM (ROAD R NATURAL JOIN INTERSECTION I) RI
    JOIN FOREST F
        ON RI.forest_no = F.forest_no
WHERE F.forest_no IN (SELECT forest_no FROM FOREST WHERE name='Stone Valley');

--Q3.e:
SELECT name, RANK() OVER (
    ORDER BY num_sensors DESC 
    ) AS rank
FROM (
    SELECT ssn, name, count(*) as num_sensors
    FROM WORKER W JOIN SENSOR S ON W.ssn = S.maintainer
    WHERE S.energy <= 2
    GROUP BY name
    ) AS SENEOR_WORKER;

--Q3.f:
SELECT name
FROM FOREST
WHERE acid_level>0.6
AND forest_no IN
    (SELECT forest_no FROM COVERAGE WHERE state=
    (SELECT abbreviation FROM STATE WHERE name='Pennsylvania'));

-- another way:
SELECT F.name
FROM FOREST F JOIN COVERAGE C ON F.forest_no = C.forest_no
WHERE F.acid_level>0.6 AND C.state=(
    SELECT abbreviation
    FROM STATE
    WHERE STATE.name='Pennsylvania');

--Q3.g:
SELECT DISTINCT FOREST.name
FROM INTERSECTION IS1, FOREST
WHERE IS1.forest_no = FOREST.forest_no
AND IS1.forest_no != (SELECT forest_no FROM FOREST WHERE name='Big Woods')
AND NOT EXISTS
    (
    SELECT *
    FROM INTERSECTION IS2
    WHERE IS2.forest_no = (SELECT forest_no FROM FOREST WHERE name='Big Woods')
      AND IS2.road_no NOT IN
          (SELECT road_no FROM INTERSECTION WHERE forest_no = IS1.forest_no)
    );

-- another way:
SELECT DISTINCT F.name
FROM 
    (
      SELECT I.forest_no, I.road_no
      FROM INTERSECTION I
      WHERE I.forest_no != (SELECT forest_no FROM FOREST WHERE name = 'Big Woods')
    ) SX JOIN FOREST F ON SX.forest_no=F.forest_no
WHERE NOT EXISTS
(
    (
      SELECT p.road_no
      FROM (
              SELECT I.road_no
              FROM INTERSECTION I
              WHERE I.forest_no=(SELECT forest_no FROM FOREST WHERE name = 'Big Woods')
            ) P 
    )
EXCEPT
    (
      SELECT sp.road_no 
      FROM (
        SELECT I.forest_no, I.road_no
        FROM INTERSECTION I
        WHERE I.forest_no != (SELECT forest_no FROM FOREST F WHERE F.name = 'Big Woods')
            ) SP
      WHERE SP.forest_no = SX.forest_no 
    ) 
);

--Q3.h:
SELECT week_num, week_start || ' to ' || week_end AS start_to_end, AVG(temperature) AS avg_temperature
FROM (
      SELECT week_num,
             DATE(DATE_TRUNC('week', report_time)) AS week_start,
             DATE(DATE_TRUNC('week', DATE(report_time) + 7)) - 1 AS week_end,
             temperature
      FROM (
            SELECT sensor_id, temperature, report_time, EXTRACT(WEEK FROM report_time) - 35 AS week_num 
            FROM report
           ) REPORT_WEEK_NO
      WHERE EXTRACT(MONTH FROM report_time)>=9
        AND EXTRACT(MONTH FROM report_time)<=11
     ) FULL_INFO
GROUP BY week_num, week_start || ' to ' || week_end
ORDER BY week_num DESC;


-----------------------------------------------
-- Q4
-----------------------------------------------

--Q4.a:
CREATE OR REPLACE VIEW DUTIES 
AS
SELECT Maintainer, COUNT(*) as Total
FROM SENSOR
GROUP BY Maintainer;

--Q4.b:
DROP materialized VIEW if exists DUTIES_MV;
CREATE MATERIALIZED VIEW DUTIES_MV
AS
SELECT *
FROM DUTIES;

--Q4.c:
CREATE OR REPLACE VIEW FOREST_SENSOR AS
SELECT forest_no, Name, Sensor_Id
FROM FOREST
         JOIN SENSOR
              ON (X between MBR_XMin and MBR_XMax) and (Y between MBR_YMin and MBR_YMax);

-- OR
CREATE OR REPLACE VIEW FOREST_SENSOR (forest_no, forest_name ,sensor_id) AS
SELECT F.forest_no, name, sensor_id
FROM SENSOR S, FOREST F
WHERE (X BETWEEN MBR_XMin AND MBR_XMax)
  AND (Y BETWEEN MBR_YMin AND MBR_YMax);


--Q4.d:
CREATE OR REPLACE VIEW FOREST_ROAD (forest_name ,num_of_road) AS
SELECT name, total_num
FROM (
    SELECT forest_no, COUNT(*) AS total_num
    FROM INTERSECTION
    GROUP BY forest_no
    ORDER BY forest_no ASC
     ) ROAD_COUNT
INNER JOIN
    FOREST F
ON ROAD_COUNT.forest_no = F.forest_no;


-----------------------------------------------
-- Q5
-----------------------------------------------
--Q5.a:
SELECT forest_name
FROM FOREST_ROAD
WHERE num_of_road=(
    SELECT DISTINCT num_of_road
    FROM FOREST_ROAD
    ORDER BY num_of_road DESC
    FETCH FIRST 1 ROWS ONLY OFFSET 1
    );


--Q5.b:
SELECT W.name as worker_name, employing_state, state_total_area
FROM (DUTIES D JOIN WORKER W ON D.maintainer=W.ssn)
JOIN (
    SELECT state, SUM(area) AS state_total_area
    FROM COVERAGE
    GROUP BY state
    ) STATES_AREAS ON employing_state=state
WHERE D.total=(
    SELECT D.total
    FROM DUTIES
    ORDER BY total DESC
    FETCH FIRST 1 ROWS ONLY);



--Q5.c:
SELECT distinct Name
FROM FOREST_SENSOR
WHERE name not in (SELECT name
                 FROM FOREST_SENSOR
                          natural join REPORT
                 WHERE (Report_Time between '2020-10-10 00:00:00' and '2020-10-11 00:00:00'));

--Q5.d:
SELECT Maintainer, total
FROM DUTIES_MV
WHERE total = (
    SELECT total
    FROM DUTIES_MV
    order by total desc
    FETCH FIRST 1 ROWS ONLY
);



--Q5.e:
SELECT DISTINCT forest_name
FROM FOREST_SENSORS FS
WHERE FS.forest_name != 'Big Woods'
AND NOT EXISTS
  (
    SELECT *
    FROM FOREST_SENSORS FS1
    WHERE FS1.forest_name  = 'Big Woods'
      AND FS1.sensor_id NOT IN
          (SELECT sensor_id FROM FOREST_SENSORS WHERE forest_name = FS.forest_name)
  );

-- another way:
SELECT DISTINCT name
FROM forest_sensor fs JOIN (
    SELECT sensor_id
    FROM forest_sensor
    WHERE name='Big Woods') bigwood ON fs.sensor_id=bigwood.sensor_id
WHERE name != 'Big Woods';





