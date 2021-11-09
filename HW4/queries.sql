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

-----------------------------------
---------QUESTION 2----------------
-----------------------------------
---A
INSERT INTO ROAD VALUES(105, 'Route Five', 426);
INSERT INTO INTERSECTION VALUES(1, 105);

---B
---I DO NOT UNDERSTAND
UPDATE SENSOR SET maintainer = (SELECT W.ssn
                                FROM WORKER AS W
                                WHERE W.name = 'Jason') WHERE maintainer = (SELECT W.ssn
                                                                            FROM WORKER AS W
                                                                            WHERE W.name = 'John');
UPDATE SENSOR SET maintainer = (SELECT W.ssn
                                FROM WORKER AS W
                                WHERE W.name = 'John') WHERE maintainer = (SELECT W.ssn
                                                                            FROM WORKER AS W
                                                                            WHERE W.name = 'Jason');

---why did you do what you did?? cant we just overwrite them
---C
INSERT INTO WORKER (ssn, name,  rank, employing_state) VALUES ('105588973','Natalia',1, 'OH');
UPDATE SENSOR SET maintainer = '105588973' WHERE sensor_id = 2;



-----------------------------------
---------QUESTION 3----------------
-----------------------------------

----A
SELECT sensor_id, Count(*) FROM REPORT
GROUP BY sensor_id
HAVING COUNT(*) IS NOT NULL
ORDER BY Count(*) DESC
FETCH FIRST 3 ROWS ONLY;

----B
SELECT sensor_id, Count(*) FROM REPORT
GROUP BY sensor_id
HAVING COUNT(*) IS NOT NULL
ORDER BY Count(*) DESC
FETCH FIRST 2 ROWS ONLY OFFSET 3;

----C
SELECT C.state, sum(C.area)
FROM COVERAGE AS C
GROUP BY C.state
HAVING SUM(C.area) > (SELECT sum(C2.area)
                        FROM COVERAGE AS C2 JOIN STATE S ON C2.state = S.abbreviation
                        WHERE S.name = 'Pennsylvania')
-- ORDER BY sum(C.area) DESC;   why

----D
SELECT DISTINCT R.name
FROM ROAD AS R JOIN INTERSECTION I ON I.road_no = R.road_no
WHERE R.road_no IN (SELECT I2.road_no
                    FROM FOREST F JOIN INTERSECTION I2 ON F.forest_no = I2.forest_no
                    WHERE F.name = 'Stone Valley');

----E
SELECT DISTINCT ssn, name, RANK() over (ORDER BY rank DESC) as rank
FROM(
    SELECT W.ssn, W.name, W.rank
    FROM WORKER W JOIN SENSOR S ON W.ssn = S.maintainer
    WHERE S.energy <= 2) as R;

----F
SELECT DISTINCT F.NAME
FROM FOREST AS F JOIN COVERAGE C ON F.forest_no = C.forest_no
WHERE F.forest_no IN (SELECT C2.forest_no
                    FROM COVERAGE AS C2 JOIN STATE AS S ON C2.state = S.abbreviation
                    WHERE S.name = 'Pennsylvania') AND F.acid_level > 0.6;

----g
SELECT DISTINCT F.name
FROM FOREST F JOIN (INTERSECTION I RIGHT OUTER JOIN(
                                    SELECT I2.road_no
                                    FROM FOREST F2 JOIN INTERSECTION I2 on F2.forest_no = i2.forest_no
                                    WHERE F2.name = 'Big Woods') AS T on I.road_no=T.road_no) ON F.forest_no = I.forest_no
WHERE T.road_no IS NOT NULL;

----H


-----------------------------------
---------QUESTION 4----------------
-----------------------------------

----a
DROP VIEW IF EXISTS DUTIES;
CREATE OR REPLACE VIEW DUTIES AS
    SELECT S.maintainer, count(S.sensor_id) AS num_sensors
    FROM SENSOR S
    WHERE S.maintainer IS NOT NULL
    GROUP BY S.maintainer;

---B
DROP MATERIALIZED VIEW IF EXISTS DUTIES_MV;
CREATE MATERIALIZED VIEW DUTIES_MV AS
    SELECT S.maintainer, count(S.sensor_id) AS num_sensors
    FROM SENSOR S
    WHERE S.maintainer IS NOT NULL
    GROUP BY S.maintainer;

----C
DROP VIEW IF EXISTS FOREST_SENSOR;
CREATE OR REPLACE VIEW FOREST_SENSOR AS
    SELECT F.forest_no, s.sensor_id
    FROM FOREST F
         JOIN SENSOR S ON S.x BETWEEN F.mbr_xmin AND F.mbr_xmax AND S.y BETWEEN F.mbr_ymin AND F.mbr_ymax;

----D
DROP VIEW IF EXISTS FOREST_ROAD;
CREATE OR REPLACE VIEW FOREST_ROAD AS
    SELECT forest_no, COUNT(road_no)
    FROM intersection
    GROUP BY forest_no;

