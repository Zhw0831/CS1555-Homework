--Sushruti Bansod (sdb88), Zhen Wu(zhw87)
---------------------------------------------------------------------------
-------QUESTION 1
---------------------------------------------------------------------------
DROP TABLE IF EXISTS FOREST CASCADE;
CREATE TABLE FOREST(
    forest_no VARCHAR(10),
    name VARCHAR(30),
    area REAL,
    acid_level REAL,
    mbr_xmin REAL,
    mbr_xmax REAL,
    mbr_ymin REAL,
    mbr_ymax REAL,

    CONSTRAINT FOREST_PK
        PRIMARY KEY(forest_no)
);
---
DROP TABLE IF EXISTS STATE CASCADE;
CREATE TABLE STATE(
    name VARCHAR(30),
    abbreviation VARCHAR(2),
    area REAL,
    population INTEGER,

    CONSTRAINT STATE_PK
        PRIMARY KEY(abbreviation)
);
---
DROP TABLE IF EXISTS COVERAGE CASCADE;
CREATE TABLE COVERAGE (
	forest_no VARCHAR(10),
	state VARCHAR(2),
	percentage REAL,
	area REAL,

	CONSTRAINT COVERAGE_PK
	    PRIMARY KEY(forest_no,state),
	CONSTRAINT COVERAGE_FK_forest_no
	    FOREIGN KEY(forest_no) REFERENCES FOREST(forest_no),
	CONSTRAINT COVERAGE_FK_state
	    FOREIGN KEY(state) REFERENCES STATE(abbreviation)
);
---
DROP TABLE IF EXISTS ROAD CASCADE;
CREATE TABLE ROAD(
    road_no VARCHAR(10),
    name VARCHAR(30),
    length REAL,

    CONSTRAINT ROAD_PK
        PRIMARY KEY(road_no)
);
---
DROP TABLE IF EXISTS INTERSECTION CASCADE;
CREATE TABLE INTERSECTION(
    forest_no VARCHAR(10),
    ROAD_no VARCHAR(10),

    CONSTRAINT INTERSECTION_PK
        PRIMARY KEY(forest_no,road_no),
    CONSTRAINT INTERSECTION_FK_forest_no
        FOREIGN KEY(forest_no) REFERENCES FOREST(forest_no),
    CONSTRAINT INTERSECTION_FK_road_no
        FOREIGN KEY(road_no) REFERENCES ROAD(road_no)
);

---
DROP TABLE IF EXISTS WORKER CASCADE;
CREATE TABLE WORKER(

    ssn VARCHAR(9),
    name VARCHAR(30),
    rank INTEGER,

    CONSTRAINT WORKER_PK
        PRIMARY KEY(ssn)
);
---
DROP TABLE IF EXISTS SENSOR CASCADE;
CREATE TABLE SENSOR(
    sensor_id INTEGER,
    x REAL,
    y REAL,
    last_charged TIMESTAMP,
    maintainer VARCHAR(9),
    last_read TIMESTAMP,

    CONSTRAINT SENSOR_PK
        PRIMARY KEY(sensor_id),
    CONSTRAINT SENSOR_FK
        FOREIGN KEY(maintainer) REFERENCES WORKER(ssn)
);
---
DROP TABLE IF EXISTS REPORT CASCADE;
CREATE TABLE REPORT(
    sensor_id INTEGER,
    report_time TIMESTAMP,
    temperature REAL,

    CONSTRAINT REPORT_PK
        PRIMARY KEY(sensor_id,report_time),
    CONSTRAINT REPORT_FK
        FOREIGN KEY(sensor_id) REFERENCES SENSOR(sensor_id)
);


---------------------------------------------------------------------------
----QUESTION 2
---------------------------------------------------------------------------
--a
--At first we had decided to add unique constraints to all the alternate keys.That is
--all the keys that are not the primary keys. But we faced a problem when we ran the code.
--In that case, duplicates were not allowed due to the unique constraints, to fix this issue,
--we commented out attributes which have possibilities of duplicate values. Mainly, we made
--the decision by studying the given insert statements.

ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_name;
ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_name
    UNIQUE(name);

ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_area;
ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_area
    UNIQUE(area);

ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_acid_level;
ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_acid_level
    UNIQUE(acid_level);

-- ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_mbr_xmin;
-- ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_mbr_xmin
--     UNIQUE(mbr_xmin);
--
-- ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_mbr_xmax;
-- ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_mbr_xmax
--     UNIQUE(mbr_xmax);
--
-- ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_mbr_ymin;
-- ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_mbr_ymin
--     UNIQUE(mbr_ymin);
--
-- ALTER TABLE FOREST DROP CONSTRAINT IF EXISTS FOREST_UN_mbr_ymax;
-- ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_mbr_ymax
--     UNIQUE(mbr_ymax);

ALTER TABLE STATE DROP CONSTRAINT IF EXISTS STATE_UN_name;
ALTER TABLE STATE ADD CONSTRAINT STATE_UN_name
    UNIQUE(name);

ALTER TABLE STATE DROP CONSTRAINT IF EXISTS STATE_UN_area;
ALTER TABLE STATE ADD CONSTRAINT STATE_UN_area
    UNIQUE(area);

ALTER TABLE STATE DROP CONSTRAINT IF EXISTS STATE_UN_population;
ALTER TABLE STATE ADD CONSTRAINT STATE_UN_population
    UNIQUE(population);

ALTER TABLE ROAD DROP CONSTRAINT IF EXISTS ROAD_UN_name;
ALTER TABLE ROAD ADD CONSTRAINT ROAD_UN_name
    UNIQUE(name);

ALTER TABLE ROAD DROP CONSTRAINT IF EXISTS ROAD_UN_length;
ALTER TABLE ROAD ADD CONSTRAINT ROAD_UN_length
    UNIQUE(length);

-- ALTER TABLE COVERAGE DROP CONSTRAINT  IF EXISTS COVERAGE_UN_percentage;
-- ALTER TABLE COVERAGE ADD CONSTRAINT COVERAGE_UN_percentage
--     UNIQUE(percentage);

ALTER TABLE COVERAGE DROP CONSTRAINT IF EXISTS COVERAGE_UN_area;
ALTER TABLE COVERAGE ADD CONSTRAINT COVERAGE_UN_area
    UNIQUE(area);

-- ALTER TABLE SENSOR DROP CONSTRAINT IF EXISTS SENSOR_UN_x;
-- ALTER TABLE SENSOR ADD CONSTRAINT SENSOR_UN_x
--     UNIQUE(x);
--
-- ALTER TABLE SENSOR DROP CONSTRAINT IF EXISTS SENSOR_UN_y;
-- ALTER TABLE SENSOR ADD CONSTRAINT SENSOR_UN_y
--     UNIQUE(y);

-- ALTER TABLE SENSOR DROP CONSTRAINT IF EXISTS SENSOR_UN_last_charged;
-- ALTER TABLE SENSOR ADD CONSTRAINT SENSOR_UN_last_charged
--     UNIQUE(last_charged);

-- ALTER TABLE SENSOR DROP CONSTRAINT IF EXISTS SENSOR_UN_maintainer;
-- ALTER TABLE SENSOR ADD CONSTRAINT SENSOR_UN_maintainer
--     UNIQUE(maintainer);

-- ALTER TABLE SENSOR DROP CONSTRAINT IF EXISTS SENSOR_UN_last_read;
-- ALTER TABLE SENSOR ADD CONSTRAINT SENSOR_UN_last_read
--     UNIQUE(last_read);

-- ALTER TABLE REPORT DROP CONSTRAINT IF EXISTS REPORT_UN_temperature;
-- ALTER TABLE REPORT ADD CONSTRAINT REPORT_UN_temperature
--     UNIQUE(temperature);

ALTER TABLE WORKER DROP CONSTRAINT IF EXISTS WORKER_UN_name;
ALTER TABLE WORKER ADD CONSTRAINT WORKER_UN_name
    UNIQUE(name);

ALTER TABLE WORKER DROP CONSTRAINT IF EXISTS WORKER_UN_rank;
ALTER TABLE WORKER ADD CONSTRAINT WORKER_UN_rank
    UNIQUE(rank);
--b
DROP DOMAIN IF EXISTS energy_dom;
CREATE DOMAIN energy_dom AS INTEGER
    CHECK (VALUE BETWEEN 0 AND 100);
ALTER TABLE SENSOR
    ADD energy energy_dom;
--c
ALTER TABLE FOREST DROP CONSTRAINT FOREST_UN_acid_level;
ALTER TABLE FOREST ADD CONSTRAINT FOREST_UN_acid_level
    CHECK(acid_level > 0 AND acid_level < 1);

--d
ALTER TABLE WORKER ADD COLUMN employing_state VARCHAR(2) NOT NULL;

---------------------------------------------------------------------------
--------QUESTION 3
---------------------------------------------------------------------------
--a

INSERT INTO FOREST VALUES ('1','Allegheny National Forest',3500,0.31,20,90,10,60);
INSERT INTO FOREST VALUES ('2','Pennsylvania Forest',2700,0.74,40,70,20,110);
INSERT INTO FOREST VALUES ('3','Stone Valley',5000,0.56,60,160,30,80);
INSERT INTO FOREST VALUES ('4','Big Woods',3000,0.92,150,180,20,120);
INSERT INTO FOREST VALUES ('5','Crooked Forest',2400,0.23,100,140,70,130);

INSERT INTO STATE (name, abbreviation, area, population) VALUES ('Pennsylvania', 'PA', '50000', '14000000');
INSERT INTO STATE (name, abbreviation, area, population) VALUES ('Ohio', 'OH', '45000', '12000000');
INSERT INTO STATE (name, abbreviation, area, population) VALUES ('Virginia', 'VA', '35000', '10000000');

INSERT INTO COVERAGE VALUES (1,'OH',1,3500);
INSERT INTO COVERAGE VALUES (2,'OH',1,2700);
INSERT INTO COVERAGE VALUES (3,'OH',0.3,1500);
INSERT INTO COVERAGE VALUES (3,'PA',0.42,2100);
INSERT INTO COVERAGE VALUES (3,'VA',0.28,1400);
INSERT INTO COVERAGE VALUES (4,'PA',0.4,1200);
INSERT INTO COVERAGE VALUES (4,'VA',0.6,1800);
INSERT INTO COVERAGE VALUES (5,'VA',1,2400);

INSERT INTO ROAD VALUES (1,'Forbes',500);
INSERT INTO ROAD VALUES (2,'Bigelow',300);
INSERT INTO ROAD VALUES (3,'Bayard',555);
INSERT INTO ROAD VALUES (4,'Grant',100);
INSERT INTO ROAD VALUES (5,'Carson',150);
INSERT INTO ROAD VALUES (6,'Greatview',180);
INSERT INTO ROAD VALUES (7,'Beacon',333);

INSERT INTO INTERSECTION VALUES (1,1);
INSERT INTO INTERSECTION VALUES (1,2);
INSERT INTO INTERSECTION VALUES (1,4);
INSERT INTO INTERSECTION VALUES (1,7);
INSERT INTO INTERSECTION VALUES (2,1);
INSERT INTO INTERSECTION VALUES (2,4);
INSERT INTO INTERSECTION VALUES (2,5);
INSERT INTO INTERSECTION VALUES (2,6);
INSERT INTO INTERSECTION VALUES (2,7);
INSERT INTO INTERSECTION VALUES (3,3);
INSERT INTO INTERSECTION VALUES (3,5);
INSERT INTO INTERSECTION VALUES (4,4);
INSERT INTO INTERSECTION VALUES (4,5);
INSERT INTO INTERSECTION VALUES (4,6);
INSERT INTO INTERSECTION VALUES (5,1);
INSERT INTO INTERSECTION VALUES (5,3);
INSERT INTO INTERSECTION VALUES (5,5);
INSERT INTO INTERSECTION VALUES (5,6);

INSERT INTO WORKER (ssn, name,  rank, employing_state) VALUES ('123456789','John',6, 'OH');
INSERT INTO WORKER (ssn, name,  rank, employing_state) VALUES ('121212121','Jason',5,'PA');
INSERT INTO WORKER (ssn, name,  rank, employing_state) VALUES ('222222222','Mike',4,'OH');
INSERT INTO WORKER (ssn, name,  rank, employing_state) VALUES ('333333333','Tim',2,'VA');

INSERT INTO SENSOR VALUES (1,33,29,TO_TIMESTAMP('6/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),'123456789',TO_TIMESTAMP('12/1/2020 22:00', 'mm/dd/yyyy hh24:mi'),6);
INSERT INTO SENSOR VALUES (2,78,24,TO_TIMESTAMP('7/9/2020 23:00', 'mm/dd/yyyy hh24:mi'),'222222222',TO_TIMESTAMP('11/1/2020 18:30', 'mm/dd/yyyy hh24:mi'),8);
INSERT INTO SENSOR VALUES (3,51,51,TO_TIMESTAMP('9/1/2020 18:30', 'mm/dd/yyyy hh24:mi'),'222222222',TO_TIMESTAMP('11/9/2020 8:25', 'mm/dd/yyyy hh24:mi'),4);
INSERT INTO SENSOR VALUES (4,67,49,TO_TIMESTAMP('9/9/2020 22:00', 'mm/dd/yyyy hh24:mi'),'121212121',TO_TIMESTAMP('12/6/2020 22:00', 'mm/dd/yyyy hh24:mi'),6);
INSERT INTO SENSOR VALUES (5,66,92,TO_TIMESTAMP('9/11/2020 22:00', 'mm/dd/yyyy hh24:mi'),'123456789',TO_TIMESTAMP('11/7/2020 22:00', 'mm/dd/yyyy hh24:mi'),6);
INSERT INTO SENSOR VALUES (6,100,52,TO_TIMESTAMP('9/13/2020 22:00', 'mm/dd/yyyy hh24:mi'),'121212121',TO_TIMESTAMP('11/9/2020 23:00', 'mm/dd/yyyy hh24:mi'),5);
INSERT INTO SENSOR VALUES (7,111,41,TO_TIMESTAMP('9/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),'222222222',TO_TIMESTAMP('11/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),2);
INSERT INTO SENSOR VALUES (8,120,75,TO_TIMESTAMP('10/13/2020 22:00', 'mm/dd/yyyy hh24:mi'),'123456789',TO_TIMESTAMP('11/13/2020 22:00', 'mm/dd/yyyy hh24:mi'),6);
INSERT INTO SENSOR VALUES (9,124,108,TO_TIMESTAMP('10/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),'333333333',TO_TIMESTAMP('11/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),7);
INSERT INTO SENSOR VALUES (10,153,50,TO_TIMESTAMP('11/10/2020 20:00', 'mm/dd/yyyy hh24:mi'),'333333333',TO_TIMESTAMP('11/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),1);
INSERT INTO SENSOR VALUES (11,151,33,TO_TIMESTAMP('11/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),'222222222',TO_TIMESTAMP('11/27/2020 22:00', 'mm/dd/yyyy hh24:mi'),2);
INSERT INTO SENSOR VALUES (12,151,73,TO_TIMESTAMP('11/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),'121212121',TO_TIMESTAMP('11/30/2020 9:03', 'mm/dd/yyyy hh24:mi'),2);
INSERT INTO SENSOR VALUES (13,100,20,TO_TIMESTAMP('11/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),NULL,TO_TIMESTAMP('11/30/2020 9:03', 'mm/dd/yyyy hh24:mi'),2);

INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('5/10/2020 22:00', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('5/24/2020 13:40', 'mm/dd/yyyy hh24:mi'),88);
INSERT INTO REPORT VALUES (12,TO_TIMESTAMP('6/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),87);
INSERT INTO REPORT VALUES (6,TO_TIMESTAMP('7/9/2020 23:00', 'mm/dd/yyyy hh24:mi'),38);
INSERT INTO REPORT VALUES (2,TO_TIMESTAMP('9/1/2020 18:30', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (1,TO_TIMESTAMP('9/1/2020 22:00', 'mm/dd/yyyy hh24:mi'),34);
INSERT INTO REPORT VALUES (3,TO_TIMESTAMP('9/5/2020 10:00', 'mm/dd/yyyy hh24:mi'),57);
INSERT INTO REPORT VALUES (4,TO_TIMESTAMP('9/6/2020 22:00', 'mm/dd/yyyy hh24:mi'),62);
INSERT INTO REPORT VALUES (5,TO_TIMESTAMP('9/7/2020 22:00', 'mm/dd/yyyy hh24:mi'),52);
INSERT INTO REPORT VALUES (3,TO_TIMESTAMP('9/9/2020 8:25', 'mm/dd/yyyy hh24:mi'),61);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('9/9/2020 22:00', 'mm/dd/yyyy hh24:mi'),37);
INSERT INTO REPORT VALUES (1,TO_TIMESTAMP('9/10/2020 20:00', 'mm/dd/yyyy hh24:mi'),58);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('9/10/2020 22:00', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (8,TO_TIMESTAMP('9/11/2020 2:00', 'mm/dd/yyyy hh24:mi'),44);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('9/11/2020 22:00', 'mm/dd/yyyy hh24:mi'),49);
INSERT INTO REPORT VALUES (8,TO_TIMESTAMP('9/13/2020 22:00', 'mm/dd/yyyy hh24:mi'),51);
INSERT INTO REPORT VALUES (9,TO_TIMESTAMP('9/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),55);
INSERT INTO REPORT VALUES (10,TO_TIMESTAMP('9/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),70);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('9/24/2020 13:40', 'mm/dd/yyyy hh24:mi'),88);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('9/27/2020 22:00', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (12,TO_TIMESTAMP('9/30/2020 9:03', 'mm/dd/yyyy hh24:mi'),60);
INSERT INTO REPORT VALUES (2,TO_TIMESTAMP('10/1/2020 18:30', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (1,TO_TIMESTAMP('10/1/2020 22:00', 'mm/dd/yyyy hh24:mi'),34);
INSERT INTO REPORT VALUES (3,TO_TIMESTAMP('10/5/2020 10:00', 'mm/dd/yyyy hh24:mi'),57);
INSERT INTO REPORT VALUES (5,TO_TIMESTAMP('10/7/2020 22:00', 'mm/dd/yyyy hh24:mi'),52);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('10/9/2020 22:00', 'mm/dd/yyyy hh24:mi'),37);
INSERT INTO REPORT VALUES (6,TO_TIMESTAMP('10/9/2020 23:00', 'mm/dd/yyyy hh24:mi'),38);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('10/10/2020 22:00', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('10/11/2020 22:00', 'mm/dd/yyyy hh24:mi'),49);
INSERT INTO REPORT VALUES (8,TO_TIMESTAMP('10/13/2020 22:00', 'mm/dd/yyyy hh24:mi'),51);
INSERT INTO REPORT VALUES (10,TO_TIMESTAMP('10/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),70);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('10/24/2020 13:40', 'mm/dd/yyyy hh24:mi'),88);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('10/27/2020 22:00', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (12,TO_TIMESTAMP('10/30/2020 9:03', 'mm/dd/yyyy hh24:mi'),60);
INSERT INTO REPORT VALUES (2,TO_TIMESTAMP('11/1/2020 18:30', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (3,TO_TIMESTAMP('11/5/2020 10:00', 'mm/dd/yyyy hh24:mi'),57);
INSERT INTO REPORT VALUES (3,TO_TIMESTAMP('11/6/2020 11:00', 'mm/dd/yyyy hh24:mi'),53);
INSERT INTO REPORT VALUES (4,TO_TIMESTAMP('11/6/2020 22:00', 'mm/dd/yyyy hh24:mi'),62);
INSERT INTO REPORT VALUES (5,TO_TIMESTAMP('11/7/2020 22:00', 'mm/dd/yyyy hh24:mi'),52);
INSERT INTO REPORT VALUES (3,TO_TIMESTAMP('11/9/2020 8:25', 'mm/dd/yyyy hh24:mi'),61);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('11/9/2020 22:00', 'mm/dd/yyyy hh24:mi'),37);
INSERT INTO REPORT VALUES (6,TO_TIMESTAMP('11/9/2020 23:00', 'mm/dd/yyyy hh24:mi'),38);
INSERT INTO REPORT VALUES (1,TO_TIMESTAMP('11/10/2020 20:00', 'mm/dd/yyyy hh24:mi'),58);
INSERT INTO REPORT VALUES (8,TO_TIMESTAMP('11/11/2020 2:00', 'mm/dd/yyyy hh24:mi'),44);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('11/11/2020 22:00', 'mm/dd/yyyy hh24:mi'),49);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('11/11/2020 22:00', 'mm/dd/yyyy hh24:mi'),76);
INSERT INTO REPORT VALUES (8,TO_TIMESTAMP('11/13/2020 22:00', 'mm/dd/yyyy hh24:mi'),51);
INSERT INTO REPORT VALUES (7,TO_TIMESTAMP('11/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),47);
INSERT INTO REPORT VALUES (9,TO_TIMESTAMP('11/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),55);
INSERT INTO REPORT VALUES (10,TO_TIMESTAMP('11/21/2020 22:00', 'mm/dd/yyyy hh24:mi'),70);
INSERT INTO REPORT VALUES (12,TO_TIMESTAMP('11/24/2020 13:40', 'mm/dd/yyyy hh24:mi'),77);
INSERT INTO REPORT VALUES (9,TO_TIMESTAMP('11/27/2020 22:00', 'mm/dd/yyyy hh24:mi'),33);
INSERT INTO REPORT VALUES (11,TO_TIMESTAMP('11/27/2020 22:00', 'mm/dd/yyyy hh24:mi'),46);
INSERT INTO REPORT VALUES (9,TO_TIMESTAMP('11/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),35);
INSERT INTO REPORT VALUES (12,TO_TIMESTAMP('11/28/2020 22:00', 'mm/dd/yyyy hh24:mi'),87);
INSERT INTO REPORT VALUES (12,TO_TIMESTAMP('11/30/2020 9:03', 'mm/dd/yyyy hh24:mi'),60);
INSERT INTO REPORT VALUES (1,TO_TIMESTAMP('12/1/2020 22:00', 'mm/dd/yyyy hh24:mi'),34);
INSERT INTO REPORT VALUES (4,TO_TIMESTAMP('12/6/2020 22:00', 'mm/dd/yyyy hh24:mi'),62);

---b
INSERT INTO WORKER (ssn, name,  rank, employing_state) VALUES ('123321456','Maria',3, 'OH');

--c
----violate entity constraint
INSERT INTO FOREST VALUES (1,'Angeles National Forest',6300,0.80,93,170,30,90);
----violate referential constraint
INSERT INTO SENSOR VALUES (14,101,21,TO_TIMESTAMP('11/29/2020 23:10', 'mm/dd/yyyy hh24:mi'),'987654321',TO_TIMESTAMP('11/30/2020 9:03', 'mm/dd/yyyy hh24:mi'),8);
----violate entity or not null constraint
INSERT INTO ROAD VALUES (NULL,'Bigelow',380);

---------------------------------------------------------------------------
--------QUESTION 4
---------------------------------------------------------------------------

---a
SELECT name
FROM FOREST
WHERE acid_level >= 0.65 AND acid_level <= 0.85;

---b
SELECT R.name
FROM ROAD R, FOREST F, INTERSECTION I
WHERE R.road_no = I.road_no AND F.name = 'Allegheny National Forest';

---c
SELECT S.sensor_id, W.name
FROM SENSOR S, WORKER W
WHERE S.maintainer = W.ssn OR S.maintainer IS NULL;

---d
SELECT C1.state, C2.state
FROM COVERAGE AS C1 JOIN COVERAGE C2 ON C1.forest_no = C2.forest_no
WHERE C1.state < C2.state
GROUP BY C1.state, C2.state;

---e
SELECT R.sensor_id, S.sensor_id, AVG(R.temperature) as AvgTemperature, COUNT(*) as NoSensors, F.name
FROM FOREST F, REPORT R, SENSOR S
WHERE R.sensor_id = S.sensor_id AND S.x > F.mbr_xmin
                                AND S.x < F.mbr_xmax
                                AND S.y > F.mbr_ymin
                                AND S.y < F.mbr_ymax
GROUP BY R.sensor_id, S.sensor_id, F.name
ORDER BY AvgTemperature DESC;

---f
SELECT COUNT(*) AS sensor_count, CASE
                                 WHEN W.name IS NULL
                                 THEN '-NO MAINTAINER-'
                                ELSE W.name
                                 END
FROM SENSOR S LEFT JOIN WORKER W on S.maintainer = W.ssn
GROUP BY W.name;

