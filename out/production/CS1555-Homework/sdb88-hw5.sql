-- • Add a table named EMERGENCY. It has two attributes, and it is defined as follows:
-- – EMERGENCY (sensor id, report time)
-- FK (sensor id, report time) → REPORT(sensor id, report time)
-- • Add an integer attribute called sensor count to the FOREST table with a default value of
-- zero.

CREATE TABLE EMERGENCY(
    sensor_id integer,
    report_time timestamp NOT NULL;
    CONSTRAINT EMERGENCY_FK PRIMARY KEY (sensor_id, report_time), 
    CONSTRAINT EMERGENCY_FK FOREIGN KEY (sensor_id) REFERENCES REPORT(sensor_id)
    CONSTRAINT EMERGENCY_FK FOREIGN KEY (report_time) REFERENCES REPORT(report_time)
);

ALTER TABLE FOREST ADD sensor_count integer DEFAULT 0;

--
CREATE OR REPLACE PROCEDURE incrementSensorCount_proc(x real, y real)
LANGUAGE plpgsql --not sure if we need this
AS $$
BEGIN --- looking back at the instructions should this loop through all the forests
    SELECT F.forest_no AS NUM, F.mbr_xmin AS XMIN, F.mbr_xmax AS XMAX, F.mbr_ymin AS YMIN, F.mbr_ymax AS YMAX
    FROM FOREST AS F 

    IF (x < XMAX) AND (x > XMIN) AND (y < YMAX) AND (y > YMIN) ---NOT SURE IF WE ARE ALLOWED TO ADD PARENTHESIS
        UPDATE FOREST F SET sensor_count = sensor_count + 1;
        WHERE forest_no = NUM;
    END IF;
END;
---DO WE HAVE TO CALL THE PROCEDURE

---

CREATE OR REPLACE FUNCTION computePercentage(forest_no varchar(10), area_covered real)
RETURNS TRIGGER
AS $$
BEGIN
    SELECT c.forest_no as num, area_covered/forest_no as ratio
    FROM COVERAGE c
    WHERE c.forest_no = new.forest_no
    c.percentage = ratio * 100;  ---TO MAKE IT A PERCENTAGE
    RETURN new;
END;

----

--i think there is more to this but idk what it even is
--like it says forests where its located, so do we have to figure out which forest the new sensor is located in?
    
CREATE OR REPLACE FUNCTION sensorCount()
RETURNS TRIGGER AS $$
BEGIN 
    UPDATE TABLE FOREST SET sensor_count = sensor_count+1
    WHERE forest_no = new.forest_no;
    RETURN new;
END;
DROP TRIGGER IF EXISTS sensorCount_tri on SENSOR;
CREATE TRIGGER sensorCount_tri
AFTER INSERT 
ON SENSOR
FOR EACH ROW
EXCECUTE PROCEDURE sensorCount();

-- Define a trigger percentage tri, so that when the area of a forest covered by some state
-- is updated, the trigger automatically update the corresponding value of percentage, using
-- the function computePercentage described above. Note: area in the COVERAGE
-- -- table is the area of a forest that spans the corresponding state state

DROP TRIGGER IF EXISTS percentage_tri on COVERAGE;
CREATE TRIGGER percentage_tri
AFTER UPDATE
ON COVERAGE C
FOR EACH ROW
EXCECUTE PROCEDURE percentage(C.area, C.state);

--  Define a trigger emergency tri, so that when a new report is inserted with the reported
-- temperature higher than 100 degrees, the trigger inserts a corresponding tuple into the table
-- EMERGENCY.

CREATE OR REPLACE FUNCTION emergency()
RETURNS TRIGGER AS $$
BEGIN 
    UPDATE TABLE EMERGENCY SET report_time = new.report_time;
    WHERE sensor_id = new.sensor_id;
    RETURN new;
END;
DROP TRIGGER IF EXISTS emergency_tri on REPORT;
CREATE TRIGGER sensorCount_tri
AFTER INSERT 
ON REPORT R
FOR EACH ROW BEGIN
    IF(R.temperature > 100) 
        THEN EXCECUTE PROCEDURE EMERGENCY();
    END IF;

-- You should create a trigger, called enforceMaintainer tri, that checks the maintainer of an added sensor whether they are employed within the same state as the sensor is
-- located. If the sensor’s location is outside the employing state of the maintainer, then the
-- sensor addition should fail.
----i litterally have no idea

