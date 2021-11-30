-- • Add a table named EMERGENCY. It has two attributes, and it is defined as follows:
-- – EMERGENCY (sensor id, report time)
-- FK (sensor id, report time) → REPORT(sensor id, report time)
-- • Add an integer attribute called sensor count to the FOREST table with a default value of
-- zero.
DROP TABLE IF EXISTS EMERGENCY;
CREATE TABLE EMERGENCY(
    sensor_id integer,
    report_time timestamp NOT NULL,
    CONSTRAINT EMERGENCY_PK PRIMARY KEY (sensor_id, report_time),
    CONSTRAINT EMERGENCY_FK FOREIGN KEY (sensor_id, report_time) REFERENCES REPORT(sensor_id, report_time)
);

ALTER TABLE FOREST ADD sensor_count integer DEFAULT 0;

--
CREATE OR REPLACE PROCEDURE incrementSensorCount_proc (sensor_x real, sensor_y real)
AS $$
DECLARE
forest_cursor CURSOR FOR
        SELECT forest_no
        FROM FOREST
        WHERE $1 BETWEEN mbr_xmin AND mbr_xmax AND
              $2 BETWEEN mbr_ymin AND mbr_ymax;
forest_rec FOREST%ROWTYPE; --- look at reciatiation

BEGIN
    OPEN forest_cursor;
    LOOP
        FETCH forest_cursor INTO forest_rec;
        IF NOT FOUND THEN
            EXIT;
        END IF;

        UPDATE FOREST SET sensor_count = forest.sensor_count + 1;

    END LOOP;
    CLOSE forest_cursor;
END;
$$ LANGUAGE plpgsql;

---
CREATE OR REPLACE FUNCTION computePercentage(forest_no varchar(10), area_covered real)
RETURNS real
AS $$
DECLARE 
    ratio real;
BEGIN
    SELECT (area_covered/F.area)*100 into ratio
    FROM FOREST F
    WHERE F.forest_no = computePercentage.forest_no;
    RETURN ratio;
END;
$$ LANGUAGE plpgsql;

----

CREATE OR REPLACE FUNCTION sensorCount()
RETURNS TRIGGER AS $$
BEGIN 
    UPDATE FOREST f
    SET sensor_count = sensor_count + 1
    WHERE x BETWEEN f.mbr_xmin AND f.mbr_xmax AND
          y BETWEEN f.mbr_ymin AND f.mbr_ymax;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS sensorCount_tri on SENSOR;
CREATE TRIGGER sensorCount_tri
    AFTER INSERT 
    ON SENSOR
    FOR EACH ROW
EXECUTE PROCEDURE sensorCount();


-- Define a trigger percentage tri, so that when the area of a forest covered by some state
-- is updated, the trigger automatically update the corresponding value of percentage, using
-- the function computePercentage described above. Note: area in the COVERAGE
-- -- table is the area of a forest that spans the corresponding state state

CREATE OR REPLACE FUNCTION updatePercentage()
RETURNS TRIGGER 
AS $$
BEGIN
    new.percentage:= computePercentage(forest_no,area);
    RETURN new;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS percentage_tri on COVERAGE;
CREATE TRIGGER percentage_tri
    AFTER UPDATE
    ON COVERAGE
    FOR EACH ROW
EXECUTE PROCEDURE updatePercentage();

--  Define a trigger emergency tri, so that when a new report is inserted with the reported
-- temperature higher than 100 degrees, the trigger inserts a corresponding tuple into the table
-- EMERGENCY.

CREATE OR REPLACE FUNCTION emergency()
RETURNS TRIGGER AS $$
BEGIN 
   INSERT INTO EMERGENCY VALUES (NEW.sensor_id, NEW.report_time);
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS emergency_tri on REPORT;
CREATE TRIGGER sensorCount_tri
    AFTER INSERT 
    ON REPORT
    FOR EACH ROW
        WHEN (new.temperature > 100)
            EXECUTE PROCEDURE emergency();

-- You should create a trigger, called enforceMaintainer tri, that checks the maintainer of an added sensor whether they are employed within the same state as the sensor is
-- located. If the sensor’s location is outside the employing state of the maintainer, then the
-- sensor addition should fail.

CREATE OR REPLACE FUNCTION enforceMaintainer()
RETURNS TRIGGER AS
$$
DECLARE
    possibleState varchar(2);
    employeeState varchar(2);
BEGIN
    SELECT c.state into possibleState
    FROM FOREST f JOIN COVERAGE c ON f.forest_no = c.forest_no
    WHERE x BETWEEN f.mbr_xmin AND f.mbr_xmax AND
          y BETWEEN f.mbr_ymin AND f.mbr_ymin;

    SELECT employing_state into employeeState
    FROM WORKER w
    WHERE maintainer = w.ssn;

    IF employeeState = ANY (possibleState) THEN
        INSERT INTO SENSOR
        VALUES (sensor_id, x, y, last_charged, maintainer, last_read, energy);
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforceMaintainer_tri ON SENSOR;
CREATE TRIGGER enforceMaintainer_tri
    BEFORE INSERT
    ON SENSOR
    FOR EACH ROW
EXECUTE PROCEDURE enforceMaintainer();
-----