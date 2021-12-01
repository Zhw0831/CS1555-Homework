------------------------------- New Functionalities
-------- Schema Evolution
-- emergency table
DROP TABLE IF EXISTS  EMERGENCY;
CREATE TABLE EMERGENCY
(
    sensor_id   integer,
    report_time timestamp NOT NULL,
    CONSTRAINT EMERGENCY_PK PRIMARY KEY (sensor_id, report_time),
    CONSTRAINT EMERGENCY_FK FOREIGN KEY (sensor_id, report_time) REFERENCES REPORT (sensor_id, report_time)
);

-- sensor_count attribute
ALTER TABLE FOREST ADD sensor_count integer DEFAULT 0;


-------- Procedures and Functions
-- stored procedure
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

-- function
CREATE OR REPLACE FUNCTION computePercentage (forest_no integer, area_covered real) RETURNS real AS
    $$
    DECLARE
        ratio real;
    BEGIN
        SELECT area_covered/area INTO ratio
        FROM FOREST F
        WHERE F.forest_no = computePercentage.forest_no;
        RETURN ratio;
    END;
    $$ LANGUAGE plpgsql;


-------- Triggers
-- sensorCount_tri
CREATE OR REPLACE FUNCTION incrementSensorCount_func()
RETURNS TRIGGER AS
$$
BEGIN
    UPDATE FOREST f
    SET sensor_count = sensor_count + 1
    WHERE x BETWEEN f.mbr_xmin AND f.mbr_xmax AND
          y BETWEEN f.mbr_ymin AND f.mbr_ymax;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS sensorCount_tri ON SENSOR;
CREATE TRIGGER sensorCount_tri
    AFTER INSERT
    ON SENSOR
    FOR EACH ROW
EXECUTE PROCEDURE incrementSensorCount_func();


-- percentage_tri
CREATE OR REPLACE FUNCTION updatePercentage()
RETURNS TRIGGER AS
$$
BEGIN
    NEW.percentage := computePercentage(NEW.forest_no, NEW.area);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS percentage_tri ON COVERAGE;
CREATE TRIGGER percentage_tri
    BEFORE UPDATE
    ON COVERAGE
    FOR EACH ROW
EXECUTE PROCEDURE updatePercentage();


-- emergency_tri
CREATE OR REPLACE FUNCTION insertIntoEmergency()
RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO EMERGENCY
    VALUES (NEW.sensor_id, NEW.report_time);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS emergency_tri ON REPORT;
CREATE TRIGGER emergency_tri
    BEFORE INSERT OR UPDATE
    ON REPORT
    FOR EACH ROW
    WHEN (NEW.temperature > 100)
EXECUTE PROCEDURE insertIntoEmergency();


-- enforceMaintainer_tri
-- comment: my confusion is how to get the state of the sensor. we can only know which forest the sensor is in. if the sensor
-- is in a forest, it doesn't guarantee that it is in any states because a forest can be spanned by several states.
-- however, I don't think we have other ways to figure out the problem so I have to do like this:
CREATE OR REPLACE FUNCTION checkMaintainer()
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

    RETURN NULL;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforceMaintainer_tri ON SENSOR;
CREATE TRIGGER enforceMaintainer_tri
    BEFORE INSERT
    ON SENSOR
    FOR EACH ROW
EXECUTE PROCEDURE checkMaintainer();
--------------------------------------------------------------------
----called by switch worker to check if workers are eligible to swap
---Param: the names of the workers
---Returns true if they are eligible to swap, false if not
CREATE OR REPLACE FUNCTION checkSwapState(name1 varchar(30), name2 varchar(30))
RETURNS BOOLEAN AS $$
DECLARE
w1 varchar(2);
    w2 varchar(2);
BEGIN
SELECT W.employing_state INTO w1
FROM WORKER W
WHERE W.NAME = checkSwapState.name1;

SELECT W2.employing_state INTO w2
FROM WORKER W2
WHERE W2.NAME = checkSwapState.name2;

IF w1 = w2
        THEN RETURN TRUE;
ELSE RETURN FALSE;
END IF;
end;
$$ LANGUAGE plpgsql;