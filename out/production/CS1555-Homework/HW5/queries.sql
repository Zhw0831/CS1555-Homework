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


---Called by add Forest to check if this state abbreviation exists in the State table.
--- if the employing state is not in the state table yet, we should insert the state into the state table first
CREATE OR REPLACE FUNCTION checkState(employing_state varchar(2)) RETURNS trigger
AS
$$
DECLARE
    insert integer;
BEGIN
    IF checkState.employing_state NOT IN (SELECT abbreviation FROM STATE) THEN
        INSERT INTO STATE
        VALUES (checkState.employing_state, checkState.employing_state, 0, 1);
        SELECT 1 INTO insert;
    ELSE
        SELECT 0 INTO insert;
    END IF;

    RETURN insert;
END;
$$ LANGUAGE plpgsql;

--- Task # 7
--- add a function: pick the name of the top k workers by ascending order of date and sensor energy level < 2
-- CREATE OR REPLACE FUNCTION topK() RETURNS SETOF WORKER
-- AS
-- $$
-- DECLARE
--     name_list varchar(30);
-- BEGIN
--     SELECT RANK() OVER(ORDER BY sensor_num_maintain DESC), name INTO name_list
--     FROM
--          (SELECT w.name, COUNT(sensor_id) AS sensor_num
--           FROM WORKER w JOIN SENSOR s on w.ssn = s.maintainer
--           WHERE s.energy <= 2
--           GROUP BY w.name) sensor_num_maintain;
--     RETURN name_list;
-- END;
-- $$ LANGUAGE plpgsql;

