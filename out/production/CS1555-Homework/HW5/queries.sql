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



SELECT sensor_id, count(report_time)
FROM REPORT
GROUP BY sensor_id
HAVING count(report_time) IS NOT NULL
ORDER BY count(report_time) DESC
FETCH FIRST 3 ROWS ONLY;

----THIS IS GETTING THE NAMES OF ALL THE PEOPLE WHO HAVE SENSORS BELOW 2
SELECT W.name, S.sensor_id
FROM WORKER W JOIN SENSOR S ON W.ssn = S.maintainer
WHERE S.energy <= 2;

SELECT W.name, count(S.sensor_id)
FROM WORKER W JOIN SENSOR S ON W.ssn = S.maintainer
GROUP BY W.name
FETCH FIRST 3 ROWS ONLY;
----------

SELECT A.name, count(sensor_id) FROM (SELECT name, sensor_id FROM WORKER JOIN sensor ON worker.ssn = sensor.maintainer WHERE energy <= 2) AS A
GROUP BY A.name FETCH FIRST 3 ROWS ONLY;