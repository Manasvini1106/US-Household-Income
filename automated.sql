USE US_project;

DELIMITER $$
DROP PROCEDURE IF EXISTS copy_and_clean_data;

CREATE PROCEDURE copy_and_clean_data()
BEGIN

-- CREATING THE TABLE
	CREATE TABLE IF NOT EXISTS `us_household_income_cleaned` (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `state_code` int DEFAULT NULL,
	  `state_name` text,
	  `state_ab` text,
	  `county` text,
	  `city` text,
	  `place` text,
	  `type` text,
	  `primary` text,
	  `zip_code` int DEFAULT NULL,
	  `Area_Code` text,
	  `aland` bigint DEFAULT NULL,
	  `awater` bigint DEFAULT NULL,
	  `lat` double DEFAULT NULL,
	  `lon` double DEFAULT NULL,
	  `timestamp` TIMESTAMP DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
-- COPY DATA TO NEW TABLE
	INSERT INTO `us_household_income_cleaned`
	SELECT *, CURRENT_TIMESTAMP 
    FROM us_household_income;
    
-- CLEANING DATA
	# Remove Duplicates
	DELETE FROM us_household_income_cleaned
	WHERE 
		row_id IN (
		SELECT row_id
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id, `timestamp`
				ORDER BY id, `timetsamp`) AS row_num
		FROM 
			us_household_income_cleaned
	) duplicates
	WHERE 
		row_num > 1
	);

	-- Fixing some data quality issues by fixing typos and general standardization
	UPDATE us_household_income_cleaned
	SET State_Name = 'Georgia'
	WHERE State_Name = 'georia';

	UPDATE us_household_income_cleaned
	SET County = UPPER(County);

	UPDATE us_household_income_cleaned
	SET City = UPPER(City);

	UPDATE us_household_income_cleaned
	SET Place = UPPER(Place);

	UPDATE us_household_income_cleaned
	SET State_Name = UPPER(State_Name);

	UPDATE us_household_income_cleaned
	SET `Type` = 'CDP'
	WHERE `Type` = 'CPD';

	UPDATE us_household_income_cleaned
	SET `Type` = 'Borough'
	WHERE `Type` = 'Boroughs';

END $$
DELIMITER ;

CALL copy_and_clean_data();

-- CREATE EVENT
DROP EVENT IF EXISTS run_data_cleaning;

CREATE EVENT run_data_cleaning
	ON SCHEDULE EVERY 2 MINUTE
    DO CALL copy_and_clean_data()
;

SELECT DISTINCT `timestamp`
FROM us_household_income_cleaned
;

-- CREATE TRIGGER - will not work because of COMMIT statement in procedure

DELIMITER $$
DROP TRIGGER IF EXISTS transfer_clean_data;

CREATE TRIGGER trasfer_clean_data
	AFTER INSERT ON us_household_income
    FOR EACH ROW
    BEGIN
		CALL copy_and_clean_data();
    END $$
DELIMITER ;

