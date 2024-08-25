CREATE DATABASE IF NOT EXISTS US_Project;

USE US_Project;

CREATE TABLE IF NOT EXISTS income_backup AS SELECT * FROM us_household_income;

ALTER TABLE us_household_income
MODIFY COLUMN row_id INT DEFAULT NULL,
MODIFY COLUMN id INT DEFAULT NULL,
MODIFY COLUMN state_code INT DEFAULT NULL,
MODIFY COLUMN state_name TEXT,
MODIFY COLUMN state_ab TEXT,
MODIFY COLUMN county TEXT,
MODIFY COLUMN city TEXT,
MODIFY COLUMN place TEXT,
MODIFY COLUMN `type` TEXT,
MODIFY COLUMN `primary` TEXT,
MODIFY COLUMN zip_code INT DEFAULT NULL,
MODIFY COLUMN aland BIGINT DEFAULT NULL,
MODIFY COLUMN awater BIGINT DEFAULT NULL,
MODIFY COLUMN lat DOUBLE DEFAULT NULL,
MODIFY COLUMN lon DOUBLE DEFAULT NULL;

ALTER TABLE us_householdincome_statistics
MODIFY COLUMN id INT DEFAULT NULL,
MODIFY COLUMN state_name TEXT,
MODIFY COLUMN Mean INT DEFAULT NULL,
MODIFY COLUMN Median INT DEFAULT NULL,
MODIFY COLUMN Stdev INT DEFAULT NULL,
MODIFY COLUMN sum_w DOUBLE DEFAULT NULL;

SELECT * 
FROM us_household_income;

SELECT * 
FROM us_householdincome_statistics;

SELECT COUNT(id)
FROM us_household_income;

SELECT COUNT(id)
FROM us_householdincome_statistics;

#										SECTION A - DATA CLEANING

# PART 1 - DATA CLEANING

# Finding Duplicates - using COUNT() - US_Household_Income

SELECT id,
	   COUNT(id) AS counts
FROM us_household_income
GROUP BY 1
HAVING counts > 1;

# Finding Duplicates - using ROW_NUMBER() - US_Household_Income
SELECT row_id,
	   id
FROM
(
	SELECT row_id,
		   id,
		   ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS row_num
	FROM us_household_income
) duplicates
WHERE row_num > 1
;

# Deleting Duplicates - US_Household_Income

DELETE FROM us_household_income
WHERE row_id IN 
(
	SELECT row_id
	FROM
	(
		SELECT row_id,
			   id,
			   ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS row_num
		FROM us_household_income
	) duplicates
	WHERE row_num > 1
)
;

# Finding Duplicates - using COUNT() - us_householdincome_statistics

SELECT id,
	   COUNT(id) AS num
FROM us_householdincome_statistics
GROUP BY 1
HAVING num > 1
;

# PART 2 - STANDARDIZING COLUMNS

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

UPDATE us_householdincome_statistics
SET State_Name = UPPER(State_Name);

SELECT DISTINCT state_name
FROM us_householdincome_statistics
ORDER BY 1
;

UPDATE us_household_income
SET state_name = 'Georgia'
WHERE state_name = 'georia'
;

SELECT DISTINCT state_ab
FROM us_household_income
ORDER BY 1
;

SELECT *
FROM us_household_income
WHERE place IS NULL
;

UPDATE us_household_income
SET place = 'Autaugaville'
WHERE county = 'Autauga County'
	  AND city = 'Vinemont'
;

SELECT `type`,
	   COUNT(`type`) AS counts
FROM us_household_income
GROUP BY 1
;

UPDATE us_household_income
SET `type` = 'Borough'
WHERE `type` = 'Boroughs'
;

# PART 3 - REPLACING 0s, NULLS AND BLANKS

SELECT Aland,
	   Awater
FROM us_household_income
WHERE Aland IS NULL OR Aland = '' OR Aland = 0
;

UPDATE us_household_income
SET county = UPPER(county)
;

UPDATE us_household_income
SET city = UPPER(city)
;

UPDATE us_household_income
SET place = UPPER(place)
;

UPDATE us_household_income
SET state_name = UPPER(state_name)
;

UPDATE us_household_income
SET `type` = UPPER(`type`)
;

UPDATE us_household_income
SET `primary` = UPPER(`primary`)
;

UPDATE us_household_income
SET `type` = 'CDP'
WHERE `type` = 'CPD'
;


#										SECTION B - EXPLORATORY DATA ANALYSIS


# PART 1: LOOKING AT THE SUM OF LAND AND WATER FOR STATES
SELECT state_name,
	   SUM(Aland) AS sum_land,
       SUM(Awater) AS sum_water
FROM us_household_income
GROUP BY 1
ORDER BY 3 DESC
;

#PART 2: TOP 10 STATES WITH THE LARGEST LAND

#(i) USING LIMIT

SELECT state_name,
	   SUM(Aland) AS sum_land
FROM us_household_income
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
;

#(ii) USING DENSE_RANK()

WITH cte AS
(
	SELECT state_name,
		   SUM(Aland) AS sum_land,
		   DENSE_RANK() OVER(ORDER BY SUM(Aland) DESC) AS rnk
	FROM us_household_income
	GROUP BY 1
)
SELECT state_name,
	   sum_land
FROM cte
WHERE rnk <= 10
;

# PART 3: EDA BY COMBINING TABLES

SELECT *
FROM us_household_income ui
INNER JOIN us_householdincome_statistics us
	ON ui.id = us.id
WHERE mean <> 0 
;

SELECT us.state_name,
	   county,
       `type`,
       `primary`,
        mean,
        median
FROM us_household_income ui
INNER JOIN us_householdincome_statistics us
	ON ui.id = us.id
WHERE mean <> 0 
;

#(i) AVERAGE MEAN AND AVERAGE MEDIAN INCOME BY STATE

SELECT ui.state_name,
	   ROUND(AVG(mean),1) AS avg_mean,
       ROUND(AVG(median),1) AS avg_median
FROM us_household_income ui
INNER JOIN us_householdincome_statistics us
	ON ui.id = us.id
WHERE mean <> 0 
GROUP BY 1
ORDER BY 2;

#(ii) AVERAGE MEAN AND AVERAGE MEDIAN INCOME BY TYPE

SELECT `type`,
	    COUNT(ui.id) AS num_people,
	    ROUND(AVG(mean),1) AS avg_mean,
        ROUND(AVG(median),1) AS avg_median
FROM us_household_income ui
INNER JOIN us_householdincome_statistics us
	ON ui.id = us.id
WHERE mean <> 0 
GROUP BY 1
HAVING num_people > 100
ORDER BY 3 DESC
;

#(iii) TOP 3 AVERAGE MEAN INCOME BY CITY

SELECT ui.state_name,
	   city,
	   ROUND(AVG(mean),1) AS avg_mean
FROM us_household_income ui
INNER JOIN us_householdincome_statistics us
	ON ui.id = us.id
WHERE mean <> 0 
GROUP BY 1,2
;










