SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
-- SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

SET global time_zone = '-5:00'; -- commented out in this version

SET SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';
  
-- DROP SCHEMA IF EXISTS neodb; -- commenting out for Maven Course to avoid concerning warning message
CREATE SCHEMA neodb;
USE neodb;

--
-- Creating an empty shell for the table 'neows'. We will populate it later. 
--

CREATE TABLE neo_approaches (
                neows_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                created_at TIMESTAMP NOT NULL,
                id BIGINT NOT NULL,
                name VARCHAR(50),
                close_approach_date TIMESTAMP,
                miss_distance DECIMAL(10,5)
                PRIMARY KEY (neows_id),
            );

SELECT close_approach_date, min(miss_distance) min_miss_distance, count(*) count_hazardous
FROM neo_approaches
group by close_approach_date
order by close_approach_date;

