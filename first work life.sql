-- DATA CLEANING


SELECT*
FROM layoffs;

-- 1. Remove duplicate
-- 2. standadize the data
-- 3. null values or blank values
-- 4. remove any columns



CREATE TABLE layoffs_tag
LIKE layoffs;


SELECT*
FROM layoffs_tag;

INSERT layoffs_tag

SELECT*
FROM layoffs;

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company,industry, total_laid_off, percentage_laid_off, 'date',  stage, country) AS row_num 
 FROM layoffs_tag;
 
 
 WITH duplicate_cte AS
 (
 SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',  stage, country, funds_raised_millions) AS row_num
FROM layoffs_tag
)
SELECT*
FROM duplicate_cte
WHERE row_num >1;



SELECT*
FROM  layoffs_tag
WHERE Company = 'casper';


 WITH duplicate_cte AS
 (
 SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',  stage, country, funds_raised_millions) AS row_num
FROM layoffs_tag
)
DELETE
FROM duplicate_cte
WHERE row_num >1;




CREATE TABLE `layoffs_tag2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT*
FROM layoffs_tag2
WHERE row_num >1;

INSERT INTO layoffs_tag2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',  stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_tag;


DELETE
FROM layoffs_tag2
WHERE row_num >1;

SELECT*
FROM layoffs_tag2;




--  Standardizing data

SELECT company, TRIM(company)
FROM layoffs_tag2;

UPDATE layoffs_tag2
SET company = TRIM(company);

SELECT *
FROM layoffs_tag2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_tag2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT Country, TRIM(TRAILING '.' FROM country)
FROM layoffs_tag2
ORDER BY 1;

UPDATE layoffs_tag2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE  'United States%';

SELECT  `date`
FROM layoffs_tag2;

UPDATE layoffs_tag2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_tag2
MODIFY COLUMN `date` DATE;




-- null and blank


SELECT*
FROM layoffs_tag2
WHERE _total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_tag2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_tag2
WHERE industry IS NULL
OR industry ='';

SELECT*
FROM layoffs_tag2
WHERE company = 'airbnb';





SELECT t1.industry, t2.industry
FROM layoffs_tag2 t1
JOIN layoffs_tag2 t2
  ON t1.company = t2.company
  AND t1.location =t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;


UPDATE layoffs_tag2 t1
JOIN layoffs_tag2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL ;


SELECT*
FROM layoffs_tag2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM layoffs_tag2;

DELETE
FROM layoffs_tag2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT*
FROM layoffs_tag2;

ALTER TABLE layoffs_tag2
DROP COLUMN row_num;
