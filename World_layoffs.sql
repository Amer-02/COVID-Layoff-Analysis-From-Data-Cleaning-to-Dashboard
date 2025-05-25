-- Make sure to select appropiate schema.

SELECT *
FROM layoffs;

-- ===================================================
-- DATA CLEANING 
-- ===================================================

-- 1. Create Staging Table for Cleaning
-- 2. Removing Duplicate Records
-- 3. Standardizing Data
-- 4. Dealing with NULL and Blank Values
-- 5. Removing Unnecessary Rows and Columns

-- STEP 1: Create Staging Table for Cleaning

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- STEP 2: Remove Duplicate Records

-- Identify duplicates using ROW_NUMBER
CREATE TABLE layoffs_staging2 AS
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

-- Delete rows where row_num > 1 (duplicates)
DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Drop the helper column after removing duplicates
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- STEP 3: Standardize Values

-- Remove extra spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize similar industry names (e.g., 'Crypto Currency', 'Crypto (Web3)' → 'Crypto')
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize country names
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Convert `date` column to DATE type
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- STEP 4: Handle NULLs and Blanks

-- Convert empty strings in 'industry' to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Backfill missing industry values using other records with same company/location
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
 AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- STEP 5: Remove Irrelevant Rows

-- Remove rows with no or bad layoff data 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
	OR percentage_laid_off = 0;

-- Final cleaned data
SELECT *
FROM layoffs_staging2;


-- ==========================================================
-- EDA: EXPLORATORY DATA ANALYSIS
-- ==========================================================

-- Identify patterns, trends, and interesting insights

SELECT *
FROM layoffs_staging2;

-- STEP 1: BASIC EXPLORATION

-- Max total layoffs in a single entry
SELECT MAX(total_laid_off) AS max_layoffs
FROM layoffs_staging2;

-- Max and Min percentage of layoffs
SELECT MAX(percentage_laid_off) AS max_percentage, MIN(percentage_laid_off) AS min_percentage
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- Companies that laid off 100% of staff (percentage = 1)
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

-- Same companies sorted by funding (most well-funded failures)
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- STEP 2: AGGREGATIONS & TRENDS

-- Companies with the biggest single-day layoff
SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY total_laid_off DESC
LIMIT 5;

-- Companies with the most total layoffs (cumulative)
SELECT company, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
GROUP BY company
ORDER BY layoff_total DESC
LIMIT 10;

-- Top locations by total layoffs
SELECT location, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
GROUP BY location
ORDER BY layoff_total DESC
LIMIT 10;

-- Layoffs by country
SELECT country, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
GROUP BY country
ORDER BY layoff_total DESC;

-- Layoffs by year
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY `year` ASC;

-- Layoffs by industry
SELECT industry, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
GROUP BY industry
ORDER BY layoff_total DESC;

-- Layoffs by funding stage
SELECT stage, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
GROUP BY stage
ORDER BY layoff_total DESC;

-- STEP 3: ADVANCED INSIGHTS

-- Top 3 companies with most layoffs per year
WITH Company_Year AS (
    SELECT company, YEAR(`date`) AS `year`, SUM(total_laid_off) AS layoff_total
    FROM layoffs_staging2
    WHERE total_laid_off IS NOT NULL
    GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS (
    SELECT company, `year`, layoff_total,
	DENSE_RANK() OVER (PARTITION BY `year` ORDER BY layoff_total DESC) AS ranking
    FROM Company_Year
)
SELECT company, `year`, layoff_total, ranking
FROM Company_Year_Rank
WHERE ranking <= 3 AND `year` IS NOT NULL
ORDER BY `year`, layoff_total DESC;

-- Monthly layoffs trend
SELECT DATE_FORMAT(date, '%Y-%m') AS `month`, SUM(total_laid_off) AS layoff_total
FROM layoffs_staging2
WHERE DATE_FORMAT(date, '%Y-%m') IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC;

-- Rolling cumulative layoffs by month
WITH Monthly_Layoffs AS (
    SELECT DATE_FORMAT(date, '%Y-%m') AS `month`, SUM(total_laid_off) AS layoff_total
    FROM layoffs_staging2
    GROUP BY `month`
)
SELECT `month`, layoff_total, SUM(layoff_total) OVER (ORDER BY `month` ASC) AS rolling_total_layoffs
FROM Monthly_Layoffs
WHERE `month` IS NOT NULL
ORDER BY `month` ASC;
