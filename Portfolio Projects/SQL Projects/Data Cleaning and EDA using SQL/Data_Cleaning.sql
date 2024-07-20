-- Data Cleaning

select *
from world_layoffs.layoffs;

-- 1 Remove Duplicates
-- 2 Standardize the Data
-- 3 Null Values or Blank values
-- 4 Remove Any Columns

-- Creating a seperate table for Data cleaning purpuse

create table layoffs_staging 
like layoffs;

select*
from layoffs_staging;

insert layoffs_staging 
select *
from layoffs;

-- finding duplicates

select*, row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_rum
from layoffs_staging;

-- now for further filtering we consider using cte/subquery

with layoff_cte as 
(
select*, row_number() over(
partition by company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_rum
from layoffs_staging
)
select *
from layoff_cte
where row_rum > 1;

select *
from layoffs_staging
where company = 'casper';

-- The results of the following code shows duplicate rows but we cannot delete using cte
-- now creating a new table with additional column for row number for simplicity

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select*, row_number() over(
partition by company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;

-- As we can see the above code shows duplicate rows so we'll delete those duplicate rows

delete 
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2;

-- Standardizing Data

select company, trim(company)
from layoffs_staging2;

-- As we can see some blank spaces on the column company so we'll remove those and update in the database

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'crypto%';

-- as we can see in the industry column we have 3 different names for the same industry so we'll change that 

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select *
from layoffs_staging2
where country like 'United States%'
order by 1;

-- as we can see the country column we have different names for united states so we'll change that 

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- now as we can see that the date column data type is text which should be date column so we'll change that

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- it is important to format the date before changing the data type so now we can change the data type

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2
where industry is null
or industry = '';

-- As we can see some of the industry names are not present so lets populate the industry column

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- for simplicity lets make all the blank data to null

update layoffs_staging2
set industry = null
where industry = '';

-- now with this we can clearly see that we have the industry data for the given company name which we can populate

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null )
and t2.industry is not null;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Now as we can see we are doing analysis on the total laid of people in the company but for some company we don't have total laid off 
-- or the percentage for it and we cannot do analysis with these data
-- so we'll delete those data for better analysis

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
order by company asc;

-- now we dont require the row column so we'll drop the column and hence acquire a fully cleansed data

alter table layoffs_staging2
drop column row_num;
