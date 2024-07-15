-- Ecploratory Data Analysis

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off )
from layoffs_staging2;
-- Max no of people laid off in a single day and max percentage of laid off

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;
-- No of company that got 100% laid off that is company went shut

select company, sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 desc;

-- The company which has the highest total amount of laid offs 

select min(`date`), max(`date`)
from layoffs_staging2;

-- the time frame of data that we have

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry  
order by 2 desc;

-- The industry which has the highest total amount of laid offs 

select country, sum(total_laid_off)
from layoffs_staging2
group by country 
order by 2 desc;

-- The country which has the highest total amount of laid offs 

select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date` 
order by 1      desc;

-- The total sum of laid offs on a single day

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`) 
order by 1      desc;

-- The total sum of laid offs on a single year

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage 
order by 2      desc;

-- Total laid of for the different stages of the company

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by `month`;

with rolling_total as 
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as laidoff
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by `month`
)
select `month`, laidoff,sum(laidoff) over(order by `month`) as rolling_total 
from rolling_total;

-- For calculating the Rollin total of total laid off by month and year


with company_years(company, country, years, total_laid_off) as
(
select company,country, year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`),country 
order by 3 desc 
), company_year_rank as
(
select *, dense_rank() over (partition by years order by total_laid_off desc) as total_ranking
from company_years
where years is not null

)
select *
from company_year_rank
where total_ranking <= 5;

-- Ranking the top 5 company and their country filtered by years and highest no of laid off