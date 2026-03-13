Use world_layoffs;

select *
from `layoffs (1)`;

rename table `layoffs (1)` to layoffs;

create table layoffs_2
like layoffs;

insert into layoffs_2
select * 
from layoffs;

select * 
from layoffs_2;

select *,
row_number () over(partition by 
	company, industry, total_laid_off, 
	percentage_laid_off, `date`,stage, country, funds_raised_millions) as row_num
from layoffs_2;

with duplicates_cte as 
(
select *,
row_number () over(partition by 
	company, industry, total_laid_off, 
	percentage_laid_off, `date`) as row_num
from layoffs_2
)
select * 
from duplicates_cte
where row_num > 1;

create table layoffs_2_staging 
like layoffs_2;

alter table layoffs_2_staging
add column row_num int;

insert into layoffs_2_staging
select *,
row_number () over( 
	partition by company, location, industry, total_laid_off, percentage_laid_off, 
    `date`, stage, country, funds_raised_millions) as row_num
	from layoffs_2 ;

select * 
from layoffs_2_staging;

set sql_safe_updates = 0;
delete from layoffs_2_staging
where row_num > 1;
set sql_safe_updates = 1;

select *
from layoffs_2_staging;

select company, trim(company)
from layoffs_2_staging;


set sql_safe_updates = 0;
update layoffs_2_staging
SET industry = trim(industry),
	country = trim(country),
    `date` = trim(`date`);
set sql_safe_updates = 1;


select distinct industry
from layoffs_2_staging
order by 1;

select *
from layoffs_2_staging
where industry
like 'crypto%';

set sql_safe_updates = 0;
update layoffs_2_staging
set industry = 'crypto'
where industry 
like 'crypto%';
set sql_safe_updates = 1;

Select distinct industry 
from layoffs_2_staging;


select distinct industry
from layoffs_2_staging
order by 1;

select distinct country
from layoffs_2_staging
order by 1;

select distinct country
from layoffs_2_staging
where country like 'United States%';

set sql_safe_updates = 0;
update layoffs_2_staging
set country = trim(trailing '.' from country)
where country 
like 'United States%';
set sql_safe_updates = 1;

select distinct country
from layoffs_2_staging;

select * 
from layoffs_2_staging;

select distinct `date`
from layoffs_2_staging;

select `date`,
str_to_date(`date`, '%m/%d/%Y') 
from layoffs_2_staging;

set sql_safe_updates = 0;

update layoffs_2_staging
set `date` = str_to_date(`date`, '%m/%d/%Y')
where `date` like '%/%';

set sql_safe_updates = 1;

select `date` from layoffs_2_staging limit 5;

alter table layoffs_2_staging
modify column `date` date;

select * 
from layoffs_2_staging;

set sql_safe_updates = 0;

select *
from layoffs_2_staging
where total_laid_off is null
and percentage_laid_off is null;


set sql_safe_updates = 0;

update layoffs_2_staging 
set industry = null
where industry = ''; 

set sql_safe_updates = 1;

select *
from layoffs_2_staging
where industry is null
or industry = '';


select *
from layoffs_2_staging as t1
join layoffs_2_staging as t2
	 on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

set sql_safe_updates = 0;

update layoffs_2_staging as t1
join layoffs_2_staging as t2
	 on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null; 

set sql_safe_updates = 1;

select *
from layoffs_2_staging
where industry = '';

select *
from layoffs_2_staging
where company = 'Airbnb';

set sql_safe_updates = 0;

delete 
from layoffs_2_staging 
where total_laid_off is null
and percentage_laid_off is null;

set sql_safe_updates = 1; 

select *
from layoffs_2_staging;


alter table layoffs_2_staging
drop column row_num;


-- Exploratory Data Analysis 

select * 
from layoffs_2_staging;

-- ================================
-- 1. BASIC EXPLORATION
-- ================================
-- What's the maximum number of people laid off in a single event?
-- What's the maximum percentage laid off?
select max(total_laid_off), max(percentage_laid_off)
from layoffs_2_staging;

-- ================================
-- 2. COMPANY INSIGHTS
-- ================================
-- Which companies shut down completely (100% laid off)?
select *
from layoffs_2_staging
where percentage_laid_off = 1
and total_laid_off is not null
order by total_laid_off desc;

-- Which company laid off the most people in total across all events?
select company, sum(total_laid_off)
from layoffs_2_staging
where total_laid_off is not null
group by company
order by sum(total_laid_off) desc;

-- Which companies laid off multiple times — are they struggling more?
select company, count(total_laid_off)
from layoffs_2_staging
group by company
order by count(total_laid_off) desc;

-- Which were the top 5 companies with most layoffs per year?
with company_year as (
    select company, year(`date`) as year, sum(total_laid_off) as total
    from layoffs_2_staging
    group by company, year(`date`)
),
company_year_rank AS (
    select *, dense_rank() over(partition by `year` order by total desc) as ranking
    from company_year
)
select *
from company_year_rank
where ranking <= 5;

-- Do companies with more funding lay off more or less?
select company, funds_raised_millions, total_laid_off
from layoffs_2_staging
order by funds_raised_millions desc;

select *
from layoffs_2_staging;

-- ================================
-- 3. INDUSTRY INSIGHTS
-- ================================
-- Which industry was hit hardest in total?

select industry, sum(total_laid_off)
from layoffs_2_staging
group by industry
order by sum(total_laid_off) desc; 

-- Which industries had the highest percentage laid off vs total numbers?

select industry, round(avg(percentage_laid_off), 2)
from layoffs_2_staging
group by industry
order by avg(percentage_laid_off) desc; 

-- Which industry recovered fastest?

select 
    industry,
    sum(case when year (`date`) = 2022 then total_laid_off else 0 end) as layoffs_2022,
    sum(case when year (`date`) = 2023 then total_laid_off else 0 end) as layoffs_2023
from layoffs_2_staging
group by industry
order by layoffs_2023 ASC;

-- Which industries had the highest company death rate?

Select industry, count(*) as total_companies,
	sum(case when percentage_laid_off = 1
    then 1 else 0 end ) as shutdowns
from layoffs_2_staging 
group by industry
order by industry desc;

-- ================================
-- 4. LOCATION INSIGHTS
-- ================================

-- Which country had the most layoffs?

select *
from layoffs_2_staging;

select country, sum(total_laid_off)
from layoffs_2_staging
group by country 
order by sum(total_laid_off) desc;


-- Which cities/locations were hit hardest?

select location, sum(total_laid_off)
from layoffs_2_staging
group by location
order by sum(total_laid_off) desc;

-- Were layoffs concentrated in Silicon Valley or spread globally?

-- ================================
-- 5. FUNDING INSIGHTS
-- ================================
-- Which funding stage had the most layoffs?

select 
    case 
        when funds_raised_millions < 100  then '< $100m'
        when funds_raised_millions < 500  then '$100m–$500m'
        when funds_raised_millions < 1000 then '$500m–$1b'
        else '> $1b'
    end as funding_tier,
    sum(total_laid_off) as total_laid_off
from layoffs_2_staging
where total_laid_off is not null
group by funding_tier
order by total_laid_off desc;

-- Did companies that raised more money lay off more people?

select 
    case 
        when funds_raised_millions < 100  then '< $100m'
        when funds_raised_millions < 500  then '$100m–$500m'
        when funds_raised_millions < 1000 then '$500m–$1b'
        else '> $1b'
    end as funding_tier,
    round(avg(total_laid_off), 0) as avg_laid_off
from layoffs_2_staging
where total_laid_off is not null
group by funding_tier
order by avg_laid_off desc;

-- ================================
-- 6. TIME INSIGHTS
-- ================================
-- What is the date range of our data?
select min(`date`), max(`date`)
from layoffs_2_staging;


-- Which year had the most layoffs?

select year (`date`)  as year, sum(total_laid_off)
from layoffs_2_staging
group by year (`date`)
order by sum(total_laid_off) desc;

-- Which month consistently has the most layoffs?
select month (`date`) as month, sum(total_laid_off)
from layoffs_2_staging
group by month(`date`)
order by sum(total_laid_off) desc;


-- Rolling total of layoffs month by month

with rolling_total as (
    select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total
    from layoffs_2_staging
    where substring(`date`, 1, 7) is not null
    group by `month`
    order by `month`
)
select `month`, total,
    sum(total) over(order by `month`) as rolling_total
from rolling_total;

-- Were there any sudden spikes?
-- see rolling total above. january 2023 shows the largest single month spike (92,037)


-- ================================
-- 7. SURVIVAL INSIGHTS
-- ================================
-- What % of companies completely shut down?

select 
    round(sum(case when percentage_laid_off = 1 
    then 1 else 0 end) / count(*) * 100, 2) as shutdown_pct
from layoffs_2_staging;

-- Which industries had the highest company death rate?

select 
    industry,
    count(*) as total_companies,
    sum(case when percentage_laid_off = 1 then 1 else 0 end) as shutdowns,
    round(sum(case when percentage_laid_off = 1 
    then 1 else 0 end) / count(*) * 100, 1) as shutdown_rate_pct
from layoffs_2_staging
where industry is not null
group by industry
order by shutdown_rate_pct desc;












































