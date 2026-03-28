CREATE DATABASE MalariaAnalysis

/*
DATA CLEANING & STANDARDIZATION
This step transforms raw imported data into a clean, structured table
with proper data types and standardized column names for analysis
*/

IF OBJECT_ID('dbo.gf_cleaned', 'U') IS NOT NULL
DROP TABLE dbo.gf_cleaned;

SELECT 
    World,
    Continent1       AS Continent,
    SubContinent1    AS SubContinent,
    GeographyName1   AS Country,
    Component,
    IndicatorName    AS Indicator,
    TRY_CAST(Year AS INT)        AS Year,
    TRY_CAST(Result AS FLOAT)    AS Result,
    TRY_CAST(DateTimeCreated AS DATETIME) AS DateCreated
INTO dbo.gf_cleaned
FROM dbo.malaria_data
WHERE TRY_CAST(Result AS FLOAT) IS NOT NULL
  AND TRY_CAST(Year AS INT) IS NOT NULL;

/*
QUICK DATA VALIDATION
Preview dataset and validate structure, coverage, and time range
*/

  SELECT TOP 10 *
FROM dbo.gf_cleaned;

SELECT 
    COUNT(*) AS Total_Rows,
    COUNT(DISTINCT Country) AS Countries,
    COUNT(DISTINCT Component) AS Components,
    MIN(Year) AS Start_Year,
    MAX(Year) AS End_Year
FROM dbo.gf_cleaned;

/*
GLOBAL TREND ANALYSIS
Shows how disease burden (HIV, TB, Malaria) changes over time globally
*/

SELECT 
    Year,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
GROUP BY Year, Component
ORDER BY Year, Component;

/*
RAW GEOGRAPHIC SAMPLE
Provides a snapshot of data across regions for quick inspection
*/

SELECT TOP 20 
    Continent,
    SubContinent,
    Country,
    Component,
    Result
FROM dbo.gf_cleaned;

/*
CONTINENT-LEVEL DISEASE BURDEN
Aggregates total cases by continent and disease type
*/

SELECT 
    Continent,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
GROUP BY Continent, Component
ORDER BY Continent, Total_Value DESC;

/*
CONTINENT ANALYSIS WITH NULL HANDLING
Ensures missing continent values are categorized as 'Unknown'
*/

SELECT 
    ISNULL(Continent, 'Unknown') AS Continent,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
WHERE Country IS NOT NULL
GROUP BY ISNULL(Continent, 'Unknown'), Component
ORDER BY Continent, Total_Value DESC;

/*
CUSTOM CONTINENT GROUPING (STANDARDIZED REGIONS)
Groups subcontinents into broader global regions for better reporting
*/


SELECT 
    CASE 
        WHEN SubContinent IN ('Western Africa','Eastern Africa','Central Africa','Southern Africa') THEN 'Africa'
        WHEN SubContinent IN ('South Asia','East Asia','Southeast Asia') THEN 'Asia'
        WHEN SubContinent IN ('Western Europe','Eastern Europe') THEN 'Europe'
        WHEN SubContinent IN ('North America','Caribbean','Central America','South America') THEN 'Americas'
        ELSE 'Other'
    END AS Continent,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
GROUP BY 
    CASE 
        WHEN SubContinent IN ('Western Africa','Eastern Africa','Central Africa','Southern Africa') THEN 'Africa'
        WHEN SubContinent IN ('South Asia','East Asia','Southeast Asia') THEN 'Asia'
        WHEN SubContinent IN ('Western Europe','Eastern Europe') THEN 'Europe'
        WHEN SubContinent IN ('North America','Caribbean','Central America','South America') THEN 'Americas'
        ELSE 'Other'
    END,
    Component;

/*
DYNAMIC CONTINENT GROUPING USING PATTERN MATCHING
Alternative grouping using LIKE for more flexible classification
*/

SELECT 
    CASE 
        WHEN SubContinent LIKE '%Africa%' THEN 'Africa'
        WHEN SubContinent LIKE '%Asia%' THEN 'Asia'
        WHEN SubContinent LIKE '%Europe%' THEN 'Europe'
        WHEN SubContinent LIKE '%America%' THEN 'Americas'
        ELSE 'Other'
    END AS Continent_Group,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
GROUP BY 
    CASE 
        WHEN SubContinent LIKE '%Africa%' THEN 'Africa'
        WHEN SubContinent LIKE '%Asia%' THEN 'Asia'
        WHEN SubContinent LIKE '%Europe%' THEN 'Europe'
        WHEN SubContinent LIKE '%America%' THEN 'Americas'
        ELSE 'Other'
    END,
    Component
ORDER BY Continent_Group, Total_Value DESC;

/*
TOP 10 COUNTRIES BY DISEASE BURDEN
Identifies countries contributing the highest share of cases
*/

SELECT TOP 10
    Country,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
GROUP BY Country, Component
ORDER BY Total_Value DESC;

/*
YEAR-ON-YEAR GROWTH ANALYSIS
Measures growth rate and trends across diseases over time
*/

WITH yearly_data AS (
    SELECT 
        Component,
        Year,
        SUM(Result) AS Total_Value
    FROM dbo.gf_cleaned
    GROUP BY Component, Year
)

SELECT 
    Component,
    Year,
    Total_Value,
    LAG(Total_Value) OVER (PARTITION BY Component ORDER BY Year) AS Prev_Year,
    CASE 
        WHEN LAG(Total_Value) OVER (PARTITION BY Component ORDER BY Year) IS NULL 
        THEN NULL
        ELSE 
            (Total_Value - LAG(Total_Value) OVER (PARTITION BY Component ORDER BY Year)) * 100.0 /
            LAG(Total_Value) OVER (PARTITION BY Component ORDER BY Year)
    END AS YoY_Growth_Percent
FROM yearly_data
ORDER BY Component, Year;

/*
MALARIA-SPECIFIC ANALYSIS
Breaks down malaria indicators to understand intervention performance
*/

SELECT 
    Year,
    Indicator,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
WHERE Component = 'Malaria'
GROUP BY Year, Indicator
ORDER BY Year, Total_Value DESC;

/*
INDICATOR CONTRIBUTION ANALYSIS
Identifies which indicators contribute most to overall results
*/

WITH indicator_totals AS (
    SELECT 
        Component,
        Indicator,
        SUM(Result) AS Total_Value
    FROM dbo.gf_cleaned
    GROUP BY Component, Indicator
)
SELECT 
    Component,
    Indicator,
    Total_Value,
    Total_Value * 100.0 / SUM(Total_Value) OVER (PARTITION BY Component) AS Contribution_Percent
FROM indicator_totals
ORDER BY Component, Contribution_Percent DESC;

/*
HIGH BURDEN REGIONS
Highlights subcontinents with the highest disease burden
*/

SELECT 
    SubContinent,
    Component,
    SUM(Result) AS Total_Value
FROM dbo.gf_cleaned
GROUP BY SubContinent, Component
ORDER BY Total_Value DESC;

/*
DATA QUALITY CHECK
Evaluates completeness and diversity of dataset
*/

SELECT 
    COUNT(*) AS Total_Records,
    COUNT(CASE WHEN Result IS NULL THEN 1 END) AS Missing_Values,
    COUNT(DISTINCT Indicator) AS Indicators,
    COUNT(DISTINCT Country) AS Countries
FROM dbo.gf_cleaned;

/*
POWER BI VIEW CREATION
Creates a clean, reusable data layer for dashboard consumption
*/

CREATE OR ALTER VIEW dbo.vw_health_analysis AS
SELECT 
    Year,
    Continent,
    SubContinent,
    Country,
    Component,
    Indicator,
    Result
FROM dbo.gf_cleaned;

SELECT TOP 10 *
FROM dbo.vw_health_analysis;

SELECT DISTINCT Country
FROM dbo.gf_cleaned
ORDER BY Country;

SELECT 
    CASE 
        WHEN Country = 'USA' THEN 'United States'
        WHEN Country = 'Congo (Democratic Republic)' THEN 'Democratic Republic of the Congo'
        WHEN Country = 'Tanzania(United Republic)' THEN 'Tanzania'
        WHEN Country = 'Bulivia (Plurinational State' THEN 'Bulivia'
        WHEN Country = 'Iran (Islaic Republic)' THEN 'Iran'
        WHEN Country = 'Venezula (Bolivarian Republic)' THEN 'Venezuela'
        WHEN Country = 'Korea (Democratic Peoples Republic)' THEN 'Korea DPR'
        WHEN Country = 'Lao (Peoples Democratic Republic)' THEN 'Lao'
        ELSE Country
    END AS Clean_Country,
    *
INTO dbo.gf_geo_fixed
FROM dbo.gf_cleaned;

SELECT TOP 10 *
FROM dbo.gf_geo_fixed;

