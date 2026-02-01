Select 
sum(case when s.RespondentStatus is not null then 1 else 0 end) as starts ,
Sum(case when S.RespondentStatus = 1 then 1 else 0 end) as complete , 
Sum(case when s.RespondentStatus = 1 then S.Revenue else 0 end) as revenue, 
Sum(case when S.RespondentStatus = 1 then S.cost else 0 end) as cost, 
DATEPART(HOUR, S.Survey_EndDate) As Hour, 
CAST(S.Survey_EndDate AS DATE) AS only_date 
From Survey_Info S 
where S.Survey_EndDate >= DATEADD(DAY, -7, GETDATE()) 
Group by CAST(S.Survey_EndDate AS DATE), DATEPART(HOUR, S.Survey_EndDate)






SELECT top 10 *
FROM Survey_Info S where s.Survey_EndDate > '2026-01-07'

CREATE VIEW dbo.vw
SELECT  top 100
    CONVERT(date, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS only_date,
    DATEPART(hour, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS hours,
    COUNT(*) AS total_records,
    SUM(CASE WHEN S.RespondentStatus IS NOT NULL THEN 1 ELSE 0 END) AS starts,
    SUM(CASE WHEN S.RespondentStatus = 1 THEN 1 ELSE 0 END) AS completes,
	SUM(CASE WHEN RespondentStatus = 10 THEN 1 ELSE 0 END) AS reconcile,
    SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Revenue ELSE 0 END) AS total_revenue,
    SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Cost ELSE 0 END) AS total_cost,
    C.ClientName,
    su.SupplierName,
    l.CountryCode
FROM APIClient_Surveys ss
JOIN Survey_Info S 
    ON ss.SurveyId = S.SurveyId
JOIN Clients C 
    ON ss.ClientId = C.ClientId
JOIN Suppliers su 
    ON su.SupplierId = S.SupplierId
JOIN Languages l 
    ON l.LanguageId = ss.LanguageId
WHERE S.Survey_EndDate BETWEEN '2025-09-01' AND '2025-12-01'
GROUP BY 
    CONVERT(date, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'),
    DATEPART(hour, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'),
    C.ClientName,
    su.SupplierName,
    l.CountryCode

    

	select top 10 * from vw_Survey_Hourly_DQ
 




 SELECT  top 100
    CONVERT(date, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS Dates,
    'W' + CAST(DATEPART(ISO_WEEK, S.Survey_EndDate) AS varchar(2)) AS Week,
    SUM(CASE WHEN S.RespondentStatus IS NOT NULL THEN 1 ELSE 0 END) AS Starts,
    SUM(CASE WHEN S.RespondentStatus = 1 THEN 1 ELSE 0 END) AS Completes,
    SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Revenue ELSE 0 END) AS Revenue,
    SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Cost ELSE 0 END) AS Cost,
    (SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Revenue ELSE 0 END)) - (SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Cost ELSE 0 END)) AS Profit,
    SUM(CASE WHEN S.RespondentStatus = 10 THEN 1 ELSE 0 END) AS Reconcile,
    ROUND(CAST(SUM(CASE WHEN S.RespondentStatus = 1 THEN 1 ELSE 0 END) AS FLOAT)/ NULLIF(COUNT(1), 0) * 100, 2) AS Conversion,
    ROUND(
        CASE 
            WHEN SUM(CASE WHEN S.RespondentStatus = 1 THEN 1 ELSE 0 END) = 0 THEN 0 
            ELSE CAST(SUM(CASE WHEN S.RespondentStatus = 10 THEN 1 ELSE 0 END) AS FLOAT) / 
           (SUM(CASE WHEN S.RespondentStatus = 10 THEN 1 ELSE 0 END) + SUM(CASE WHEN S.RespondentStatus = 1 THEN 1 ELSE 0 END)) * 100
        END, 2) AS Reconcile_Rate,
        (SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Cost ELSE 0 END))/(SUM(CASE WHEN S.RespondentStatus IS NOT NULL THEN 1 ELSE 0 END)) as CPC,
        (SUM(CASE WHEN S.RespondentStatus = 1 THEN S.Revenue ELSE 0 END))/(SUM(CASE WHEN S.RespondentStatus IS NOT NULL THEN 1 ELSE 0 END)) as EPC,

    C.ClientName,
    su.SupplierName,
    l.CountryCode,
    (case when S.RespondentStatus = 14 then S.TermQualificationId end) as TerminationId,
    Q.QualificationName as Termination_Reason,
    COUNT(*) AS Termination_Count
FROM APIClient_Surveys ss
left JOIN Survey_Info S 
    ON ss.SurveyId = S.SurveyId
left JOIN Clients C 
    ON ss.ClientId = C.ClientId
left JOIN Suppliers su 
    ON su.SupplierId = S.SupplierId
left JOIN Languages l 
    ON l.LanguageId = ss.LanguageId
left join Qualifications(nolock)as Q on Q.QualificationId = S.TermQualificationId 

 
WHERE S.Survey_EndDate >= DATEADD(day, -7, SYSDATETIME())
 
GROUP BY 
    CONVERT(date, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'),
    C.ClientName,
    su.SupplierName,
    l.CountryCode,
    S.TermQualificationId,
    Q.QualificationName,S.RespondentStatus,
    s.Survey_EndDate
    
ORDER BY Dates DESC;



SELECT  
    S.TermQualificationId,
    Q.QualificationName,
    COUNT(*) AS total_count,
	C.ClientName,
	l.CountryCode,
	su.SupplierName as SupplierName,
	  MONTH(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS month,
    CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) AS only_date
from  APIClient_Surveys(nolock) as ss
	inner join Survey_Info S (nolock) on ss.SurveyId= S.SurveyId 
	left join Qualifications(nolock)as Q on Q.QualificationId = S.TermQualificationId  
	inner join Clients C (nolock) on ss.ClientId = C.ClientId
    inner join suppliers (nolock) as su on su.Supplierid= S.Supplierid
	inner join Languages(nolock) as l on l.languageId = ss.LanguageId
	WHERE S.RespondentStatus = 14 and S.Survey_EndDate > '2025-01-01'
GROUP BY 
	  MONTH(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') ,
    CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) ,
    S.TermQualificationId,
    Q.QualificationName,C.ClientName,
	l.CountryCode,
	su.SupplierName




    select top 100 * from client_supplier_starts where Dates between '2026-01-12' and '2026-01-13';



    select top 10 * from vw_Survey_Hourly_DQ where only_date < '2025-10-01'

    select top 10 * from vw_Survey_Hourly_DQ order by only_date asc

    DROP VIEW IF EXISTS dbo.vw_Survey_Hourly_DQ;

-- CREATE VIEW dbo.vw_Survey_Hourly_DQ
AS
WITH SurveyData AS (
    SELECT  
        -- Only once, save the computed value for better performance
        CAST(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS DATE) AS only_date,
        DATEPART(hour, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS hours,
        S.RespondentStatus,
        S.Revenue,
        S.Cost,
        C.ClientName,
        su.SupplierName,
        l.CountryCode,
		S.UpdatedOn
    FROM APIClient_Surveys ss
    JOIN Survey_Info S ON ss.SurveyId = S.SurveyId
    JOIN Clients C ON ss.ClientId = C.ClientId
    JOIN Suppliers su ON su.SupplierId = S.SupplierId
    JOIN Languages l ON l.LanguageId = ss.LanguageId
)
SELECT
    only_date as Date,
	UpdatedOn,
    hours as Hour,
    COUNT_BIG(*) AS Starts,
    SUM(CASE WHEN RespondentStatus = 1 THEN 1 ELSE 0 END) AS Completes,
    SUM(CASE WHEN RespondentStatus = 1 THEN Revenue ELSE 0 END) AS Revenue,
    SUM(CASE WHEN RespondentStatus = 1 THEN Cost ELSE 0 END) AS Cost,
    ClientName as Client,
    SupplierName as Supplier,
    CountryCode As Country
FROM SurveyData
GROUP BY 
    only_date,
	UpdatedOn,
    hours,
    ClientName,
    SupplierName,
    CountryCode;

    select top 100 * from vw_supplier_client_report order by Dates DESC
    
    where Dates between '2025-01-01' and '2025-07-01'





   -- CREATE VIEW dbo.vw_supplier_client_report
AS
	WITH base_data AS (
    SELECT 
        ss.SurveyId,
        S.RespondentStatus,
        rs.Name AS RespondentStatusName,
        S.TermQualificationId,
        Q.QualificationName AS TerminateReason,
        S.Revenue,
        S.Cost,
        C.ClientName AS Client,
        su.SupplierName AS Supplier,
        l.CountryCode AS Country,
 
        -- Keep UpdatedAt as datetime (for incremental refresh)
        S.UpdatedON as updatedtime,
 
        -- Survey end datetime (EST, KEEP TIME)
        CAST(
            S.Survey_EndDate AT TIME ZONE 'UTC'
            AT TIME ZONE 'Eastern Standard Time'
            AS DATETIME2
        ) AS survey_end_dt,
		CAST(
            S.UpdatedON AT TIME ZONE 'UTC'
            AT TIME ZONE 'Eastern Standard Time'
            AS DATETIME2
        ) AS UpdatedON
 
    FROM Survey_Info S
    JOIN APIClient_Surveys ss ON ss.SurveyId = S.SurveyId
    JOIN Clients C ON ss.ClientId = C.ClientId
    JOIN Suppliers su ON su.SupplierId = S.SupplierId
    JOIN Languages l ON l.LanguageId = ss.LanguageId
    LEFT JOIN Qualifications Q ON Q.QualificationId = S.TermQualificationId
    LEFT JOIN RespondentStatus rs ON rs.RespondentStatusId = S.RespondentStatus
 )
SELECT
    -- Date
    survey_end_dt as Dates,
	UpdatedON,
 
    -- Hour number (0–23)
    DATEPART(HOUR, survey_end_dt) AS hour_no,
 
    -- Week & Month (display)
    'w' + CAST(DATEPART(ISO_WEEK, survey_end_dt) AS VARCHAR(2)) AS week_no,
    FORMAT(survey_end_dt, 'MMM yyyy') AS month_year,
    Client,
    Supplier,
    Country,
	RespondentStatus as Status,
    RespondentStatusName,
    TermQualificationId,
    TerminateReason,
 
    COUNT(*) AS starts,
 
    SUM(CASE WHEN RespondentStatus = 1 THEN 1 ELSE 0 END) AS completes,
ROUND(
    100.0 * SUM(CASE WHEN RespondentStatus = 1 THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
) AS Conversion,
 
 
    SUM(CASE WHEN RespondentStatus = 10 THEN 1 ELSE 0 END) AS Reconcile,
 
    ROUND(
    100.0 * SUM(CASE WHEN RespondentStatus = 10 THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
) AS ReconcilePer,
 
    SUM(CASE WHEN RespondentStatus = 1 THEN Revenue ELSE 0 END) AS Revenue,
    SUM(CASE WHEN RespondentStatus = 1 THEN Cost ELSE 0 END) AS Cost,
 
    ROUND(
        SUM(CASE WHEN RespondentStatus = 1 THEN Cost ELSE 0 END)
        / NULLIF(SUM(CASE WHEN RespondentStatus = 1 THEN 1 ELSE 0 END), 0),
        2
    ) AS CPC,
 
    ROUND(
        SUM(CASE WHEN RespondentStatus = 1 THEN Revenue ELSE 0 END)
        / NULLIF(SUM(CASE WHEN RespondentStatus = 1 THEN 1 ELSE 0 END), 0),
        2
    ) AS EPC,
 
    SUM(CASE WHEN RespondentStatus = 1 THEN Revenue ELSE 0 END)
    - SUM(CASE WHEN RespondentStatus = 1 THEN Cost ELSE 0 END) AS Profit,
 
    ROUND(
        1.0 * (
            SUM(CASE WHEN RespondentStatus = 1 THEN Revenue ELSE 0 END)
            - SUM(CASE WHEN RespondentStatus = 1 THEN Cost ELSE 0 END)
        )
        / NULLIF(SUM(CASE WHEN RespondentStatus = 1 THEN Revenue ELSE 0 END), 0),
        2
    ) AS Profit_per,
 
    SUM(CASE WHEN RespondentStatus = 14 THEN 1 ELSE 0 END) AS qualification_count
 
FROM base_data
GROUP BY
    CAST(survey_end_dt AS DATE),
    DATEPART(HOUR, survey_end_dt),
	survey_end_dt,
	UpdatedON,
    Client,
    Supplier,
    Country,
    RespondentStatus,
    RespondentStatusName,
    TermQualificationId,
    TerminateReason;



    SELECT
    ss.SurveyId,
    S.RespondentStatus,
    rs.Name AS RespondentStatusName,
    S.TermQualificationId,
    Q.QualificationName AS TerminateReason,
    S.Revenue,
    S.Cost,
    C.ClientName AS Client,
    su.SupplierName AS Supplier,
    l.CountryCode AS Country,
    -- Keep as UTC datetime for partitioning + indexing (critical!)
    S.UpdatedON AS updatedtime_utc,          -- Original UTC
    S.Survey_EndDate AS survey_end_dt_utc   -- Original UTC

FROM Survey_Info S
INNER JOIN APIClient_Surveys ss ON ss.SurveyId = S.SurveyId
INNER JOIN Clients C ON ss.ClientId = C.ClientId
INNER JOIN Suppliers su ON su.SupplierId = S.SupplierId
INNER JOIN Languages l ON l.LanguageId = ss.LanguageId
LEFT JOIN Qualifications Q ON Q.QualificationId = S.TermQualificationId
LEFT JOIN RespondentStatus rs ON rs.RespondentStatusId = S.RespondentStatus;