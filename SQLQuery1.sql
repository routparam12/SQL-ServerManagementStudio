SELECT  top 100
    CONVERT(date, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS only_date,
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
WHERE S.Survey_EndDate >= '2025-07-01'
GROUP BY 
    CONVERT(date, S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'),
    C.ClientName,
    su.SupplierName,
    l.CountryCode


select top 100 * from Survey_Info S


SELECT top 100
    S.TermQualificationId,
    Q.QualificationName,
    COUNT(*) AS total_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS count_percentage,
	C.ClientName,
	l.CountryCode,
	su.SupplierName as SupplierName,
	  MONTH(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS month,
    DAY(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS day_only,	 
    CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) AS only_date,
	DATEPART(HOUR, (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')) AS hours
from  APIClient_Surveys(nolock) as ss
	inner join Survey_Info S (nolock) on ss.SurveyId= S.SurveyId 
	left join Qualifications(nolock)as Q on Q.QualificationId = S.TermQualificationId  
	inner join Clients C (nolock) on ss.ClientId = C.ClientId
    inner join suppliers (nolock) as su on su.Supplierid= S.Supplierid
	inner join Languages(nolock) as l on l.languageId = ss.LanguageId
	WHERE S.RespondentStatus = 14 and S.Survey_EndDate > '2025-09-01'
GROUP BY 
	  MONTH(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') ,
    DAY(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') ,	 
    CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) ,
	DATEPART(HOUR, (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')),
    S.TermQualificationId,
    Q.QualificationName,C.ClientName,
	l.CountryCode,
	su.SupplierName 
	order by total_count desc


select top 100
    ss.AccountId,    
	S.supplierUserId,
    S.SurveyRespondentId,
	ss.SurveyId,
	isnull(ss.BuyerId,0) as BuyerId,
	isnull(ss.BuyerName, b.BuyerName) AS BuyerName,
	l.LanguageId,
	l.CountryCode,
	C.ClientId, 
	C.ClientName,
    C.clienttypeid, 
	su.SupplierId as SupplierId ,
	su.SupplierName as SupplierName,
	S.Survey_Date as Survey_StartDate,
	S.Survey_EndDate,
    DATEADD(hh, -5, S.Survey_Date) as Survey_StartDate_EST,
    DATEADD(hh, -5, S.Survey_EndDate) as Survey_EndDate_EST,
    S.Cost  as Cost,
	S.Revenue  as Revenue,
	rs.RespondentStatusId,
    S.TermQualificationId,
    Q.QualificationName,
    S.Survey_IP as IPAddress,
    S.UserID,
	S.isrouter,
    S.routerlogic,
    isnull(S.subsource, '0') AS subsource,
    isnull(assc.sourcename, 'null') AS sourcename,
	rs.Name as RespondentStatus,
	ors.RespondentStatusId  as OrginalRespondentStatusId,
	ors.name  as OrginalRespondentStatus
	from  APIClient_Surveys(nolock) as ss
	inner join Survey_Info S (nolock) on ss.SurveyId= S.SurveyId 
    inner join Clients C (nolock) on ss.ClientId = C.ClientId
    inner join suppliers (nolock) as su on su.Supplierid= S.Supplierid
    inner join RespondentStatus (nolock) as rs on rs.RespondentStatusId = S.RespondentStatus
    inner join RespondentStatus (nolock) as ors on ors.RespondentStatusId = S.OriginalStatus
    inner join Languages(nolock) as l on l.languageId = ss.LanguageId
    left join APIClient_BuyerConfig(nolock) as b on b.ClientId = ss.ClientId and b.BuyerId = ss.buyerId
    left join Qualifications(nolock)as Q on Q.QualificationId = S.TermQualificationId
	left join APISupplier_SourceConfig(nolock) as assc on  assc.supplierid = s.Supplierid and assc.sourceid = s.subsource
    where S.Survey_EndDate between  '2025-09-01' and '2025-11-01'


    select top 100 ss.SurveyId,
            rs.Name as RespondentStatus,
            rs.RespondentStatusId,
            C.ClientName,
	        l.CountryCode,
	        su.SupplierName as SupplierName,
            CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) AS only_date
    from APIClient_Surveys(nolock) as ss 
            inner join Survey_Info S (nolock) on ss.SurveyId= S.SurveyId 
            inner join RespondentStatus (nolock) as rs on rs.RespondentStatusId = S.RespondentStatus
            inner join Clients C (nolock) on ss.ClientId = C.ClientId
            inner join suppliers (nolock) as su on su.Supplierid= S.Supplierid
	        inner join Languages(nolock) as l on l.languageId = ss.LanguageId
            WHERE S.Survey_EndDate >= '2025-07-01'
            group by CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) ,
            	C.ClientName,
	            l.CountryCode,
	            su.SupplierName,
                ss.surveyId,
                rs.Name,
                rs.RespondentStatusId



                SELECT DISTINCT TOP (100)
    ss.SurveyId,
    rs.Name AS RespondentStatus,
    rs.RespondentStatusId,
    C.ClientName,
    l.CountryCode,
    su.SupplierName,
    dt.only_date
FROM APIClient_Surveys ss WITH (NOLOCK)
JOIN Survey_Info S WITH (NOLOCK)
    ON ss.SurveyId = S.SurveyId
JOIN RespondentStatus rs WITH (NOLOCK)
    ON rs.RespondentStatusId = S.RespondentStatus
JOIN Clients C WITH (NOLOCK)
    ON ss.ClientId = C.ClientId
JOIN Suppliers su WITH (NOLOCK)
    ON su.SupplierId = S.SupplierId
JOIN Languages l WITH (NOLOCK)
    ON l.LanguageId = ss.LanguageId
CROSS APPLY (
    SELECT CAST(
        S.Survey_EndDate AT TIME ZONE 'UTC'
        AT TIME ZONE 'Eastern Standard Time'
        AS DATE
    ) AS only_date
) dt
WHERE S.Survey_EndDate >= '2025-07-01';



            select top 100 * from APIClient_Surveys ss

            select top 100 * from RespondentStatus rs

            select top 100 * from Survey_Info s




SELECT  top 100
    S.TermQualificationId,
    Q.QualificationName,
    COUNT(*) AS total_count,
	C.ClientName,
	l.CountryCode,
	su.SupplierName as SupplierName,
    rs.Name as RespondentStatus,rs.RespondentStatusId,
	  MONTH(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS month,
    CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) AS only_date
from  APIClient_Surveys(nolock) as ss
	inner join Survey_Info S (nolock) on ss.SurveyId= S.SurveyId 
    inner join RespondentStatus (nolock) as rs on rs.RespondentStatusId = S.RespondentStatus 
	left join Qualifications(nolock)as Q on Q.QualificationId = S.TermQualificationId  
	inner join Clients C (nolock) on ss.ClientId = C.ClientId
    inner join suppliers (nolock) as su on su.Supplierid= S.Supplierid
	inner join Languages(nolock) as l on l.languageId = ss.LanguageId
	WHERE S.RespondentStatus = 14 and S.Survey_EndDate >= '2025-07-01'
GROUP BY 
	  MONTH(S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') ,
    CAST( (S.Survey_EndDate AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS DATE ) ,
    S.TermQualificationId,
    Q.QualificationName,C.ClientName,
	l.CountryCode,
	su.SupplierName,
    rs.Name,
    rs.RespondentStatusId




