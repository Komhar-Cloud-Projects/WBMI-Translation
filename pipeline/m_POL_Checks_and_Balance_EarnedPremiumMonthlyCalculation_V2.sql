WITH
SQ_EarnedPremiumMonthlyCalculation AS (
	IF OBJECT_ID('tempdb.dbo.#EPM', 'U') IS NOT NULL
	drop table #EPM;
	IF OBJECT_ID('tempdb.dbo.#WEPM', 'U') IS NOT NULL
	drop table #WEPM;
	IF OBJECT_ID('tempdb.dbo.#WFAM', 'U') IS NOT NULL
	drop table #WFAM;
	IF OBJECT_ID('tempdb.dbo.#PM', 'U') IS NOT NULL
	drop table #PM;
	IF OBJECT_ID('tempdb.dbo.#EPMData', 'U') IS NOT NULL
	drop table #EPMData;
	IF OBJECT_ID('tempdb.dbo.#EPData', 'U') IS NOT NULL
	drop table #EPData;
	
	
	Declare @RunDate as datetime
	
	set @RunDate=dateadd(ss,-1,dateadd(MM,Datediff(MM,0,getdate())+@{pipeline().parameters.NO_OF_MONTHS},0));
	
	
	
	select
	A.PolicyKey,A.PolicyAKID,A.StatisticalCoverageAKID,A.RatingCoverageAKId,ProductCode,A.PremiumMasterCalculationPKID,PremiumTransactionExpirationDate,A.rundate,sum(A.EarnedPremium) EarnedPremium,sum(A.UnearnedPremium) UnearnedPremium,PremiumType,PremiumTransactionEffectiveDate
	into #EPM
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumMonthlyCalculation A
	where RunDate=@RunDate
	group by A.PolicyKey,A.PolicyAKID,A.StatisticalCoverageAKID,A.RatingCoverageAKId,ProductCode,A.PremiumMasterCalculationPKID,PremiumTransactionExpirationDate,A.rundate,PremiumType,PremiumTransactionEffectiveDate
	
	
	
	
	
	select WorkEarnedPremiumCoverageMonthlyID,StatisticalCoverageAkid,RatingCoverageAKId,MinimumPremium,PremiumType,RunDate,StatisticalCoverageCancellationDate, PolicyAKID 
	into #WEPM
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkEarnedPremiumCoverageMonthly B
	where RunDate=@RunDate
	
	
	
	
	
	select WorkFirstAuditId,PolicyKey,StatisticalCoverageAKID,PremiumMasterCalculationID,RatingCoverageAKId,PremiumMasterPremiumType,Rundate,PolicyAKID 
	into #WFAM
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkFirstAudit A
	where RunDate=@RunDate
	
	
	
	select PremiumMasterCalculationID,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterPremium,PremiumMasterPremiumType,PremiumMasterRundate, PolicyAKID
	into #PM
	 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation A
	where PremiumMasterRunDate<=@RunDate
	and exists (select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.EarnedPremiumMonthlyCalculation B where RunDate=@RunDate and A.PremiumMasterCalculationID=B.PremiumMasterCalculationPKID)
	
	
	select A.PolicyKey,A.PolicyAKID,A.StatisticalCoverageAKID,A.RatingCoverageAKId,A.ProductCode,A.PremiumMasterCalculationPKID,A.PremiumType,A.rundate,A.EarnedPremium,A.UnearnedPremium,
	C.PremiumMasterPremium,B.MinimumPremium,WorkFirstAuditId,WorkEarnedPremiumCoverageMonthlyID,A.PremiumTransactionEffectiveDate 
	into #EPMData
	from #EPM A
	left outer join #WEPM B
	on A.StatisticalCoverageAKID=B.StatisticalCoverageAKID
	and A.RatingCoverageAKId=B.RatingCoverageAKId
	and A.PolicyAKId=B.PolicyAKId
	and A.PremiumType=B.PremiumType
	and A.RunDate=B.RunDate
	and convert(varchar(6),B.StatisticalCoverageCancellationDate,112)<=convert(varchar(6),A.RunDate,112)
	left outer join #WFAM D
	on A.StatisticalCoverageAKID=D.StatisticalCoverageAKID
	and A.RatingCoverageAKId=D.RatingCoverageAKId
	and A.PolicyAKId=D.PolicyAKId
	and A.PremiumType=D.PremiumMasterPremiumType
	and A.RunDate=D.Rundate
	inner join #PM C
	on A.PremiumMasterCalculationPKID=C.PremiumMasterCalculationID
	and A.PremiumType=C.PremiumMasterPremiumType
	
	
	
	
	
	
	select A.PolicyKey,A.PolicyAKID,A.StatisticalCoverageAKID,A.RatingCoverageAKId,A.ProductCode,A.PremiumMasterCalculationPKID,A.PremiumType,A.rundate,A.EarnedPremium,A.UnearnedPremium,
	C.PremiumMasterPremium,A.PremiumTransactionExpirationDate
	into #EPData
	from #EPM A
	inner join #PM C
	on A.PremiumMasterCalculationPKID=C.PremiumMasterCalculationID
	and A.PremiumType=C.PremiumMasterPremiumType
	
	
	insert into WorkCheckandBalanceEarnedPremiumMonthlyCalculation
	select getdate(),getdate(),PolicyKey,-1,-1,-1, ProductCode,-1,'1800-01-01',Rundate,EarnedPremium,UnearnedPremium,'','1800-01-01',-1,-1,'1800-01-01',-1,-1,' ',PremiumMasterPremium,'1800-01-01',TypeOfData
	from 
	(
	
	--------------------Validation for WP-EP=UEP--------------------------------------------------------
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium, '1' TypeOfData
	from #EPMData
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumMasterPremium,ProductCode
	having abs(PremiumMasterPremium-sum(EarnedPremium)-sum(UnearnedPremium))>0.01
	
	UNION
	
	--------------------Validation for Cancellation subjected to Audit----------------------------------
	
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium,'2' TypeOfData 
	from #EPMData
	where WorkFirstAuditId is not null
	and PremiumTransactionEffectiveDate<=RunDate
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumMasterPremium,ProductCode
	having PremiumMasterPremium=sum(EarnedPremium) and sum(UnearnedPremium)<>0.0
	
	UNION
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium,'3' TypeOfData 
	from #EPMData
	where WorkFirstAuditId is not null
	and PremiumTransactionEffectiveDate<=RunDate
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumMasterPremium,ProductCode
	having PremiumMasterPremium<>sum(EarnedPremium)
	
	
	UNION
	-----------------------------------------Validation for regular cancellation----------------------------
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium, '4' TypeOfData 
	from #EPMData
	where WorkFirstAuditId is null
	and WorkEarnedPremiumCoverageMonthlyID is not null
	and MinimumPremium<>0.0
	and PremiumTransactionEffectiveDate<=RunDate
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumMasterPremium,ProductCode
	having PremiumMasterPremium=sum(EarnedPremium) and sum(UnearnedPremium)<>0.0
	
	UNION
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium, '5' TypeOfData 
	from #EPMData
	where WorkFirstAuditId is null
	and WorkEarnedPremiumCoverageMonthlyID is not null
	and MinimumPremium<>0.0
	and PremiumTransactionEffectiveDate<=RunDate
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumTransactionEffectiveDate,PremiumMasterPremium,ProductCode
	having PremiumMasterPremium<>sum(EarnedPremium)
	
	
	UNION
	
	---------------------------------Validation for regular expiration------------------------------------------
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium, '6' TypeOfData 
	from #EPData
	where convert(varchar(6),rundate,112)>=convert(varchar(6),PremiumTransactionExpirationDate,112)
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumMasterPremium,ProductCode
	having PremiumMasterPremium=sum(EarnedPremium) and sum(UnearnedPremium)<>0.0
	
	UNION
	
	
	select @RunDate as Rundate,PolicyKey,ProductCode,PremiumMasterPremium,sum(EarnedPremium) EarnedPremium, sum(UnearnedPremium) UnearnedPremium, '7' TypeOfData 
	from #EPData
	where convert(varchar(6),rundate,112)>=convert(varchar(6),PremiumTransactionExpirationDate,112)
	group by PolicyKey,StatisticalCoverageAKID,RatingCoverageAKId,PremiumMasterCalculationPKID,Rundate,PremiumMasterPremium,ProductCode
	having PremiumMasterPremium<>sum(EarnedPremium)
	
	) a
),
WorkCheckAndBalanceEarnedPremiumMonthlyCalculation1 AS (
	TRUNCATE TABLE WorkCheckAndBalanceEarnedPremiumMonthlyCalculation;
	INSERT INTO WorkCheckAndBalanceEarnedPremiumMonthlyCalculation
	(PolicyKey)
	SELECT 
	POLICYKEY
	FROM SQ_EarnedPremiumMonthlyCalculation
),
SQ_WorkCheckAndBalanceEarnedPremiumMonthlyCalculation AS (
	select rundate, policykey, productcode, 
	 CONVERT(money,PremiumMasterPremium,0) as PremiumMasterPremium,
	 CONVERT(money,EarnedPremium,0) as EarnedPremium,
	  CONVERT(money,UnearnedPremium,0) as UnearnedPremium,
	  TypeofData from 
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkCheckandBalanceEarnedPremiumMonthlyCalculation
	order by 2,4
),
XP_SRC_DataCollect AS (
	SELECT
	RunDate,
	PolicyKey AS pol_key,
	ProductCode,
	PremiumMasterPremium AS PremiumTransactionAmount,
	EarnedPremium,
	UnearnedPremium,
	TypeOfData AS I_Message,
	-- *INF*: DECODE(I_Message,
	-- '1','Sum of Earned Premium and UnEarned Premium is not equal to Written Premium',
	-- '2','Policy Life cycle end event: Cancellation subjected to audit: Earned Premium Eaquals Written but Unearned<>0',
	-- '3','Policy Life cycle end event: Cancellation subjected to audit: Earned Premium <> Written',
	-- '4','Policy Life cycle end event: Regular Cancellation: Earned Premium = Written but unearned premium<>0',
	-- '5','Policy Life cycle end event: Regular Cancellation: Earned Premium <> Written' ,
	-- '6','Policy Life cycle end event: Regular Expiration: Earned Premium = Written but unearned premium<>0',
	-- '7','Policy Life cycle end event: Regular Expiration: Earned Premium <> Written'
	-- )
	DECODE(I_Message,
		'1', 'Sum of Earned Premium and UnEarned Premium is not equal to Written Premium',
		'2', 'Policy Life cycle end event: Cancellation subjected to audit: Earned Premium Eaquals Written but Unearned<>0',
		'3', 'Policy Life cycle end event: Cancellation subjected to audit: Earned Premium <> Written',
		'4', 'Policy Life cycle end event: Regular Cancellation: Earned Premium = Written but unearned premium<>0',
		'5', 'Policy Life cycle end event: Regular Cancellation: Earned Premium <> Written',
		'6', 'Policy Life cycle end event: Regular Expiration: Earned Premium = Written but unearned premium<>0',
		'7', 'Policy Life cycle end event: Regular Expiration: Earned Premium <> Written') AS V_Message,
	V_Message AS O_Message
	FROM SQ_WorkCheckAndBalanceEarnedPremiumMonthlyCalculation
),
LKP_Product AS (
	SELECT
	ProductDescription,
	IN_ProductCode,
	ProductCode
	FROM (
		SELECT 
			ProductDescription,
			IN_ProductCode,
			ProductCode
		FROM Product
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode ORDER BY ProductDescription DESC) = 1
),
XP_GetLKP_Values AS (
	SELECT
	XP_SRC_DataCollect.pol_key,
	LKP_Product.ProductDescription,
	-- *INF*: IIF(ISNULL(ProductDescription),'',ProductDescription)
	IFF(ProductDescription IS NULL, '', ProductDescription) AS v_ProductDescription,
	XP_SRC_DataCollect.PremiumTransactionAmount,
	XP_SRC_DataCollect.EarnedPremium,
	XP_SRC_DataCollect.UnearnedPremium,
	XP_SRC_DataCollect.O_Message AS Message,
	XP_SRC_DataCollect.RunDate
	FROM XP_SRC_DataCollect
	LEFT JOIN LKP_Product
	ON LKP_Product.ProductCode = XP_SRC_DataCollect.ProductCode
),
AGG_Detail AS (
	SELECT
	pol_key,
	ProductDescription,
	PremiumTransactionAmount,
	-- *INF*: TO_CHAR(SUM(PremiumTransactionAmount))
	TO_CHAR(SUM(PremiumTransactionAmount)) AS O_PremiumTransactionAmount,
	EarnedPremium,
	-- *INF*: TO_CHAR(SUM(EarnedPremium))
	TO_CHAR(SUM(EarnedPremium)) AS O_EarnedPremium,
	UnearnedPremium,
	-- *INF*: TO_CHAR(SUM(UnearnedPremium))
	TO_CHAR(SUM(UnearnedPremium)) AS O_UnearnedPremium,
	-- *INF*: TO_CHAR(SUM(PremiumTransactionAmount)-sum(EarnedPremium))
	TO_CHAR(SUM(PremiumTransactionAmount) - sum(EarnedPremium)) AS DIFF,
	Message,
	RunDate
	FROM XP_GetLKP_Values
	GROUP BY pol_key, ProductDescription, Message
),
EXP_Format_Email_Message AS (
	SELECT
	pol_key,
	Message,
	ProductDescription AS i_ProductDescription,
	O_PremiumTransactionAmount AS i_PremiumTransactionAmount,
	O_EarnedPremium AS i_EarnedPremium,
	O_UnearnedPremium AS i_UnearnedPremium,
	DIFF,
	RunDate AS i_RunDate,
	'Policy: '
 || pol_key
 || ' Product Description: '
 || i_ProductDescription
 || ' Written Premium: '
 || i_PremiumTransactionAmount
 || ' Earned Premium: '
 || i_EarnedPremium
 || ' Difference: '
 || DIFF	
 || ' UnEarned Premium: '
 || i_UnearnedPremium AS v_policy_info,
	v_row_count AS v_row_count_temp,
	@{pipeline().parameters.EMAIL_ADDRESS} AS email_address,
	-- *INF*: TO_CHAR(i_RunDate) || '  ' || @{pipeline().parameters.EMAIL_SUBJECT}
	TO_CHAR(i_RunDate) || '  ' || @{pipeline().parameters.EMAIL_SUBJECT} AS email_subject,
	v_row_count_temp + 1 AS v_row_count,
	-- *INF*: '</table>' ||  CHR(10) ||'<br></br>' || CHR(10) ||   '<table width="100%" style="border-collapse: collapse" bordercolor="#111111" cellpadding="3" cellspacing="0">' || CHR(10) ||
	-- '	<tr><td width="50"><b><font face="Arial" size="5">Details</font></b></td></tr>' || CHR(10) 
	'</table>' || CHR(10) || '<br></br>' || CHR(10) || '<table width="100%" style="border-collapse: collapse" bordercolor="#111111" cellpadding="3" cellspacing="0">' || CHR(10) || '	<tr><td width="50"><b><font face="Arial" size="5">Details</font></b></td></tr>' || CHR(10) AS v_email_body_header,
	-- *INF*: '	<tr><td width="100"><font face="Arial" size="2">' || Message || ':' || '</font></td>' ||CHR(10)
	--  || '<td width="200"><font face="Arial" size="2">' || v_policy_info || '</font></td></tr>' || CHR(10)
	'	<tr><td width="100"><font face="Arial" size="2">' || Message || ':' || '</font></td>' || CHR(10) || '<td width="200"><font face="Arial" size="2">' || v_policy_info || '</font></td></tr>' || CHR(10) AS v_email_body_content,
	-- *INF*: IIF(v_row_count = 1,
	-- 	v_email_body_header || CHR(10) || v_email_body_content,
	-- 	v_email_body_content)
	IFF(v_row_count = 1, v_email_body_header || CHR(10) || v_email_body_content, v_email_body_content) AS v_email_body,
	v_email_body AS out_email_body,
	-- *INF*: IIF(v_row_count = 1, 'C','D')
	IFF(v_row_count = 1, 'C', 'D') AS sort_indicator
	FROM AGG_Detail
),
AGG_Distinct_Email_Address_Subject AS (
	SELECT
	email_address,
	email_subject
	FROM EXP_Format_Email_Message
	QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address, email_subject ORDER BY NULL) = 1
),
email_subject AS (
	INSERT INTO email_subject
	(FIELD1)
	SELECT 
	email_subject AS FIELD1
	FROM AGG_Distinct_Email_Address_Subject
),
SRT_ErrorMessage AS (
	SELECT
	Message, 
	pol_key
	FROM XP_GetLKP_Values
	ORDER BY Message ASC, pol_key ASC
),
AGG_PolicyCount AS (
	SELECT
	pol_key,
	Message,
	-- *INF*: TO_CHAR(count(pol_key))
	TO_CHAR(count(pol_key)) AS PolicyCount
	FROM SRT_ErrorMessage
	GROUP BY Message
),
EXP_Format_Email_Message1 AS (
	SELECT
	Message,
	PolicyCount,
	v_row_count AS v_row_count_temp,
	v_row_count_temp + 1 AS v_row_count,
	-- *INF*: '<table width="100%" style="border-collapse: collapse" bordercolor="#111111" cellpadding="3" cellspacing="0">' || CHR(10) ||
	-- '	<tr><td width="50"><b><font face="Arial" size="5">Summary</font></b></td></tr>' || CHR(10)
	'<table width="100%" style="border-collapse: collapse" bordercolor="#111111" cellpadding="3" cellspacing="0">' || CHR(10) || '	<tr><td width="50"><b><font face="Arial" size="5">Summary</font></b></td></tr>' || CHR(10) AS v_email_body_header,
	-- *INF*: '	<tr><td width="150"><font face="Arial" size="2">' || Message||':     ' ||   'Count of Policies: '||PolicyCount || '</font></td></tr>' || CHR(10)
	'	<tr><td width="150"><font face="Arial" size="2">' || Message || ':     ' || 'Count of Policies: ' || PolicyCount || '</font></td></tr>' || CHR(10) AS v_email_body_content,
	-- *INF*: IIF(v_row_count = 1,
	-- 	v_email_body_header || CHR(10) || v_email_body_content,
	-- 	v_email_body_content)
	IFF(v_row_count = 1, v_email_body_header || CHR(10) || v_email_body_content, v_email_body_content) AS v_email_body,
	v_email_body AS out_email_body,
	-- *INF*: IIF(v_row_count = 1,'A','B')
	IFF(v_row_count = 1, 'A', 'B') AS sort_indicator
	FROM AGG_PolicyCount
),
Union AS (
	SELECT out_email_body, sort_indicator
	FROM EXP_Format_Email_Message1
	UNION
	SELECT out_email_body, sort_indicator
	FROM EXP_Format_Email_Message
),
SRT_POlicyCount_Details AS (
	SELECT
	out_email_body, 
	sort_indicator
	FROM Union
	ORDER BY sort_indicator ASC
),
email_body AS (
	INSERT INTO email_body
	(FIELD1)
	SELECT 
	out_email_body AS FIELD1
	FROM SRT_POlicyCount_Details
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	email_address AS FIELD1
	FROM AGG_Distinct_Email_Address_Subject
),