WITH
SQ_WBPolicyStaging AS (
	SELECT  distinct P.PolicyNumber , WP.PolicyVersionFormatted as pol_version  
	FROM wc_stage_archive.dbo.ArchWBPolicyStaging WP
	INNER JOIN  wc_stage_archive.dbo.archDCPolicyStaging P ON WP.sessionid = P.sessionid
	 Where
	p.ExtractDate >=@{pipeline().parameters.STARTDATE}
	UNION
	
	SELECT  
	distinct P.PolicyNumber, P.PolicyVersion as pol_version 
	FROM wc_stage_archive.dbo.ArchWorkDCTPLPolicy P
	Where
	p.ExtractDate >=@{pipeline().parameters.STARTDATE}
),
EXP_POLKEY AS (
	SELECT
	pol_version,
	PolicyNumber,
	-- *INF*: CONCAT(PolicyNumber,pol_version)
	CONCAT(PolicyNumber, pol_version) AS Pol_key
	FROM SQ_WBPolicyStaging
),
SQ_policy_dim AS (
	DECLARE @DATE datetime
	SET @DATE = DATEADD(mm,DATEDIFF(m,0,GETDATE()),0)
	
	       SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(PTF.DirectWrittenPremium) as TotalPremiumChange
	       FROM dbo.PremiumTransactionfact PTF
	       INNER JOIN dbo.Policy_Dim POL
	         on POL.pol_dim_id = PTF.PolicyDimId
	  INNER JOIN dbo.calendar_dim CD on CD.clndr_id = PTF.PremiumTransactionBookedDateID
	       WHERE CD.clndr_date >= @DATE  ---- transactions with booked of current month and future months
	  AND POL.pol_sym = '000'
	  GROUP BY POL.Pol_Num, POL.Pol_Key
),
SQ_policy_dim1 AS (
	DECLARE @DATE datetime
	SET @DATE = DATEADD(mm,DATEDIFF(m,0,GETDATE()),0)
	         SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(PMF.PremiumMasterDirectWrittenPremium) as TotalPremiumChange
	       FROM dbo.PremiumMasterFact PMF
	       INNER JOIN dbo.Policy_Dim POL
	  on POL.pol_dim_id = PMF.PolicyDimId
	  WHERE POL.pol_sym = '000'
	       GROUP BY
	              POL.Pol_Num,
	              POL.Pol_Key
),
SQ_policy_dim2 AS (
	DECLARE @DATE datetime
	SET @DATE = DATEADD(mm,DATEDIFF(m,0,GETDATE()),0)
	       SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(PTCT.PassThroughChargeTransactionAmount) as TotalPremiumChange
	       FROM dbo.PassThroughChargeTransactionfact PTCT
	       INNER JOIN dbo.Policy_Dim POL
	         on POL.pol_dim_id = PTCT.PolicyDimId
	 WHERE POL.pol_sym = '000'
	       GROUP BY     POL.Pol_Num,
	              POL.Pol_Key
),
UN_all AS (
	SELECT SourceReference, SourceUID, TotalPremiumChange
	FROM SQ_policy_dim
	UNION
	SELECT SourceReference, SourceUID, TotalPremiumChange
	FROM SQ_policy_dim1
	UNION
	SELECT SourceReference, SourceUID, TotalPremiumChange
	FROM SQ_policy_dim2
),
JNR_Stage_DM AS (SELECT
	EXP_POLKEY.Pol_key, 
	UN_all.SourceReference, 
	UN_all.SourceUID, 
	UN_all.TotalPremiumChange
	FROM UN_all
	INNER JOIN EXP_POLKEY
	ON EXP_POLKEY.Pol_key = UN_all.SourceUID
),
AGG_SR_SUID AS (
	SELECT
	SourceReference,
	SourceUID,
	TotalPremiumChange,
	-- *INF*: SUM(TotalPremiumChange)
	SUM(TotalPremiumChange) AS TotalPremiumChangeOut
	FROM JNR_Stage_DM
	GROUP BY SourceReference, SourceUID
),
EXP_Values AS (
	SELECT
	SourceReference,
	SourceUID,
	TotalPremiumChangeOut AS TotalPremiumChange,
	-- *INF*: '{' ||
	--    '"SourceSystemCode":"DWMRT",' ||  
	--    '"ComponentName":"PolicyMart",' ||
	--    '"TypeCode":"EXBAL",' ||
	--    '"SourceReference":"'|| SourceReference || '",' ||
	--    '"SourceTypeCode":"POLKY",'  || 
	--    '"SourceUID":"'|| SourceUID ||'",' ||  
	--    '"TransactionType":"All Transactions",'  || 
	--    '"TransactionTotal":' || TO_CHAR(TotalPremiumChange) ||
	-- '}'
	'{' || '"SourceSystemCode":"DWMRT",' || '"ComponentName":"PolicyMart",' || '"TypeCode":"EXBAL",' || '"SourceReference":"' || SourceReference || '",' || '"SourceTypeCode":"POLKY",' || '"SourceUID":"' || SourceUID || '",' || '"TransactionType":"All Transactions",' || '"TransactionTotal":' || TO_CHAR(TotalPremiumChange) || '}' AS v_JSON_row,
	v_JSON_row AS Json_RowData,
	@{pipeline().parameters.TARGETFILE} AS Filename
	FROM AGG_SR_SUID
),
TransactionalData_JSONFile AS (
	INSERT INTO JSONFile
	(JSONMessage, FileName)
	SELECT 
	Json_RowData AS JSONMESSAGE, 
	Filename AS FILENAME
	FROM EXP_Values
),