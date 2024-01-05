WITH
SQ_WBPolicyStaging AS (
	SELECT  distinct P.PolicyNumber , WP.PolicyVersionFormatted as pol_version
	FROM dbo.WBPolicyStaging WP
	INNER JOIN  dbo.DCPolicyStaging P ON WP.sessionid = P.sessionid
	UNION
	SELECT distinct P.PolicyNumber, P.PolicyVersion as pol_version
	FROM dbo.WorkDCTPLPolicy P
),
EXP_POLKEY AS (
	SELECT
	pol_version,
	PolicyNumber,
	-- *INF*: CONCAT(PolicyNumber,pol_version)
	CONCAT(PolicyNumber, pol_version) AS Pol_key
	FROM SQ_WBPolicyStaging
),
SQ_policy_dim1 AS (
	SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(df.DividendPaidAmount) as TotalDividendChange
	       FROM dbo.DividendFact DF
	       INNER JOIN dbo.Policy_Dim POL
	  on POL.pol_dim_id = DF.PolicyDimId
	  WHERE POL.pol_sym = '000'
	       GROUP BY
	              POL.Pol_Num,
	              POL.Pol_Key
),
JNR_Stage_DM AS (SELECT
	EXP_POLKEY.Pol_key, 
	SQ_policy_dim1.SourceReference, 
	SQ_policy_dim1.SourceUID, 
	SQ_policy_dim1.TotalDividendChange AS TotalPremiumChange
	FROM SQ_policy_dim1
	INNER JOIN EXP_POLKEY
	ON EXP_POLKEY.Pol_key = SQ_policy_dim1.SourceUID
),
AGG_SR_SUID AS (
	SELECT
	SourceReference, 
	SourceUID, 
	TotalPremiumChange, 
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
	--    '"TypeCode":"DMDIV",' ||
	--    '"SourceReference":"'|| SourceReference || '",' ||
	--    '"SourceTypeCode":"POLKY",'  || 
	--    '"SourceUID":"'|| SourceUID ||'",' ||  
	--    '"TransactionType":"Dividends",'  || 
	--    '"TransactionTotal":' || TO_CHAR(TotalPremiumChange) ||
	-- '}'
	'{' || '"SourceSystemCode":"DWMRT",' || '"ComponentName":"PolicyMart",' || '"TypeCode":"DMDIV",' || '"SourceReference":"' || SourceReference || '",' || '"SourceTypeCode":"POLKY",' || '"SourceUID":"' || SourceUID || '",' || '"TransactionType":"Dividends",' || '"TransactionTotal":' || TO_CHAR(TotalPremiumChange) || '}' AS v_JSON_row,
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