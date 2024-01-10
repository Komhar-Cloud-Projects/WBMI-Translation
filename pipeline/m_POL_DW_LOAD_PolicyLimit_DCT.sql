WITH
LKP_DCLimitStaging AS (
	SELECT
	Value,
	SessionId,
	ObjectId,
	Type
	FROM (
		SELECT DLT.Value as Value, 
		DLT.SessionId as SessionId, 
		DLT.ObjectId as ObjectId, 
		DLT.Type as Type 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLimitStaging DLT
		WHERE DLT.ObjectName='DC_Line'
		
		union all
		SELECT DLT.Value as Value, 
		DLT.SessionId as SessionId, 
		CUL.LineId as ObjectId, 
		DLT.Type as Type 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLimitStaging DLT
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCULineStaging WCUL
		on DLT.ObjectId=WCUL.WB_CU_LineId
		join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCULineStaging CUL
		on WCUL.CU_LineId=CUL.CU_LineId
		WHERE DLT.ObjectName='WB_CU_Line'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,ObjectId,Type ORDER BY Value DESC) = 1
),
SQ_PolicyLimit AS (
	SELECT
	WorkDCTPolicy.SessionId, 
	WorkDCTPolicy.PolicyNumber, 
	WorkDCTPolicy.PolicyVersion, 
	WorkDCTPolicy.TransactionCreatedDate,
	WorkDCTInsuranceLine.LineType,
	WorkDCTInsuranceLine.LineId
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy
	inner hash join
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTInsuranceLine
	on 
	WorkDCTPolicy.PolicyId=WorkDCTInsuranceLine.PolicyId
	where WorkDCTPolicy.PolicyStatus<>'Quote' 
	and WorkDCTPolicy.TransactionState='committed'
	and WorkDCTPolicy.TransactionType @{pipeline().parameters.EXCLUDE_TTYPE}
	ORDER BY WorkDCTPolicy.PolicyNumber, 
	ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),WorkDCTPolicy.PolicyVersion),2),'00'),
	WorkDCTInsuranceLine.LineType,
	WorkDCTPolicy.TransactionCreatedDate, 
	WorkDCTPolicy.SessionId
),
EXP_Values AS (
	SELECT
	SessionId AS i_SessionId,
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	LineType AS i_LineType,
	LineId AS i_LineId,
	TransactionCreatedDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_PolicyNumber) AS v_PolicyNumber,
	-- *INF*: IIF(ISNULL(i_PolicyVersion),'00',LPAD(LTRIM(RTRIM(i_PolicyVersion)),2,'0'))
	IFF(i_PolicyVersion IS NULL, '00', LPAD(LTRIM(RTRIM(i_PolicyVersion)), 2, '0')) AS v_PolicyVersion,
	-- *INF*: IIF(ISNULL(i_LineType) or IS_SPACES(i_LineType) or LENGTH(i_LineType)=0,'N/A',LTRIM(RTRIM(i_LineType)))
	IFF(i_LineType IS NULL OR IS_SPACES(i_LineType) OR LENGTH(i_LineType) = 0, 'N/A', LTRIM(RTRIM(i_LineType))) AS v_InsuranceLine,
	-- *INF*: DECODE(TRUE,
	-- IN(lower(v_InsuranceLine),'generalliability','sbopgeneralliability'), :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'PolicyPerOccurenceLimit'),
	-- lower(v_InsuranceLine)='commercialumbrella',
	-- IIF(NOT ISNULL(:LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'UmbrellaLimit')),:LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'UmbrellaLimit'),:LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'HigherLimit')),
	-- lower(v_InsuranceLine)='businessowners',:LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'Liability'),
	-- 'N/A')
	DECODE(TRUE,
		IN(lower(v_InsuranceLine), 'generalliability', 'sbopgeneralliability'), LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyPerOccurenceLimit.Value,
		lower(v_InsuranceLine) = 'commercialumbrella', IFF(NOT LKP_DCLIMITSTAGING_i_SessionId_i_LineId_UmbrellaLimit.Value IS NULL, LKP_DCLIMITSTAGING_i_SessionId_i_LineId_UmbrellaLimit.Value, LKP_DCLIMITSTAGING_i_SessionId_i_LineId_HigherLimit.Value),
		lower(v_InsuranceLine) = 'businessowners', LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Liability.Value,
		'N/A') AS v_PolicyPerOccurenceLimit,
	-- *INF*: DECODE(lower(v_InsuranceLine),
	-- 'generalliability', 
	-- :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'PolicyAggregateLimit'), 
	-- 'sbopgeneralliability', 
	-- :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'PolicyAggregateLimit'), 
	-- 'workerscompensation',
	-- :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'Policy'), 
	-- 'N/A')
	DECODE(lower(v_InsuranceLine),
		'generalliability', LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyAggregateLimit.Value,
		'sbopgeneralliability', LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyAggregateLimit.Value,
		'workerscompensation', LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Policy.Value,
		'N/A') AS v_PolicyAggregateLimit,
	-- *INF*: IIF(in(lower(v_InsuranceLine),'generalliability','sbopgeneralliability'), :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'ProductsAggregateLimit'), 'N/A')
	IFF(in(lower(v_InsuranceLine), 'generalliability', 'sbopgeneralliability'), LKP_DCLIMITSTAGING_i_SessionId_i_LineId_ProductsAggregateLimit.Value, 'N/A') AS v_PolicyProductAggregateLimit,
	-- *INF*: IIF(lower(v_InsuranceLine)='workerscompensation', :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'EachAccident'), 'N/A')
	IFF(lower(v_InsuranceLine) = 'workerscompensation', LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachAccident.Value, 'N/A') AS v_PolicyPerAccidentLimit,
	-- *INF*: IIF(lower(v_InsuranceLine)='workerscompensation', :LKP.LKP_DCLIMITSTAGING(i_SessionId, i_LineId, 'EachEmployeeDisease'), 'N/A')
	IFF(lower(v_InsuranceLine) = 'workerscompensation', LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachEmployeeDisease.Value, 'N/A') AS v_PolicyPerDiseaseLimit,
	v_PolicyNumber || v_PolicyVersion AS o_PolicyKey,
	v_InsuranceLine AS o_InsuranceLine,
	-- *INF*: IIF(ISNULL(v_PolicyPerOccurenceLimit) OR IS_SPACES(v_PolicyPerOccurenceLimit) OR LENGTH(v_PolicyPerOccurenceLimit)=0, 'N/A', LTRIM(RTRIM(v_PolicyPerOccurenceLimit)))
	IFF(v_PolicyPerOccurenceLimit IS NULL OR IS_SPACES(v_PolicyPerOccurenceLimit) OR LENGTH(v_PolicyPerOccurenceLimit) = 0, 'N/A', LTRIM(RTRIM(v_PolicyPerOccurenceLimit))) AS o_PolicyPerOccurenceLimit,
	-- *INF*: IIF(ISNULL(v_PolicyAggregateLimit) OR IS_SPACES(v_PolicyAggregateLimit) OR LENGTH(v_PolicyAggregateLimit)=0, 'N/A', LTRIM(RTRIM(v_PolicyAggregateLimit)))
	IFF(v_PolicyAggregateLimit IS NULL OR IS_SPACES(v_PolicyAggregateLimit) OR LENGTH(v_PolicyAggregateLimit) = 0, 'N/A', LTRIM(RTRIM(v_PolicyAggregateLimit))) AS v_PolicyAggregateLimit_new,
	-- *INF*: DECODE(lower(v_InsuranceLine),
	-- 'generalliability', v_PolicyAggregateLimit_new,
	-- 'sbopgeneralliability', v_PolicyAggregateLimit_new,
	-- 'workerscompensation',v_PolicyAggregateLimit_new || '000',
	-- 'N/A')
	DECODE(lower(v_InsuranceLine),
		'generalliability', v_PolicyAggregateLimit_new,
		'sbopgeneralliability', v_PolicyAggregateLimit_new,
		'workerscompensation', v_PolicyAggregateLimit_new || '000',
		'N/A') AS o_PolicyAggregateLimit,
	-- *INF*: IIF(ISNULL(v_PolicyProductAggregateLimit) OR IS_SPACES(v_PolicyProductAggregateLimit) OR LENGTH(v_PolicyProductAggregateLimit)=0, 'N/A', LTRIM(RTRIM(v_PolicyProductAggregateLimit)))
	IFF(v_PolicyProductAggregateLimit IS NULL OR IS_SPACES(v_PolicyProductAggregateLimit) OR LENGTH(v_PolicyProductAggregateLimit) = 0, 'N/A', LTRIM(RTRIM(v_PolicyProductAggregateLimit))) AS o_PolicyProductAggregateLimit,
	-- *INF*: IIF(ISNULL(v_PolicyPerAccidentLimit) OR IS_SPACES(v_PolicyPerAccidentLimit) OR LENGTH(v_PolicyPerAccidentLimit)=0, 'N/A', LTRIM(RTRIM(v_PolicyPerAccidentLimit)))
	IFF(v_PolicyPerAccidentLimit IS NULL OR IS_SPACES(v_PolicyPerAccidentLimit) OR LENGTH(v_PolicyPerAccidentLimit) = 0, 'N/A', LTRIM(RTRIM(v_PolicyPerAccidentLimit))) AS v_PolicyPerAccidentLimit_new,
	-- *INF*: DECODE(lower(v_InsuranceLine),
	-- 'generalliability', v_PolicyPerAccidentLimit_new,
	-- 'sbopgeneralliability', v_PolicyPerAccidentLimit_new,
	-- 'workerscompensation', v_PolicyPerAccidentLimit_new|| '000',
	-- 'N/A')
	DECODE(lower(v_InsuranceLine),
		'generalliability', v_PolicyPerAccidentLimit_new,
		'sbopgeneralliability', v_PolicyPerAccidentLimit_new,
		'workerscompensation', v_PolicyPerAccidentLimit_new || '000',
		'N/A') AS o_PolicyPerAccidentLimit,
	-- *INF*: IIF(ISNULL(v_PolicyPerDiseaseLimit) OR IS_SPACES(v_PolicyPerDiseaseLimit) OR LENGTH(v_PolicyPerDiseaseLimit)=0, 'N/A', LTRIM(RTRIM(v_PolicyPerDiseaseLimit)))
	IFF(v_PolicyPerDiseaseLimit IS NULL OR IS_SPACES(v_PolicyPerDiseaseLimit) OR LENGTH(v_PolicyPerDiseaseLimit) = 0, 'N/A', LTRIM(RTRIM(v_PolicyPerDiseaseLimit))) AS v_PolicyPerDiseaseLimit_new,
	-- *INF*: DECODE(lower(v_InsuranceLine),
	-- 'generalliability',v_PolicyPerDiseaseLimit_new ,
	-- 'sbopgeneralliability',v_PolicyPerDiseaseLimit_new ,
	-- 'workerscompensation',v_PolicyPerDiseaseLimit_new || '000',
	-- 'N/A')
	DECODE(lower(v_InsuranceLine),
		'generalliability', v_PolicyPerDiseaseLimit_new,
		'sbopgeneralliability', v_PolicyPerDiseaseLimit_new,
		'workerscompensation', v_PolicyPerDiseaseLimit_new || '000',
		'N/A') AS o_PolicyPerDiseaseLimit
	FROM SQ_PolicyLimit
	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyPerOccurenceLimit
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyPerOccurenceLimit.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyPerOccurenceLimit.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyPerOccurenceLimit.Type = 'PolicyPerOccurenceLimit'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_UmbrellaLimit
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_UmbrellaLimit.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_UmbrellaLimit.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_UmbrellaLimit.Type = 'UmbrellaLimit'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_HigherLimit
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_HigherLimit.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_HigherLimit.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_HigherLimit.Type = 'HigherLimit'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Liability
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Liability.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Liability.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Liability.Type = 'Liability'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyAggregateLimit
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyAggregateLimit.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyAggregateLimit.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_PolicyAggregateLimit.Type = 'PolicyAggregateLimit'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Policy
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Policy.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Policy.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_Policy.Type = 'Policy'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_ProductsAggregateLimit
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_ProductsAggregateLimit.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_ProductsAggregateLimit.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_ProductsAggregateLimit.Type = 'ProductsAggregateLimit'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachAccident
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachAccident.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachAccident.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachAccident.Type = 'EachAccident'

	LEFT JOIN LKP_DCLIMITSTAGING LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachEmployeeDisease
	ON LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachEmployeeDisease.SessionId = i_SessionId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachEmployeeDisease.ObjectId = i_LineId
	AND LKP_DCLIMITSTAGING_i_SessionId_i_LineId_EachEmployeeDisease.Type = 'EachEmployeeDisease'

),
AGG_RemoveDuplicates AS (
	SELECT
	TransactionCreatedDate AS i_TransactionCreatedDate,
	-- *INF*: MIN(i_TransactionCreatedDate)
	MIN(i_TransactionCreatedDate) AS o_TransactionCreatedDate,
	o_PolicyKey AS PolicyKey,
	o_InsuranceLine AS InsuranceLine,
	o_PolicyPerOccurenceLimit AS PolicyPerOccurenceLimit,
	o_PolicyAggregateLimit AS PolicyAggregateLimit,
	o_PolicyProductAggregateLimit AS PolicyProductAggregateLimit,
	o_PolicyPerAccidentLimit AS PolicyPerAccidentLimit,
	o_PolicyPerDiseaseLimit AS PolicyPerDiseaseLimit
	FROM EXP_Values
	GROUP BY PolicyKey, InsuranceLine, PolicyPerOccurenceLimit, PolicyAggregateLimit, PolicyProductAggregateLimit, PolicyPerAccidentLimit, PolicyPerDiseaseLimit
),
SRT_Transactions AS (
	SELECT
	PolicyKey, 
	InsuranceLine, 
	o_TransactionCreatedDate AS TransactionCreatedDate, 
	PolicyPerOccurenceLimit, 
	PolicyAggregateLimit, 
	PolicyProductAggregateLimit, 
	PolicyPerAccidentLimit, 
	PolicyPerDiseaseLimit
	FROM AGG_RemoveDuplicates
	ORDER BY PolicyKey ASC, InsuranceLine ASC, TransactionCreatedDate ASC
),
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and exists ( select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT where WCT.PolicyNumber=pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
LKP_PolicyLimit AS (
	SELECT
	PolicyLimitId,
	EffectiveDate,
	PolicyLimitAKId,
	PolicyPerOccurenceLimit,
	PolicyAggregateLimit,
	PolicyProductAggregateLimit,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	PolicyPerClaimLimit,
	PolicyAKId,
	InsuranceLine,
	ExpirationDate
	FROM (
		SELECT PLT.PolicyLimitId as PolicyLimitId, 
		PLT.PolicyLimitAKId as PolicyLimitAKId,
		PLT.PolicyPerOccurenceLimit as PolicyPerOccurenceLimit,
		PLT.PolicyAggregateLimit as PolicyAggregateLimit,
		PLT.PolicyProductAggregateLimit as PolicyProductAggregateLimit,
		PLT.PolicyPerAccidentLimit as PolicyPerAccidentLimit,
		PLT.PolicyPerDiseaseLimit as PolicyPerDiseaseLimit,
		PLT.PolicyPerClaimLimit as PolicyPerClaimLimit,
		PLT.PolicyAKId as PolicyAKId,
		PLT.InsuranceLine as InsuranceLine,
		PLT.EffectiveDate as EffectiveDate,
		PLT.ExpirationDate as ExpirationDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit PLT
		join @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy P
		on PLT.PolicyAKID=P.pol_ak_id and P.crrnt_snpsht_flag=1
		where PLT.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
		exists ( select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT where WCT.PolicyNumber=pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine,EffectiveDate,ExpirationDate ORDER BY PolicyLimitId) = 1
),
SEQ_PolicyLimitAKId AS (
	CREATE SEQUENCE SEQ_PolicyLimitAKId
	START = 0
	INCREMENT = 1;
),
EXP_DetectChange AS (
	SELECT
	LKP_PolicyLimit.PolicyLimitId AS lkp_PolicyLimitId,
	LKP_PolicyLimit.EffectiveDate AS lkp_EffectiveDate,
	LKP_PolicyLimit.PolicyLimitAKId AS lkp_PolicyLimitAKId,
	LKP_PolicyLimit.PolicyPerOccurenceLimit AS lkp_PolicyPerOccurenceLimit,
	LKP_PolicyLimit.PolicyAggregateLimit AS lkp_PolicyAggregateLimit,
	LKP_PolicyLimit.PolicyProductAggregateLimit AS lkp_PolicyProductAggregateLimit,
	LKP_PolicyLimit.PolicyPerAccidentLimit AS lkp_PolicyPerAccidentLimit,
	LKP_PolicyLimit.PolicyPerDiseaseLimit AS lkp_PolicyPerDiseaseLimit,
	LKP_PolicyLimit.PolicyPerClaimLimit AS lkp_PolicyPerClaimLimit,
	SEQ_PolicyLimitAKId.NEXTVAL AS i_NEXTVAL,
	SRT_Transactions.TransactionCreatedDate AS i_TransactionCreatedDate,
	LKP_Policy.pol_ak_id,
	SRT_Transactions.InsuranceLine,
	SRT_Transactions.PolicyPerOccurenceLimit,
	SRT_Transactions.PolicyAggregateLimit,
	SRT_Transactions.PolicyProductAggregateLimit,
	SRT_Transactions.PolicyPerAccidentLimit,
	SRT_Transactions.PolicyPerDiseaseLimit,
	'TBD' AS v_PolicyPerClaimLimit,
	-- *INF*: DECODE(TRUE,ISNULL(lkp_PolicyLimitId) AND pol_ak_id<>-1,'NEW',
	-- pol_ak_id<>-1
	-- AND (lkp_PolicyPerOccurenceLimit<>PolicyPerOccurenceLimit
	-- OR lkp_PolicyAggregateLimit<>PolicyAggregateLimit
	-- OR lkp_PolicyProductAggregateLimit<>PolicyProductAggregateLimit
	-- OR lkp_PolicyPerAccidentLimit<>PolicyPerAccidentLimit
	-- OR lkp_PolicyPerDiseaseLimit<>PolicyPerDiseaseLimit
	-- OR lkp_PolicyPerClaimLimit<>v_PolicyPerClaimLimit), IIF(lkp_EffectiveDate != i_TransactionCreatedDate, 'NEW', 'UPDATE'),'NOCHANGE')
	DECODE(TRUE,
		lkp_PolicyLimitId IS NULL AND pol_ak_id <> - 1, 'NEW',
		pol_ak_id <> - 1 AND ( lkp_PolicyPerOccurenceLimit <> PolicyPerOccurenceLimit OR lkp_PolicyAggregateLimit <> PolicyAggregateLimit OR lkp_PolicyProductAggregateLimit <> PolicyProductAggregateLimit OR lkp_PolicyPerAccidentLimit <> PolicyPerAccidentLimit OR lkp_PolicyPerDiseaseLimit <> PolicyPerDiseaseLimit OR lkp_PolicyPerClaimLimit <> v_PolicyPerClaimLimit ), IFF(lkp_EffectiveDate != i_TransactionCreatedDate, 'NEW', 'UPDATE'),
		'NOCHANGE') AS v_change_flag,
	-- *INF*: DECODE(TRUE,
	-- pol_ak_id = v_Prev_pol_ak_id AND InsuranceLine = v_Prev_InsuranceLine,v_Prev_PolicyLimitAKId,
	--  NOT ISNULL(lkp_PolicyLimitAKId), lkp_PolicyLimitAKId,
	-- i_NEXTVAL 
	-- )
	DECODE(TRUE,
		pol_ak_id = v_Prev_pol_ak_id AND InsuranceLine = v_Prev_InsuranceLine, v_Prev_PolicyLimitAKId,
		NOT lkp_PolicyLimitAKId IS NULL, lkp_PolicyLimitAKId,
		i_NEXTVAL) AS v_PolicyLimitAKId,
	pol_ak_id AS v_Prev_pol_ak_id,
	InsuranceLine AS v_Prev_InsuranceLine,
	v_PolicyLimitAKId AS v_Prev_PolicyLimitAKId,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_TransactionCreatedDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	CURRENT_TIMESTAMP AS o_CreatedDate,
	CURRENT_TIMESTAMP AS o_ModifiedDate,
	v_PolicyLimitAKId AS o_PolicyLimitAKId,
	v_change_flag AS o_change_flag,
	v_PolicyPerClaimLimit AS o_PolicyPerClaimLimit
	FROM SRT_Transactions
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = SRT_Transactions.PolicyKey
	LEFT JOIN LKP_PolicyLimit
	ON LKP_PolicyLimit.PolicyAKId = LKP_Policy.pol_ak_id AND LKP_PolicyLimit.InsuranceLine = SRT_Transactions.InsuranceLine AND LKP_PolicyLimit.EffectiveDate <= SRT_Transactions.TransactionCreatedDate AND LKP_PolicyLimit.ExpirationDate >= SRT_Transactions.TransactionCreatedDate
),
RTR_Insert_Update AS (
	SELECT
	o_change_flag AS i_change_flag,
	lkp_PolicyLimitId AS PolicyLimitId_Inactive,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_PolicyLimitAKId AS PolicyLimitAKId,
	pol_ak_id,
	InsuranceLine,
	PolicyPerOccurenceLimit,
	PolicyAggregateLimit,
	PolicyProductAggregateLimit,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	o_PolicyPerClaimLimit AS PolicyPerClaimLimit
	FROM EXP_DetectChange
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE i_change_flag='NEW'),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE i_change_flag='UPDATE'),
TGT_PolicyLimit_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyLimitAKId, PolicyAKId, InsuranceLine, PolicyPerOccurenceLimit, PolicyAggregateLimit, PolicyProductAggregateLimit, PolicyPerAccidentLimit, PolicyPerDiseaseLimit, PolicyPerClaimLimit)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYLIMITAKID, 
	pol_ak_id AS POLICYAKID, 
	INSURANCELINE, 
	POLICYPEROCCURENCELIMIT, 
	POLICYAGGREGATELIMIT, 
	POLICYPRODUCTAGGREGATELIMIT, 
	POLICYPERACCIDENTLIMIT, 
	POLICYPERDISEASELIMIT, 
	POLICYPERCLAIMLIMIT
	FROM RTR_Insert_Update_INSERT
),
UPD_Existing AS (
	SELECT
	PolicyLimitId_Inactive AS PolicyLimitId, 
	AuditID, 
	ModifiedDate, 
	PolicyPerOccurenceLimit, 
	PolicyAggregateLimit, 
	PolicyProductAggregateLimit, 
	PolicyPerAccidentLimit, 
	PolicyPerDiseaseLimit, 
	PolicyPerClaimLimit
	FROM RTR_Insert_Update_UPDATE
),
TGT_PolicyLimit_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit AS T
	USING UPD_Existing AS S
	ON T.PolicyLimitId = S.PolicyLimitId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.ModifiedDate = S.ModifiedDate, T.PolicyPerOccurenceLimit = S.PolicyPerOccurenceLimit, T.PolicyAggregateLimit = S.PolicyAggregateLimit, T.PolicyProductAggregateLimit = S.PolicyProductAggregateLimit, T.PolicyPerAccidentLimit = S.PolicyPerAccidentLimit, T.PolicyPerDiseaseLimit = S.PolicyPerDiseaseLimit, T.PolicyPerClaimLimit = S.PolicyPerClaimLimit
),
SQ_PolicyLimit_Expired AS (
	SELECT 
		PL.PolicyLimitId, 
		PL.EffectiveDate,
		PL.ExpirationDate, 
		PL.PolicyLimitAKId 
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit PL
	WHERE EXISTS
		( SELECT PolicyLimitAKId  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit b
		WHERE b.CurrentSnapshotFlag = 1 AND b.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	AND b.PolicyLimitAKId=PL.PolicyLimitAKId 
	GROUP BY b.PolicyLimitAKId
	HAVING COUNT(*) > 1) 
	AND PL.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	ORDER BY PL.PolicyLimitAKId ,PL.EffectiveDate DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	PolicyLimitAKId AS in_PolicyLimitAKId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	PolicyLimitId,
	-- *INF*: DECODE(TRUE,
	-- in_PolicyLimitAKId = v_prev_PolicyLimitAKId ,
	-- ADD_TO_DATE(v_prev_EffectiveDate,'SS',-1),orig_ExpirationDate)
	DECODE(TRUE,
		in_PolicyLimitAKId = v_prev_PolicyLimitAKId, ADD_TO_DATE(v_prev_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	in_PolicyLimitAKId AS v_prev_PolicyLimitAKId,
	EffectiveDate AS v_prev_EffectiveDate,
	0 AS out_CurrentSnapshotFlag,
	v_ExpirationDate AS out_ExpirationDate,
	CURRENT_TIMESTAMP AS out_ModifiedDate
	FROM SQ_PolicyLimit_Expired
),
FIL_FirstRowInAKGroup AS (
	SELECT
	orig_ExpirationDate AS in_orig_ExpirationDate, 
	PolicyLimitId, 
	out_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	out_ExpirationDate AS ExpirationDate, 
	out_ModifiedDate AS ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE in_orig_ExpirationDate != ExpirationDate
),
EXPIRE_policy AS (
	SELECT
	PolicyLimitId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
TGT_PolicyLimit_Expired AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit AS T
	USING EXPIRE_policy AS S
	ON T.PolicyLimitId = S.PolicyLimitId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),