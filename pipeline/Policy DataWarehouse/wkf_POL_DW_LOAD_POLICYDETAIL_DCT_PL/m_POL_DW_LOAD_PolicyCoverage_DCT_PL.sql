WITH
SQ_WorkDCTPLCoverage AS (
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	substring(REPLACE(Addresskey,P.Policykey+'||',''),1,charindex('|',REPLACE(Addresskey,P.Policykey+'||','') ,1)-1) Locationid,
	'0000' LocationNumber,
	P.PolicyEffectiveDate,
	P.PolicyExpirationDate,
	0.00 CommissionCustomerCareAmount
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLLocation L
	on P.PolicyKey=L.PolicyKey
	and P.StartDate=L.StartDate
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	and L.AddressKey=C.RiskAddressKey
	and not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION
	
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	'' Locationid,
	'0000' LocationNumber,
	P.PolicyEffectiveDate,
	P.PolicyExpirationDate,
	0.00 CommissionCustomerCareAmount
	from
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	WHERE 
	not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Set_Keys AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	-- *INF*: PolicyNumber || IIF(ISNULL(ltrim(rtrim(PolicyVersion))) or Length(ltrim(rtrim(PolicyVersion)))=0 or IS_SPACES(PolicyVersion),'00',PolicyVersion)
	PolicyNumber || IFF(ltrim(rtrim(PolicyVersion)) IS NULL OR Length(ltrim(rtrim(PolicyVersion))) = 0 OR IS_SPACES(PolicyVersion), '00', PolicyVersion) AS v_PolicyKey,
	v_PolicyKey AS o_PolicyKey,
	v_PolicyKey || '|' || Locationid || '|' || LocationNumber AS o_RiskLocationKey,
	Locationid,
	LocationNumber,
	'N/A' AS LineOfInsuranceDesc,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	CommissionCustomerCareAmount
	FROM SQ_WorkDCTPLCoverage
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
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT
		where WCT.PolicyNumber=pol_num
		and ISNULL(RIGHT('00'+CONVERT(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
LKP_RiskLocation AS (
	SELECT
	RiskLocationAKID,
	RiskLocationKey
	FROM (
		SELECT 
			RiskLocationAKID,
			RiskLocationKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
		WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and
		PolicyAKId in (
		select pol_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT
		where WCT.PolicyNumber=pol.pol_num
		and ISNULL(RIGHT('00'+CONVERT(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod)
		and pol.crrnt_snpsht_flag=1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
),
EXP_Values AS (
	SELECT
	EXP_Set_Keys.LineOfInsuranceDesc AS i_Type,
	EXP_Set_Keys.PolicyEffectiveDate AS i_EffectiveDate,
	EXP_Set_Keys.PolicyExpirationDate AS i_ExpirationDate,
	EXP_Set_Keys.CommissionCustomerCareAmount,
	LKP_Policy.pol_ak_id AS i_pol_ak_id,
	LKP_RiskLocation.RiskLocationAKID AS i_RiskLocationAKID,
	-- *INF*: IIF(ISNULL(i_pol_ak_id),-1,i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL, - 1, i_pol_ak_id) AS o_Pol_AK_ID,
	-- *INF*: IIF(ISNULL(i_RiskLocationAKID),-1,i_RiskLocationAKID)
	-- --i_pol_ak_id||i_LocationNumber||i_Territory||i_LocationXmlId
	-- 
	-- --i_Id||i_PolicyVersion||i_LocationNumber||i_Territory||i_LocationXmlId
	IFF(i_RiskLocationAKID IS NULL, - 1, i_RiskLocationAKID) AS o_RiskLocationAKID,
	i_Type AS o_Type,
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: IIF(ISNULL(i_ExpirationDate),TO_DATE('21001231235959' , 'YYYYMMDDHH24MISS'),i_ExpirationDate)
	IFF(i_ExpirationDate IS NULL, TO_DATE('21001231235959', 'YYYYMMDDHH24MISS'), i_ExpirationDate) AS o_ExpirationDate,
	'0' AS o_AuditableIndicator,
	'N/A' AS o_RiskGradeCode,
	i_LineOfBusiness AS o_PriorInsuranceLine,
	'Guaranteed Cost' AS o_RatingPlanDescription,
	-1 AS PolicyLimitAKId,
	-1 AS PriorCoverageId,
	-1 AS InterstateRiskID
	FROM EXP_Set_Keys
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Set_Keys.o_PolicyKey
	LEFT JOIN LKP_RiskLocation
	ON LKP_RiskLocation.RiskLocationKey = EXP_Set_Keys.o_RiskLocationKey
),
LKP_RatingPlan AS (
	SELECT
	RatingPlanAKId,
	RatingPlanDescription
	FROM (
		SELECT 
			RatingPlanAKId,
			RatingPlanDescription
		FROM RatingPlan
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingPlanDescription ORDER BY RatingPlanAKId) = 1
),
LKP_sup_insurance_line AS (
	SELECT
	sup_ins_line_id,
	ins_line_descript
	FROM (
		SELECT 
			sup_ins_line_id,
			ins_line_descript
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_descript ORDER BY sup_ins_line_id) = 1
),
EXP_MD5 AS (
	SELECT
	EXP_Values.o_RiskLocationAKID AS i_RiskLocationAKID,
	EXP_Values.o_Pol_AK_ID AS i_pol_ak_id,
	LKP_sup_insurance_line.sup_ins_line_id AS i_sup_ins_line_id,
	EXP_Values.o_Type AS i_Type,
	EXP_Values.o_EffectiveDate AS i_EffectiveDate,
	EXP_Values.o_ExpirationDate AS i_ExpirationDate,
	EXP_Values.InterstateRiskID AS i_InterstateRiskID,
	EXP_Values.o_AuditableIndicator AS AuditableIndicator,
	EXP_Values.o_RiskGradeCode AS RiskGradeCode,
	-- *INF*: IIF(ISNULL(i_pol_ak_id),ERROR('Pol_ak_id can not be blank!'),i_pol_ak_id)
	IFF(i_pol_ak_id IS NULL, ERROR('Pol_ak_id can not be blank!'), i_pol_ak_id) AS v_pol_ak_id,
	-- *INF*: IIF(ISNULL(i_RiskLocationAKID),ERROR('RiskLocationAKID can not be blank!'),i_RiskLocationAKID)
	IFF(i_RiskLocationAKID IS NULL, ERROR('RiskLocationAKID can not be blank!'), i_RiskLocationAKID) AS v_RiskLocationAKID,
	-- *INF*: TO_CHAR(i_pol_ak_id)||'~'||TO_CHAR(i_RiskLocationAKID)
	-- 
	-- --- Change ID and version with Pol_ak_id for UID Project
	TO_CHAR(i_pol_ak_id) || '~' || TO_CHAR(i_RiskLocationAKID) AS v_PolicyCoverageKey,
	-- *INF*: MD5(TO_CHAR(v_pol_ak_id)||TO_CHAR(v_RiskLocationAKID)||i_Type||TO_CHAR(i_EffectiveDate))
	MD5(TO_CHAR(v_pol_ak_id) || TO_CHAR(v_RiskLocationAKID) || i_Type || TO_CHAR(i_EffectiveDate)) AS o_PolicyCoverageHashKey,
	v_pol_ak_id AS o_PolicyAKID,
	v_RiskLocationAKID AS o_RiskLocationAKID,
	-- *INF*: IIF(ISNULL(v_PolicyCoverageKey),'N/A',v_PolicyCoverageKey)
	IFF(v_PolicyCoverageKey IS NULL, 'N/A', v_PolicyCoverageKey) AS o_PolicyCoverageKey,
	i_Type AS o_InsuranceLine,
	i_Type AS o_TypeBureauCode,
	i_EffectiveDate AS o_PolicyCoverageEffectiveDate,
	i_ExpirationDate AS o_PolicyCoverageExpirationDate,
	-- *INF*: IIF(ISNULL(i_sup_ins_line_id),-1,i_sup_ins_line_id)
	IFF(i_sup_ins_line_id IS NULL, - 1, i_sup_ins_line_id) AS o_sup_ins_line_id,
	-- *INF*: -1
	-- --IIF(ISNULL(i_sup_type_bureau_code_id),---1,i_sup_type_bureau_code_id)
	- 1 AS o_sup_type_bureau_code_id,
	-- *INF*: IIF(ISNULL(i_InterstateRiskID),'N/A',TO_CHAR(i_InterstateRiskID))
	IFF(i_InterstateRiskID IS NULL, 'N/A', TO_CHAR(i_InterstateRiskID)) AS o_InterstateRiskId,
	EXP_Values.PolicyLimitAKId,
	EXP_Values.PriorCoverageId,
	EXP_Values.CommissionCustomerCareAmount,
	LKP_RatingPlan.RatingPlanAKId
	FROM EXP_Values
	LEFT JOIN LKP_RatingPlan
	ON LKP_RatingPlan.RatingPlanDescription = EXP_Values.o_RatingPlanDescription
	LEFT JOIN LKP_sup_insurance_line
	ON LKP_sup_insurance_line.ins_line_descript = EXP_Values.o_Type
),
LKP_PolicyCoverage AS (
	SELECT
	PolicyCoverageAKID,
	PolicyCoverageExpirationDate,
	AuditableIndicator,
	RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	CustomerCareCommissionRate,
	RatingPlanAkId,
	PolicyCoverageHashKey
	FROM (
		SELECT 
			PolicyCoverageAKID,
			PolicyCoverageExpirationDate,
			AuditableIndicator,
			RiskGradeCode,
			InterstateRiskId,
			PolicyLimitAKId,
			PriorCoverageId,
			CustomerCareCommissionRate,
			RatingPlanAkId,
			PolicyCoverageHashKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
		WHERE CurrentSnapshotFlag='1' and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and
		PolicyAKId in (
		select pol_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT
		where WCT.PolicyNumber=pol.pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod)
		and pol.crrnt_snpsht_flag=1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_PolicyCoverage.PolicyCoverageAKID AS i_PolicyCoverageAKID,
	LKP_PolicyCoverage.PolicyCoverageExpirationDate AS i_PolicyCoverageExpirationDate,
	LKP_PolicyCoverage.AuditableIndicator AS i_AuditableIndicator,
	LKP_PolicyCoverage.RiskGradeCode AS i_RiskGradeCode,
	LKP_PolicyCoverage.InterstateRiskId AS i_InterstateRiskId,
	LKP_PolicyCoverage.PolicyLimitAKId AS i_PolicyLimitAKId,
	LKP_PolicyCoverage.PriorCoverageId AS i_PriorCoverageId,
	LKP_PolicyCoverage.CustomerCareCommissionRate AS i_CustomerCareCommissionRate,
	LKP_PolicyCoverage.RatingPlanAkId AS i_RatingPlanAkId,
	-- *INF*: DECODE(i_AuditableIndicator,'T', '1', 'F','0', NULL)
	DECODE(i_AuditableIndicator,
	'T', '1',
	'F', '0',
	NULL) AS v_LKP_AuditableIndicator,
	EXP_MD5.AuditableIndicator,
	EXP_MD5.RiskGradeCode,
	EXP_MD5.o_PolicyCoverageHashKey AS PolicyCoverageHashKey,
	EXP_MD5.o_PolicyAKID AS PolicyAKID,
	EXP_MD5.o_RiskLocationAKID AS RiskLocationAKID,
	EXP_MD5.o_PolicyCoverageKey AS PolicyCoverageKey,
	EXP_MD5.o_InsuranceLine AS InsuranceLine,
	EXP_MD5.o_TypeBureauCode AS TypeBureauCode,
	EXP_MD5.o_PolicyCoverageEffectiveDate AS PolicyCoverageEffectiveDate,
	EXP_MD5.o_PolicyCoverageExpirationDate AS PolicyCoverageExpirationDate,
	EXP_MD5.o_sup_ins_line_id AS sup_ins_line_id,
	EXP_MD5.o_sup_type_bureau_code_id AS sup_type_bureau_code_id,
	EXP_MD5.CommissionCustomerCareAmount,
	EXP_MD5.o_InterstateRiskId AS InterstateRiskId,
	EXP_MD5.PolicyLimitAKId,
	EXP_MD5.PriorCoverageId,
	EXP_MD5.RatingPlanAKId,
	-- *INF*: IIF(ISNULL(i_PolicyCoverageAKID), 'NEW', IIF(i_PolicyCoverageExpirationDate<>PolicyCoverageExpirationDate OR v_LKP_AuditableIndicator<>AuditableIndicator OR i_RiskGradeCode<>RiskGradeCode OR i_InterstateRiskId<>InterstateRiskId OR i_PolicyLimitAKId<>PolicyLimitAKId OR i_PriorCoverageId<>PriorCoverageId OR ISNULL(i_CustomerCareCommissionRate) OR i_CustomerCareCommissionRate<>CommissionCustomerCareAmount OR i_RatingPlanAkId <> RatingPlanAKId,
	-- 'UPDATE', 'NOCHANGE'))
	IFF(i_PolicyCoverageAKID IS NULL, 'NEW', IFF(i_PolicyCoverageExpirationDate <> PolicyCoverageExpirationDate OR v_LKP_AuditableIndicator <> AuditableIndicator OR i_RiskGradeCode <> RiskGradeCode OR i_InterstateRiskId <> InterstateRiskId OR i_PolicyLimitAKId <> PolicyLimitAKId OR i_PriorCoverageId <> PriorCoverageId OR i_CustomerCareCommissionRate IS NULL OR i_CustomerCareCommissionRate <> CommissionCustomerCareAmount OR i_RatingPlanAkId <> RatingPlanAKId, 'UPDATE', 'NOCHANGE')) AS o_ChangeFlag
	FROM EXP_MD5
	LEFT JOIN LKP_PolicyCoverage
	ON LKP_PolicyCoverage.PolicyCoverageHashKey = EXP_MD5.o_PolicyCoverageHashKey
),
FIL_InsertNewRows AS (
	SELECT
	i_PolicyCoverageAKID, 
	AuditableIndicator, 
	RiskGradeCode, 
	PolicyCoverageHashKey, 
	PolicyAKID, 
	RiskLocationAKID, 
	PolicyCoverageKey, 
	InsuranceLine, 
	TypeBureauCode, 
	PolicyCoverageEffectiveDate, 
	PolicyCoverageExpirationDate, 
	sup_ins_line_id, 
	sup_type_bureau_code_id, 
	o_ChangeFlag AS ChangeFlag, 
	InterstateRiskId, 
	PolicyLimitAKId, 
	PriorCoverageId, 
	CommissionCustomerCareAmount, 
	RatingPlanAKId
	FROM EXP_DetectChange
	WHERE ChangeFlag='NEW'  OR ChangeFlag='UPDATE'
),
SEQ_PolicyCoverageAKID AS (
	CREATE SEQUENCE SEQ_PolicyCoverageAKID
	START = 0
	INCREMENT = 1;
),
EXP_AKandMetaData AS (
	SELECT
	i_PolicyCoverageAKID,
	AuditableIndicator AS i_AuditableIndicator,
	RiskGradeCode AS i_RiskGradeCode,
	PolicyCoverageHashKey AS i_PolicyCoverageHashKey,
	PolicyAKID AS i_PolicyAKID,
	RiskLocationAKID AS i_RiskLocationAKID,
	PolicyCoverageKey AS i_PolicyCoverageKey,
	InsuranceLine AS i_InsuranceLine,
	TypeBureauCode AS i_TypeBureauCode,
	PolicyCoverageEffectiveDate AS i_PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate AS i_PolicyCoverageExpirationDate,
	sup_ins_line_id AS i_sup_ins_line_id,
	sup_type_bureau_code_id AS i_sup_type_bureau_code_id,
	ChangeFlag AS i_ChangeFlag,
	SEQ_PolicyCoverageAKID.NEXTVAL,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: IIF(i_ChangeFlag='NEW', TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE)
	IFF(i_ChangeFlag = 'NEW', TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59' , 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreateDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_LogicalIndicator,
	i_PolicyCoverageHashKey AS o_PolicyCoverageHashKey,
	-- *INF*: IIF(ISNULL(i_PolicyCoverageAKID),NEXTVAL, i_PolicyCoverageAKID)
	IFF(i_PolicyCoverageAKID IS NULL, NEXTVAL, i_PolicyCoverageAKID) AS o_PolicyCoverageAKID,
	i_PolicyAKID AS o_PolicyAKID,
	i_RiskLocationAKID AS o_RiskLocationAKID,
	i_PolicyCoverageKey AS o_PolicyCoverageKey,
	i_InsuranceLine AS o_InsuranceLine,
	i_TypeBureauCode AS o_TypeBureauCode,
	i_PolicyCoverageEffectiveDate AS o_PolicyCoverageEffectiveDate,
	i_PolicyCoverageExpirationDate AS o_PolicyCoverageExpirationDate,
	i_sup_ins_line_id AS o_sup_ins_line_id,
	i_sup_type_bureau_code_id AS o_sup_type_bureau_code_id,
	i_AuditableIndicator AS o_AuditableIndicator,
	i_RiskGradeCode AS o_RiskGradeCode,
	InterstateRiskId,
	PolicyLimitAKId,
	PriorCoverageId,
	CommissionCustomerCareAmount,
	RatingPlanAKId
	FROM FIL_InsertNewRows
),
TGT_PolicyCoverage_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, PolicyCoverageHashKey, PolicyCoverageAKID, PolicyAKID, RiskLocationAKID, PolicyCoverageKey, InsuranceLine, TypeBureauCode, PolicyCoverageEffectiveDate, PolicyCoverageExpirationDate, SupInsuranceLineId, SupTypeBureauCodeId, RatingPlanAKId, AuditableIndicator, RiskGradeCode, InterstateRiskId, PolicyLimitAKId, PriorCoverageId, CustomerCareCommissionRate)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreateDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_LogicalIndicator AS LOGICALINDICATOR, 
	o_PolicyCoverageHashKey AS POLICYCOVERAGEHASHKEY, 
	o_PolicyCoverageAKID AS POLICYCOVERAGEAKID, 
	o_PolicyAKID AS POLICYAKID, 
	o_RiskLocationAKID AS RISKLOCATIONAKID, 
	o_PolicyCoverageKey AS POLICYCOVERAGEKEY, 
	o_InsuranceLine AS INSURANCELINE, 
	o_TypeBureauCode AS TYPEBUREAUCODE, 
	o_PolicyCoverageEffectiveDate AS POLICYCOVERAGEEFFECTIVEDATE, 
	o_PolicyCoverageExpirationDate AS POLICYCOVERAGEEXPIRATIONDATE, 
	o_sup_ins_line_id AS SUPINSURANCELINEID, 
	o_sup_type_bureau_code_id AS SUPTYPEBUREAUCODEID, 
	RATINGPLANAKID, 
	o_AuditableIndicator AS AUDITABLEINDICATOR, 
	o_RiskGradeCode AS RISKGRADECODE, 
	INTERSTATERISKID, 
	POLICYLIMITAKID, 
	PRIORCOVERAGEID, 
	CommissionCustomerCareAmount AS CUSTOMERCARECOMMISSIONRATE
	FROM EXP_AKandMetaData
),
SQ_PolicyCoverage AS (
	SELECT 
		PolicyCoverageID,
		EffectiveDate,
		ExpirationDate,
		PolicyCoverageAKID 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage a
	WHERE  exists 
		   (SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage b
	           WHERE CurrentSnapshotFlag = 1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND a.PolicyCoverageAKID=b.PolicyCoverageAKID GROUP BY PolicyCoverageAKID  HAVING count(*) > 1)
	AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' and CurrentSnapshotFlag=1
	ORDER BY PolicyCoverageAKID , EffectiveDate  DESC
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	EffectiveDate AS i_eff_from_date,
	PolicyCoverageAKID AS i_PolicyCoverageAKID,
	ExpirationDate AS orig_eff_to_date,
	PolicyCoverageID,
	-- *INF*: DECODE(TRUE,
	-- i_PolicyCoverageAKID = v_prev_cust_ak_id  ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
	i_PolicyCoverageAKID = v_prev_cust_ak_id, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	i_PolicyCoverageAKID AS v_prev_cust_ak_id,
	i_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	v_eff_to_date AS eff_to_date,
	SYSDATE AS modified_date
	FROM SQ_PolicyCoverage
),
FIL_FirstRowInAKGroup AS (
	SELECT
	orig_eff_to_date AS i_orig_eff_to_date, 
	PolicyCoverageID, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE i_orig_eff_to_date != eff_to_date
),
UPD_PolicyCoverage AS (
	SELECT
	PolicyCoverageID, 
	crrnt_snpsht_flag AS CurrentSnapshotFlag, 
	eff_to_date AS ExpirationDate, 
	modified_date AS ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
TGT_PolicyCoverage_Update AS (
	MERGE INTO PolicyCoverage AS T
	USING UPD_PolicyCoverage AS S
	ON T.PolicyCoverageID = S.PolicyCoverageID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),