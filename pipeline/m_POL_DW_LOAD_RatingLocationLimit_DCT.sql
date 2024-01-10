WITH
SQ_RatingLocationLimit AS (
	select * from
	(select distinct p.PolicyNumber+right('00'+convert(varchar(10),ISNULL(p.PolicyVersion,0)),2) as PolicyKey
	,w.LineType as Insuranceline
	,lo.LocationXmlId as RatingLocationKey
	,p.TransactionCreatedDate as TransactionEnteredDate
	,DLT.Type as RatingLocationLimitType
	,DLT.Value as RatingLocationLimitValue
	from DCLimitStaging DLT
	join DCLocationAssociationStaging la
	on la.ObjectId=DLT.ObjectId
	and la.ObjectName='DC_BP_Location'
	join DCLocationStaging lo
	on lo.LocationId=la.LocationId
	join WorkDCTTransactionInsuranceLineLocationBridge l
	on l.LocationAssociationId=la.LocationAssociationId
	join WorkDCTInsuranceLine w
	on w.LineId=l.LineId
	and w.LineType='BusinessOwners'
	join WorkDCTPolicy p
	on p.SessionId=DLT.SessionId
	and p.TransactionPurpose<>'Offset'
	and p.TransactionState='committed'
	and p.PolicyStatus<>'Quote'
	where DLT.Type='EquipmentBreakdown'
	and DLT.ObjectName='DC_BP_Location'
	union all
	select distinct p.PolicyNumber+right('00'+convert(varchar(10),ISNULL(p.PolicyVersion,0)),2) as PolicyKey
	,w.LineType as Insuranceline
	,lo.LocationXmlId as RatingLocationKey
	,p.TransactionCreatedDate
	,DLT.Type as RatingLocationLimitType
	,DLT.Value as RatingLocationLimitValue
	from DCLimitStaging DLT
	join DCLocationAssociationStaging la
	on la.ObjectId=DLT.ObjectId
	and la.ObjectName='DC_CF_Location'
	join DCLocationStaging lo
	on lo.LocationId=la.LocationId
	join WorkDCTTransactionInsuranceLineLocationBridge l
	on l.LocationAssociationId=la.LocationAssociationId
	join WorkDCTInsuranceLine w
	on w.LineId=l.LineId
	and w.LineType in ('Property','SBOPProperty')
	join WorkDCTPolicy p
	on p.SessionId=DLT.SessionId
	and p.TransactionPurpose<>'Deprecated'
	and p.TransactionState='committed'
	and p.PolicyStatus<>'Quote'
	where DLT.Type='EquipmentBreakdown'
	and DLT.ObjectName='DC_CF_Location') a
	order by PolicyKey,InsuranceLine,RatingLocationKey,RatingLocationLimitType,TransactionEnteredDate
),
AGG_RemoveDuplicateShreds AS (
	SELECT
	PolicyKey,
	InsuranceLine,
	RatingLocationKey,
	RatingLocationLimitType,
	TransactionEnteredDate,
	RatingLocationLimitValue
	FROM SQ_RatingLocationLimit
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey, InsuranceLine, RatingLocationKey, RatingLocationLimitType, TransactionEnteredDate ORDER BY NULL) = 1
),
EXP_Default AS (
	SELECT
	PolicyKey,
	InsuranceLine,
	RatingLocationKey,
	RatingLocationLimitType,
	TransactionEnteredDate,
	RatingLocationLimitValue,
	-- *INF*: IIF(PolicyKey=v_prev_PolicyKey AND InsuranceLine=v_prev_InsuranceLine AND RatingLocationKey=v_prev_RatingLocationKey AND RatingLocationLimitType=v_prev_RatingLocationLimitType,1,0)
	IFF(PolicyKey = v_prev_PolicyKey AND InsuranceLine = v_prev_InsuranceLine AND RatingLocationKey = v_prev_RatingLocationKey AND RatingLocationLimitType = v_prev_RatingLocationLimitType, 1, 0) AS v_SameGroupFlag,
	-- *INF*: DECODE(TRUE,
	-- v_SameGroupFlag=0,1,
	-- RatingLocationLimitValue!=v_prev_RatingLocationLimitValue,1,
	-- 0)
	DECODE(TRUE,
		v_SameGroupFlag = 0, 1,
		RatingLocationLimitValue != v_prev_RatingLocationLimitValue, 1,
		0) AS v_Filter,
	PolicyKey AS v_prev_PolicyKey,
	InsuranceLine AS v_prev_InsuranceLine,
	RatingLocationKey AS v_prev_RatingLocationKey,
	RatingLocationLimitType AS v_prev_RatingLocationLimitType,
	RatingLocationLimitValue AS v_prev_RatingLocationLimitValue,
	v_Filter AS o_Filter
	FROM AGG_RemoveDuplicateShreds
),
FIL_NoChange AS (
	SELECT
	PolicyKey, 
	InsuranceLine, 
	RatingLocationKey, 
	RatingLocationLimitType, 
	TransactionEnteredDate, 
	RatingLocationLimitValue, 
	o_Filter AS Filter
	FROM EXP_Default
	WHERE Filter=1
),
LKP_policy AS (
	SELECT
	pol_ak_id,
	PolicyKey,
	pol_key
	FROM (
		SELECT p.pol_ak_id as pol_ak_id, p.pol_key as pol_key
		FROM V2.policy p
		inner hash join (
		select distinct PolicyNumber from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy) w
		on p.pol_num=w.PolicyNumber
		where p.crrnt_snpsht_flag=1 AND p.source_sys_id='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
LKP_RatingLocationLimit AS (
	SELECT
	RatingLocationLimitAKId,
	RatingLocationLimitValue,
	PolicyAKId,
	InsuranceLine,
	RatingLocationKey,
	RatingLocationLimitType,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT a.RatingLocationLimitAKId as RatingLocationLimitAKId,
		a.RatingLocationLimitValue as RatingLocationLimitValue,
		a.PolicyAKId as PolicyAKId,
		a.InsuranceLine as InsuranceLine,
		a.RatingLocationKey as RatingLocationKey,
		a.RatingLocationLimitType as RatingLocationLimitType,
		a.EffectiveDate as EffectiveDate,
		a.ExpirationDate as ExpirationDate
		FROM RatingLocationLimit a
		join (
		select distinct LocationXMLId from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging) b
		on a.RatingLocationKey=b.LocationXMLId
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKId,InsuranceLine,RatingLocationKey,RatingLocationLimitType,EffectiveDate,ExpirationDate ORDER BY RatingLocationLimitAKId) = 1
),
SEQ_LocationLimit AS (
	CREATE SEQUENCE SEQ_LocationLimit
	START = 1
	INCREMENT = 1;
),
EXP_Change AS (
	SELECT
	SEQ_LocationLimit.NEXTVAL,
	LKP_RatingLocationLimit.RatingLocationLimitAKId AS lkp_RatingLocationLimitAKId,
	LKP_RatingLocationLimit.RatingLocationLimitValue AS lkp_RatingLocationLimitValue,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	TransactionEnteredDate AS EffectiveDate,
	-- *INF*: TO_DATE('21001231235959','YYYYMMDDHH24MISS')
	TO_DATE('21001231235959', 'YYYYMMDDHH24MISS') AS ExpirationDate,
	'DCT' AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	LKP_policy.pol_ak_id AS PolicyAKId,
	FIL_NoChange.InsuranceLine,
	FIL_NoChange.RatingLocationKey,
	FIL_NoChange.RatingLocationLimitType,
	FIL_NoChange.TransactionEnteredDate,
	FIL_NoChange.RatingLocationLimitValue,
	-- *INF*: IIF(PolicyAKId=v_prev_PolicyAKId AND v_prev_InsuranceLine=InsuranceLine AND RatingLocationKey=v_prev_RatingLocationKey AND RatingLocationLimitType=v_prev_RatingLocationLimitType,1,0)
	IFF(PolicyAKId = v_prev_PolicyAKId AND v_prev_InsuranceLine = InsuranceLine AND RatingLocationKey = v_prev_RatingLocationKey AND RatingLocationLimitType = v_prev_RatingLocationLimitType, 1, 0) AS v_SameGroupFlag,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(PolicyAKId),0,
	-- ISNULL(lkp_RatingLocationLimitAKId),1,
	-- RatingLocationLimitValue!=lkp_RatingLocationLimitValue,1,
	-- 0)
	DECODE(TRUE,
		PolicyAKId IS NULL, 0,
		lkp_RatingLocationLimitAKId IS NULL, 1,
		RatingLocationLimitValue != lkp_RatingLocationLimitValue, 1,
		0) AS v_Filter,
	-- *INF*: DECODE(TRUE,
	-- v_SameGroupFlag=1,v_RatingLocationLimitAKId,
	-- NOT ISNULL(lkp_RatingLocationLimitAKId),lkp_RatingLocationLimitAKId,
	-- NEXTVAL)
	DECODE(TRUE,
		v_SameGroupFlag = 1, v_RatingLocationLimitAKId,
		NOT lkp_RatingLocationLimitAKId IS NULL, lkp_RatingLocationLimitAKId,
		NEXTVAL) AS v_RatingLocationLimitAKId,
	PolicyAKId AS v_prev_PolicyAKId,
	InsuranceLine AS v_prev_InsuranceLine,
	RatingLocationKey AS v_prev_RatingLocationKey,
	RatingLocationLimitType AS v_prev_RatingLocationLimitType,
	RatingLocationLimitValue AS v_prev_RatingLocationLimitValue,
	v_RatingLocationLimitAKId AS RatingLocationLimitAKId,
	v_Filter AS Filter
	FROM FIL_NoChange
	LEFT JOIN LKP_RatingLocationLimit
	ON LKP_RatingLocationLimit.PolicyAKId = LKP_policy.pol_ak_id AND LKP_RatingLocationLimit.InsuranceLine = FIL_NoChange.InsuranceLine AND LKP_RatingLocationLimit.RatingLocationKey = FIL_NoChange.RatingLocationKey AND LKP_RatingLocationLimit.RatingLocationLimitType = FIL_NoChange.RatingLocationLimitType AND LKP_RatingLocationLimit.EffectiveDate <= FIL_NoChange.TransactionEnteredDate AND LKP_RatingLocationLimit.ExpirationDate >= FIL_NoChange.TransactionEnteredDate
	LEFT JOIN LKP_policy
	ON LKP_policy.pol_key = FIL_NoChange.PolicyKey
),
RTR_Change AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	RatingLocationLimitAKId,
	PolicyAKId,
	InsuranceLine,
	RatingLocationKey,
	RatingLocationLimitType,
	RatingLocationLimitValue,
	Filter
	FROM EXP_Change
),
RTR_Change_INSERT AS (SELECT * FROM RTR_Change WHERE Filter=1),
TGT_RatingLocationLimit_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingLocationLimit
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, RatingLocationLimitAKId, PolicyAKId, InsuranceLine, RatingLocationKey, RatingLocationLimitType, RatingLocationLimitValue)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RATINGLOCATIONLIMITAKID, 
	POLICYAKID, 
	INSURANCELINE, 
	RATINGLOCATIONKEY, 
	RATINGLOCATIONLIMITTYPE, 
	RATINGLOCATIONLIMITVALUE
	FROM RTR_Change_INSERT
),
SQ_RatingLocationLimit_Expire AS (
	SELECT 
		RatingLocationLimitID,
		EffectiveDate, 
		ExpirationDate,
		RatingLocationLimitAKID
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingLocationLimit a
	WHERE  EXISTS
		 (SELECT 1
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingLocationLimit b 
		   WHERE CurrentSnapshotFlag = 1 and SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		    and a.RatingLocationLimitAKID = b.RatingLocationLimitAKID
	GROUP BY  RatingLocationLimitAKID  HAVING count(*) > 1)
	AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	ORDER BY  RatingLocationLimitAKID ,EffectiveDate  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	RatingLocationLimitId,
	EffectiveDate,
	ExpirationDate,
	RatingLocationLimitAKId,
	-- *INF*: DECODE(TRUE,
	-- RatingLocationLimitAKId = v_PrevRatingLocationLimitAKID ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),ExpirationDate)
	DECODE(TRUE,
		RatingLocationLimitAKId = v_PrevRatingLocationLimitAKID, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		ExpirationDate) AS v_eff_to_date,
	RatingLocationLimitAKId AS v_PrevRatingLocationLimitAKID,
	EffectiveDate AS v_prev_eff_from_date,
	0 AS o_crrnt_snpsht_flag,
	v_eff_to_date AS o_eff_to_date,
	SYSDATE AS o_modified_date
	FROM SQ_RatingLocationLimit_Expire
),
FIL_FirstRowInAKGroup AS (
	SELECT
	RatingLocationLimitId, 
	ExpirationDate, 
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE ExpirationDate != eff_to_date
),
UPD_RatingLocationLimit AS (
	SELECT
	RatingLocationLimitId, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_RatingLocationLimit_Update AS (

	------------ PRE SQL ----------
	UPDATE A
	SET A.EffectiveDate='1800-1-1'
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingLocationLimit A
	WHERE NOT EXISTS (
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingLocationLimit B
	WHERE A.RatingLocationLimitAKId=B.RatingLocationLimitAKId
	AND B.SourceSystemId='DCT'
	and B.EffectiveDate<A.EffectiveDate)
	AND A.EffectiveDate>'1800-1-1'
	AND A.SourceSystemId='DCT'
	-------------------------------


	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingLocationLimit AS T
	USING UPD_RatingLocationLimit AS S
	ON T.RatingLocationLimitId = S.RatingLocationLimitId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.crrnt_snpsht_flag, T.ExpirationDate = S.eff_to_date, T.ModifiedDate = S.modified_date
),