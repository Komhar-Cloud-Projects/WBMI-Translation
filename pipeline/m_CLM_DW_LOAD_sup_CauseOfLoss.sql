WITH
SQ_cause_of_loss_stage AS (
	SELECT
		cause_of_loss_id,
		line_of_business,
		major_peril,
		cause_of_loss,
		num_cause_of_loss,
		alph_cause_of_loss,
		abbr_cause_of_loss,
		cause_of_loss_nm,
		bur_cause_of_loss1,
		bur_cause_of_loss2,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		cov_category_code
	FROM cause_of_loss_stage
),
EXP_Accept_Inputs_Set_Defaults AS (
	SELECT
	line_of_business,
	major_peril,
	cause_of_loss,
	cause_of_loss_nm,
	bur_cause_of_loss1,
	bur_cause_of_loss2,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(line_of_business)),'ACV','AFV','BO','BOP','CF','GL','SMP'), bur_cause_of_loss2,
	-- LTRIM(RTRIM(line_of_business))='CPP' and :UDF.DEFAULT_VALUE_FOR_STRINGS(bur_cause_of_loss2)<>'N/A' , bur_cause_of_loss2,
	-- LTRIM(RTRIM(line_of_business))='CPP' and :UDF.DEFAULT_VALUE_FOR_STRINGS(bur_cause_of_loss2)='N/A', bur_cause_of_loss1,
	-- LTRIM(RTRIM(line_of_business))='IMC', bur_cause_of_loss1,
	-- 'TBD'
	-- )
	-- 
	-- 
	-- 
	DECODE(TRUE,
		LTRIM(RTRIM(line_of_business
			)
		) IN ('ACV','AFV','BO','BOP','CF','GL','SMP'), bur_cause_of_loss2,
		LTRIM(RTRIM(line_of_business
			)
		) = 'CPP' 
		AND :UDF.DEFAULT_VALUE_FOR_STRINGS(bur_cause_of_loss2
		) <> 'N/A', bur_cause_of_loss2,
		LTRIM(RTRIM(line_of_business
			)
		) = 'CPP' 
		AND :UDF.DEFAULT_VALUE_FOR_STRINGS(bur_cause_of_loss2
		) = 'N/A', bur_cause_of_loss1,
		LTRIM(RTRIM(line_of_business
			)
		) = 'IMC', bur_cause_of_loss1,
		'TBD'
	) AS v_BureauCauseOfLoss,
	-- *INF*: IIF(ISNULL(cause_of_loss_nm),'N/A',rtrim(ltrim(cause_of_loss_nm)))
	IFF(cause_of_loss_nm IS NULL,
		'N/A',
		rtrim(ltrim(cause_of_loss_nm
			)
		)
	) AS OUT_cause_of_loss_nm,
	source_system_id,
	-- *INF*: IIF(ISNULL(source_system_id),'N/A',rtrim(ltrim(source_system_id)))
	IFF(source_system_id IS NULL,
		'N/A',
		rtrim(ltrim(source_system_id
			)
		)
	) AS OUT_source_system_id,
	'1' AS current_snapshot_flag,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(v_BureauCauseOfLoss))) OR LENGTH(LTRIM(RTRIM(v_BureauCauseOfLoss)))=0 OR IS_SPACES(LTRIM(RTRIM(v_BureauCauseOfLoss))),'N/A',LTRIM(RTRIM(v_BureauCauseOfLoss)))
	IFF(LTRIM(RTRIM(v_BureauCauseOfLoss
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(v_BureauCauseOfLoss
				)
			)
		) = 0 
		OR LENGTH(LTRIM(RTRIM(v_BureauCauseOfLoss
			)
		))>0 AND TRIM(LTRIM(RTRIM(v_BureauCauseOfLoss
			)
		))='',
		'N/A',
		LTRIM(RTRIM(v_BureauCauseOfLoss
			)
		)
	) AS OUT_BureauCauseOfLoss
	FROM SQ_cause_of_loss_stage
),
LKP_Sup_CauseOfLoss AS (
	SELECT
	CauseOfLossId,
	CauseOfLossAKID,
	CauseOfLossName,
	BureauCauseOfLoss,
	CauseOfLoss,
	LineOfBusiness,
	MajorPeril
	FROM (
		SELECT 
		sup_CauseOfLoss.CauseOfLossId as CauseOfLossId, 
		sup_CauseOfLoss.CauseOfLossAKID as CauseOfLossAKID, 
		sup_CauseOfLoss.CauseOfLossName as CauseOfLossName, 
		sup_CauseOfLoss.BureauCauseOfLoss as BureauCauseOfLoss, 
		sup_CauseOfLoss.CauseOfLoss as CauseOfLoss, 
		sup_CauseOfLoss.LineOfBusiness as LineOfBusiness, 
		sup_CauseOfLoss.MajorPeril as MajorPeril
		FROM 
		sup_CauseOfLoss
		WHERE
		sup_CauseOfLoss.CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CauseOfLoss,LineOfBusiness,MajorPeril ORDER BY CauseOfLossId DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Sup_CauseOfLoss.CauseOfLossId AS lkp_CauseOfLossId,
	LKP_Sup_CauseOfLoss.CauseOfLossAKID AS lkp_CauseOfLossAKID,
	LKP_Sup_CauseOfLoss.CauseOfLossName AS lkp_CauseOfLossName,
	LKP_Sup_CauseOfLoss.BureauCauseOfLoss AS lkp_BureauCauseOfLoss,
	EXP_Accept_Inputs_Set_Defaults.OUT_source_system_id AS source_system_id,
	EXP_Accept_Inputs_Set_Defaults.line_of_business,
	EXP_Accept_Inputs_Set_Defaults.major_peril,
	EXP_Accept_Inputs_Set_Defaults.cause_of_loss,
	EXP_Accept_Inputs_Set_Defaults.OUT_cause_of_loss_nm AS cause_of_loss_nm,
	EXP_Accept_Inputs_Set_Defaults.current_snapshot_flag,
	EXP_Accept_Inputs_Set_Defaults.OUT_BureauCauseOfLoss AS BureauCauseOfLoss,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CauseOfLossId),'NEW',
	-- RTRIM(LTRIM(lkp_CauseOfLossName)) <> RTRIM(LTRIM(cause_of_loss_nm))
	-- OR RTRIM(LTRIM(lkp_BureauCauseOfLoss)) <> RTRIM(LTRIM(BureauCauseOfLoss)),'UPDATE',
	-- 'NOCHANGE')
	-- 
	DECODE(TRUE,
		lkp_CauseOfLossId IS NULL, 'NEW',
		RTRIM(LTRIM(lkp_CauseOfLossName
			)
		) <> RTRIM(LTRIM(cause_of_loss_nm
			)
		) 
		OR RTRIM(LTRIM(lkp_BureauCauseOfLoss
			)
		) <> RTRIM(LTRIM(BureauCauseOfLoss
			)
		), 'UPDATE',
		'NOCHANGE'
	) AS v_change_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_change_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_change_flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS eff_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS exp_date,
	SYSDATE AS current_date,
	v_change_flag AS change_flag,
	'0' AS expire_snapshot_flag,
	-- *INF*: IIF(v_change_flag='UPDATE', ADD_TO_DATE(SYSDATE,'SS',-1),SYSDATE)
	-- 
	-- -- if we have the scenario where the lookup will be expired then the SYSDATE  minus one second will be the new exp date as SYSDATE will be the new records eff date.
	-- 
	IFF(v_change_flag = 'UPDATE',
		DATEADD(SECOND,- 1,SYSDATE),
		SYSDATE
	) AS lkp_exp_date_OUT
	FROM EXP_Accept_Inputs_Set_Defaults
	LEFT JOIN LKP_Sup_CauseOfLoss
	ON LKP_Sup_CauseOfLoss.CauseOfLoss = EXP_Accept_Inputs_Set_Defaults.cause_of_loss AND LKP_Sup_CauseOfLoss.LineOfBusiness = EXP_Accept_Inputs_Set_Defaults.line_of_business AND LKP_Sup_CauseOfLoss.MajorPeril = EXP_Accept_Inputs_Set_Defaults.major_peril
),
RTR_route_transactions AS (
	SELECT
	lkp_CauseOfLossId AS CauseOfLossId,
	current_snapshot_flag,
	audit_id,
	eff_date,
	exp_date,
	source_system_id AS OUT_source_system_id,
	current_date,
	lkp_CauseOfLossAKID AS CauseOfLossAKID,
	line_of_business,
	major_peril,
	cause_of_loss,
	cause_of_loss_nm AS OUT_cause_of_loss_nm,
	BureauCauseOfLoss,
	change_flag,
	expire_snapshot_flag,
	lkp_exp_date_OUT
	FROM EXP_Detect_Changes
),
RTR_route_transactions_Insert AS (SELECT * FROM RTR_route_transactions WHERE IN(change_flag,'NEW','UPDATE')),
RTR_route_transactions_Update AS (SELECT * FROM RTR_route_transactions WHERE change_flag='UPDATE'),
SEQ_Inserts AS (
	CREATE SEQUENCE SEQ_Inserts
	START = 0
	INCREMENT = 1;
),
EXP_prepare_insert AS (
	SELECT
	current_snapshot_flag,
	audit_id,
	eff_date,
	exp_date,
	OUT_source_system_id AS source_system_id,
	current_date,
	CauseOfLossAKID,
	-- *INF*: IIF(change_flag='NEW',NEXTVAL,CauseOfLossAKID)
	IFF(change_flag = 'NEW',
		NEXTVAL,
		CauseOfLossAKID
	) AS OUT_cause_of_loss_AKID,
	line_of_business,
	major_peril,
	cause_of_loss,
	OUT_cause_of_loss_nm AS cause_of_loss_nm,
	BureauCauseOfLoss,
	change_flag,
	SEQ_Inserts.NEXTVAL
	FROM RTR_route_transactions_Insert
),
UPDTRANS_INSERT AS (
	SELECT
	current_snapshot_flag, 
	audit_id, 
	eff_date, 
	exp_date, 
	source_system_id AS OUT_source_system_id, 
	current_date, 
	OUT_cause_of_loss_AKID AS CauseOfLossAKID, 
	line_of_business, 
	major_peril, 
	cause_of_loss, 
	cause_of_loss_nm, 
	BureauCauseOfLoss, 
	change_flag, 
	NEXTVAL
	FROM EXP_prepare_insert
),
INSERT_first_sup_CauseOfLoss AS (
	INSERT INTO sup_CauseOfLoss
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CauseOfLossAKID, LineOfBusiness, MajorPeril, CauseOfLoss, CauseOfLossName, BureauCauseOfLoss)
	SELECT 
	current_snapshot_flag AS CURRENTSNAPSHOTFLAG, 
	audit_id AS AUDITID, 
	eff_date AS EFFECTIVEDATE, 
	exp_date AS EXPIRATIONDATE, 
	OUT_source_system_id AS SOURCESYSTEMID, 
	current_date AS CREATEDDATE, 
	current_date AS MODIFIEDDATE, 
	CAUSEOFLOSSAKID, 
	line_of_business AS LINEOFBUSINESS, 
	major_peril AS MAJORPERIL, 
	cause_of_loss AS CAUSEOFLOSS, 
	cause_of_loss_nm AS CAUSEOFLOSSNAME, 
	BUREAUCAUSEOFLOSS
	FROM UPDTRANS_INSERT
),
UPDTRANS_UPDATE_EXPIRE AS (
	SELECT
	CauseOfLossId AS CauseOfLossId2, 
	CauseOfLossAKID AS CauseOfLossAKID3, 
	expire_snapshot_flag AS current_snapshot_flag3, 
	lkp_exp_date_OUT AS lkp_exp_date_OUT3, 
	current_date AS current_date3
	FROM RTR_route_transactions_Update
),
EXPIRE_sup_CauseOfLoss AS (
	MERGE INTO sup_CauseOfLoss AS T
	USING UPDTRANS_UPDATE_EXPIRE AS S
	ON T.CauseOfLossId = S.CauseOfLossId2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.current_snapshot_flag3, T.ExpirationDate = S.lkp_exp_date_OUT3, T.ModifiedDate = S.current_date3
),