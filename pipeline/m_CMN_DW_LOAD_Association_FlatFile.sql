WITH
SQ_Association AS (

-- TODO Manual --

),
LKP_Association AS (
	SELECT
	AssociationId,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	AssociationAKId,
	AssociationCode,
	AssociationDescription
	FROM (
		SELECT 
			AssociationId,
			CurrentSnapshotFlag,
			AuditId,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			CreatedDate,
			ModifiedDate,
			AssociationAKId,
			AssociationCode,
			AssociationDescription
		FROM Association
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociationCode ORDER BY AssociationId) = 1
),
EXPTRANS AS (
	SELECT
	SQ_Association.EffectiveDate,
	SQ_Association.ExpirationDate,
	SQ_Association.AssociationCode,
	SQ_Association.AssociationDescription,
	LKP_Association.AssociationCode AS LKP_AssociationCode,
	LKP_Association.AssociationId,
	LKP_Association.AssociationDescription AS LKP_AssociationDescription,
	-- *INF*: decode(True,isnull(LKP_AssociationCode),1,not isnull(LKP_AssociationCode) and AssociationDescription<>LKP_AssociationDescription,2,0)
	-- --iif(isnull(LKP_AssociationCode),1,0)
	decode(True,
		LKP_AssociationCode IS NULL, 1,
		LKP_AssociationCode IS NULL 
		AND AssociationDescription <> LKP_AssociationDescripNOT tion, 2,
		0
	) AS V_Change_Flag,
	V_Change_Flag AS O_Change_Flag
	FROM SQ_Association
	LEFT JOIN LKP_Association
	ON LKP_Association.AssociationCode = SQ_Association.AssociationCode
),
RTRTRANS AS (
	SELECT
	EffectiveDate,
	ExpirationDate,
	AssociationCode,
	AssociationDescription,
	O_Change_Flag,
	AssociationCode AS AssociationCode4,
	AssociationId
	FROM EXPTRANS
),
RTRTRANS_Insert AS (SELECT * FROM RTRTRANS WHERE O_Change_Flag=1),
RTRTRANS_Update AS (SELECT * FROM RTRTRANS WHERE O_Change_Flag=2),
SEQ_AssociationAkid AS (
	CREATE SEQUENCE SEQ_AssociationAkid
	START = 0
	INCREMENT = 1;
),
EXP_Insert AS (
	SELECT
	1 AS CurrentSnapshotFlag,
	EffectiveDate AS i_EffectiveDate,
	-- *INF*: TO_DATE(substr(i_EffectiveDate,1,19), 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(substr(i_EffectiveDate, 1, 19
		), 'YYYY-MM-DD HH24:MI:SS'
	) AS o_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	-- *INF*: TO_DATE(substr(i_ExpirationDate,1,19), 'YYYY-MM-DD HH24:MI:SS')
	-- 
	-- --TO_DATE(i_ExpirationDate, 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(substr(i_ExpirationDate, 1, 19
		), 'YYYY-MM-DD HH24:MI:SS'
	) AS O_ExpirationDate,
	AssociationCode,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	-- *INF*: SESSSTARTTIME
	-- --substr(i_ExpirationDate,1,19)
	SESSSTARTTIME AS ModifiedDate,
	AssociationDescription,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SEQ_AssociationAkid.NEXTVAL
	FROM RTRTRANS_Insert
),
Trg_Association_Insert AS (
	INSERT INTO Association
	(ModifiedUserId, ModifiedDate, EffectiveDate, ExpirationDate, AssociationAKId, AssociationCode, AssociationDescription)
	SELECT 
	SourceSystemId AS MODIFIEDUSERID, 
	MODIFIEDDATE, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	O_ExpirationDate AS EXPIRATIONDATE, 
	NEXTVAL AS ASSOCIATIONAKID, 
	ASSOCIATIONCODE, 
	ASSOCIATIONDESCRIPTION
	FROM EXP_Insert
),
EXP_Update AS (
	SELECT
	EffectiveDate AS i_EffectiveDate3,
	-- *INF*: TO_DATE(substr(i_EffectiveDate3,1,19), 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(substr(i_EffectiveDate3, 1, 19
		), 'YYYY-MM-DD HH24:MI:SS'
	) AS O_EffectiveDate3,
	CURRENT_TIMESTAMP AS ModifiedDate,
	ExpirationDate AS i_ExpirationDate3,
	-- *INF*: TO_DATE(substr(i_ExpirationDate3,1,19), 'YYYY-MM-DD HH24:MI:SS')
	TO_DATE(substr(i_ExpirationDate3, 1, 19
		), 'YYYY-MM-DD HH24:MI:SS'
	) AS O_ExpirationDate3,
	AssociationCode AS AssociationCode3,
	AssociationDescription AS AssociationDescription3,
	AssociationCode4 AS AssociationCode43,
	AssociationId AS AssociationId3,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM RTRTRANS_Update
),
UPD_Update AS (
	SELECT
	O_EffectiveDate3 AS EffectiveDate3, 
	O_ExpirationDate3 AS ExpirationDate3, 
	AssociationCode3, 
	AssociationDescription3, 
	AssociationId3, 
	ModifiedDate, 
	SourceSystemId
	FROM EXP_Update
),
Trg_Association_Update AS (
	MERGE INTO Association AS T
	USING UPD_Update AS S
	ON T.AssociationId = S.AssociationId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedUserId = S.SourceSystemId, T.ModifiedDate = S.ModifiedDate, T.EffectiveDate = S.EffectiveDate3, T.ExpirationDate = S.ExpirationDate3, T.AssociationDescription = S.AssociationDescription3
),