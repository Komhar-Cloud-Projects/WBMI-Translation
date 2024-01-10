WITH
SQ_NurseAssignment AS (
	SELECT 
	N.NurseAssignmentId, 
	N.EffectiveDate, 
	N.SourceSystemId, 
	N.NurseAssignmentAkId, 
	N.claim_party_ak_id, 
	N.OpenDate, 
	N.ClosedDate 
	
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.NurseAssignment N
	where
	  N.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}'
	AND
	   N.CurrentSnapshotFlag = 1
),
EXP_Src_Values_Default AS (
	SELECT
	EdwNurseAssignmentPkId,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	NurseAssignmentAkId AS IN_NurseAssignmentAkId,
	-- *INF*: iif(isnull(IN_NurseAssignmentAkId), -1, IN_NurseAssignmentAkId)
	IFF(IN_NurseAssignmentAkId IS NULL,
		- 1,
		IN_NurseAssignmentAkId
	) AS v_NurseAssignmentAkId,
	v_NurseAssignmentAkId AS EdwNurseAssignmentAkId,
	claim_party_ak_id AS IN_claim_party_ak_id,
	-- *INF*: iif(isnull(IN_claim_party_ak_id), -1,IN_claim_party_ak_id)
	IFF(IN_claim_party_ak_id IS NULL,
		- 1,
		IN_claim_party_ak_id
	) AS v_claim_party_ak_id,
	v_claim_party_ak_id AS claim_party_ak_id,
	OpenDate AS IN_OpenDate,
	-- *INF*: iif(isnull(IN_OpenDate),to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),IN_OpenDate)
	IFF(IN_OpenDate IS NULL,
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		IN_OpenDate
	) AS v_OpenDate,
	v_OpenDate AS OpenDate,
	ClosedDate AS IN_ClosedDate,
	-- *INF*: iif(isnull(ltrim(rtrim(IN_ClosedDate))),to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'),IN_ClosedDate)
	IFF(ltrim(rtrim(IN_ClosedDate
			)
		) IS NULL,
		to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
		),
		IN_ClosedDate
	) AS v_ClosedDate,
	v_ClosedDate AS ClosedDate
	FROM SQ_NurseAssignment
),
LKP_NurseAssignmentDim AS (
	SELECT
	NurseAssignmentDimId,
	EdwNurseAssignmentAkId
	FROM (
		SELECT
		N.NurseAssignmentDimId as NurseAssignmentDimId, N.EdwNurseAssignmentAkId as EdwNurseAssignmentAkId
		
		 FROM
		 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentDim N
		
		where
		N.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EdwNurseAssignmentAkId ORDER BY NurseAssignmentDimId) = 1
),
LKP_claim_party AS (
	SELECT
	claim_party_full_name,
	claim_party_first_name,
	claim_party_last_name,
	claim_party_mid_name,
	claim_party_name_prfx,
	claim_party_name_sfx,
	claim_party_ak_id
	FROM (
		SELECT
		CP.claim_party_full_name as claim_party_full_name, 
		CP.claim_party_first_name as claim_party_first_name, 
		CP.claim_party_last_name as claim_party_last_name, 
		CP.claim_party_mid_name as claim_party_mid_name, 
		CP.claim_party_name_prfx as claim_party_name_prfx, 
		CP.claim_party_name_sfx as claim_party_name_sfx, 
		CP.claim_party_ak_id as claim_party_ak_id
		
		 FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party CP
		
		where
		CP.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_ak_id ORDER BY claim_party_full_name) = 1
),
EXP_Lkp_Records AS (
	SELECT
	LKP_NurseAssignmentDim.NurseAssignmentDimId,
	LKP_claim_party.claim_party_full_name AS IN_NurseFullName,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_NurseFullName),'N/A',
	-- IS_SPACES(IN_NurseFullName),'N/A',
	-- LENGTH(IN_NurseFullName)=0,'N/A',
	-- LTRIM(RTRIM(IN_NurseFullName)))
	DECODE(TRUE,
		IN_NurseFullName IS NULL, 'N/A',
		LENGTH(IN_NurseFullName)>0 AND TRIM(IN_NurseFullName)='', 'N/A',
		LENGTH(IN_NurseFullName
		) = 0, 'N/A',
		LTRIM(RTRIM(IN_NurseFullName
			)
		)
	) AS NurseFullName,
	LKP_claim_party.claim_party_first_name AS IN_NurseFirstName,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_NurseFirstName),'N/A',
	-- IS_SPACES(IN_NurseFirstName),'N/A',
	-- LENGTH(IN_NurseFirstName)=0,'N/A',
	-- LTRIM(RTRIM(IN_NurseFirstName)))
	DECODE(TRUE,
		IN_NurseFirstName IS NULL, 'N/A',
		LENGTH(IN_NurseFirstName)>0 AND TRIM(IN_NurseFirstName)='', 'N/A',
		LENGTH(IN_NurseFirstName
		) = 0, 'N/A',
		LTRIM(RTRIM(IN_NurseFirstName
			)
		)
	) AS NurseFirstName,
	LKP_claim_party.claim_party_last_name AS IN_NurseLastName,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_NurseLastName),'N/A',
	-- IS_SPACES(IN_NurseLastName),'N/A',
	-- LENGTH(IN_NurseLastName)=0,'N/A',
	-- LTRIM(RTRIM(IN_NurseLastName)))
	DECODE(TRUE,
		IN_NurseLastName IS NULL, 'N/A',
		LENGTH(IN_NurseLastName)>0 AND TRIM(IN_NurseLastName)='', 'N/A',
		LENGTH(IN_NurseLastName
		) = 0, 'N/A',
		LTRIM(RTRIM(IN_NurseLastName
			)
		)
	) AS NurseLastName,
	LKP_claim_party.claim_party_mid_name AS IN_NurseMiddleName,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_NurseMiddleName),'N/A',
	-- IS_SPACES(IN_NurseMiddleName),'N/A',
	-- LENGTH(IN_NurseMiddleName)=0,'N/A',
	-- LTRIM(RTRIM(IN_NurseMiddleName)))
	DECODE(TRUE,
		IN_NurseMiddleName IS NULL, 'N/A',
		LENGTH(IN_NurseMiddleName)>0 AND TRIM(IN_NurseMiddleName)='', 'N/A',
		LENGTH(IN_NurseMiddleName
		) = 0, 'N/A',
		LTRIM(RTRIM(IN_NurseMiddleName
			)
		)
	) AS NurseMiddleName,
	LKP_claim_party.claim_party_name_prfx AS IN_NurseNamePrefix,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_NurseNamePrefix),'N/A',
	-- IS_SPACES(IN_NurseNamePrefix),'N/A',
	-- LENGTH(IN_NurseNamePrefix)=0,'N/A',
	-- LTRIM(RTRIM(IN_NurseNamePrefix)))
	DECODE(TRUE,
		IN_NurseNamePrefix IS NULL, 'N/A',
		LENGTH(IN_NurseNamePrefix)>0 AND TRIM(IN_NurseNamePrefix)='', 'N/A',
		LENGTH(IN_NurseNamePrefix
		) = 0, 'N/A',
		LTRIM(RTRIM(IN_NurseNamePrefix
			)
		)
	) AS NurseNamePrefix,
	LKP_claim_party.claim_party_name_sfx AS IN_NurseNameSuffix,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(IN_NurseNameSuffix),'N/A',
	-- IS_SPACES(IN_NurseNameSuffix),'N/A',
	-- LENGTH(IN_NurseNameSuffix)=0,'N/A',
	-- LTRIM(RTRIM(IN_NurseNameSuffix)))
	DECODE(TRUE,
		IN_NurseNameSuffix IS NULL, 'N/A',
		LENGTH(IN_NurseNameSuffix)>0 AND TRIM(IN_NurseNameSuffix)='', 'N/A',
		LENGTH(IN_NurseNameSuffix
		) = 0, 'N/A',
		LTRIM(RTRIM(IN_NurseNameSuffix
			)
		)
	) AS NurseNameSuffix
	FROM 
	LEFT JOIN LKP_NurseAssignmentDim
	ON LKP_NurseAssignmentDim.EdwNurseAssignmentAkId = EXP_Src_Values_Default.EdwNurseAssignmentAkId
	LEFT JOIN LKP_claim_party
	ON LKP_claim_party.claim_party_ak_id = EXP_Src_Values_Default.claim_party_ak_id
),
RTR_Insert_Update AS (
	SELECT
	EXP_Lkp_Records.NurseAssignmentDimId,
	EXP_Src_Values_Default.CurrentSnapshotFlag,
	EXP_Src_Values_Default.AuditId AS AuditID,
	EXP_Src_Values_Default.EffectiveDate,
	EXP_Src_Values_Default.ExpirationDate,
	EXP_Src_Values_Default.CreatedDate,
	EXP_Src_Values_Default.ModifiedDate,
	EXP_Src_Values_Default.EdwNurseAssignmentPkId,
	EXP_Src_Values_Default.EdwNurseAssignmentAkId,
	EXP_Src_Values_Default.OpenDate,
	EXP_Src_Values_Default.ClosedDate,
	EXP_Lkp_Records.NurseFullName,
	EXP_Lkp_Records.NurseFirstName,
	EXP_Lkp_Records.NurseLastName,
	EXP_Lkp_Records.NurseMiddleName,
	EXP_Lkp_Records.NurseNamePrefix,
	EXP_Lkp_Records.NurseNameSuffix
	FROM EXP_Lkp_Records
	 -- Manually join with EXP_Src_Values_Default
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE isnull(NurseAssignmentDimId)),
RTR_Insert_Update_DEFAULT1 AS (SELECT * FROM RTR_Insert_Update WHERE NOT ( (isnull(NurseAssignmentDimId)) )),
UPD_Update AS (
	SELECT
	NurseAssignmentDimId, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EdwNurseAssignmentPkId, 
	EdwNurseAssignmentAkId, 
	OpenDate, 
	ClosedDate, 
	NurseFullName, 
	NurseFirstName, 
	NurseLastName, 
	NurseMiddleName, 
	NurseNamePrefix, 
	NurseNameSuffix
	FROM RTR_Insert_Update_DEFAULT1
),
NurseAssignmentDim_Update AS (
	MERGE INTO NurseAssignmentDim AS T
	USING UPD_Update AS S
	ON T.NurseAssignmentDimId = S.NurseAssignmentDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.AuditID = S.AuditID, T.EffectiveDate = S.EffectiveDate, T.ExpirationDate = S.ExpirationDate, T.CreatedDate = S.CreatedDate, T.ModifiedDate = S.ModifiedDate, T.EdwNurseAssignmentPkId = S.EdwNurseAssignmentPkId, T.EdwNurseAssignmentAkId = S.EdwNurseAssignmentAkId, T.OpenDate = S.OpenDate, T.ClosedDate = S.ClosedDate, T.NurseFullName = S.NurseFullName, T.NurseFirstName = S.NurseFirstName, T.NurseLastName = S.NurseLastName, T.NurseMiddleName = S.NurseMiddleName, T.NurseNamePrefix = S.NurseNamePrefix, T.NurseNameSuffix = S.NurseNameSuffix
),
UPD_Insert AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	EdwNurseAssignmentPkId, 
	EdwNurseAssignmentAkId, 
	OpenDate, 
	ClosedDate, 
	NurseFullName, 
	NurseFirstName, 
	NurseLastName, 
	NurseMiddleName, 
	NurseNamePrefix, 
	NurseNameSuffix
	FROM RTR_Insert_Update_Insert
),
NurseAssignmentDim_Insert AS (
	INSERT INTO NurseAssignmentDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EdwNurseAssignmentPkId, EdwNurseAssignmentAkId, OpenDate, ClosedDate, NurseFullName, NurseFirstName, NurseLastName, NurseMiddleName, NurseNamePrefix, NurseNameSuffix)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWNURSEASSIGNMENTPKID, 
	EDWNURSEASSIGNMENTAKID, 
	OPENDATE, 
	CLOSEDDATE, 
	NURSEFULLNAME, 
	NURSEFIRSTNAME, 
	NURSELASTNAME, 
	NURSEMIDDLENAME, 
	NURSENAMEPREFIX, 
	NURSENAMESUFFIX
	FROM UPD_Insert
),
SQ_NurseAssignmentDim AS (
	SELECT
	A.NurseAssignmentDimId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.EdwNurseAssignmentAkId 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentDim A
	
	where Exists
	 (
	SELECT 1
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.NurseAssignmentDim B
	
	WHERE
	A.EdwNurseAssignmentAkId = B.EdwNurseAssignmentAkId
	AND
	B.CurrentSnapshotFlag = 1
	
	GROUP BY 
	B.EdwNurseAssignmentAkId
	HAVING
	count(*) > 1
	 )
	
	ORDER BY
	A.EdwNurseAssignmentAkId,
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	NurseAssignmentDimId,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: decode(true,
	-- EdwNurseAssignmentAkId = v_PREV_ROW_EdwNurseAssignmentAkId,
	--  add_to_date(v_PREV_ROW_EffectiveDate,'SS', -1),
	-- orig_ExpirationDate)
	decode(true,
		EdwNurseAssignmentAkId = v_PREV_ROW_EdwNurseAssignmentAkId, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveDate),
		orig_ExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	sysdate AS ModifiedDate,
	EdwNurseAssignmentAkId,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	EdwNurseAssignmentAkId AS v_PREV_ROW_EdwNurseAssignmentAkId
	FROM SQ_NurseAssignmentDim
),
FIL_FirstRowAkId AS (
	SELECT
	NurseAssignmentDimId, 
	CurrentSnapshotFlag, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_NurseAssignmentDim AS (
	SELECT
	NurseAssignmentDimId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
NurseAssignmentDim_Expire AS (
	MERGE INTO NurseAssignmentDim AS T
	USING UPD_NurseAssignmentDim AS S
	ON T.NurseAssignmentDimId = S.NurseAssignmentDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),