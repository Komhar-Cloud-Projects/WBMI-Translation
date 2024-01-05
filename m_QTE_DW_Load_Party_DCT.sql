WITH
SQ_WorkDCTPolicy AS (
	SELECT
		SessionId,
		Name,
		FirstName,
		LastName,
		MiddleName,
		Title,
		CustomerNum,
		SICCode,
		SICCodeDesc,
		NAICSCode,
		NAICSCodeDesc,
		InceptionDate,
		EntityType,
		Association,
		DoingBusinessAs,
		PhoneNumber
	FROM WorkDCTPolicy
	ON WorkDCTPolicy.QuoteActionTimeStamp is not null
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_Remove_Duplicates AS (
	SELECT
	SessionId AS i_SessionId, 
	Name AS i_Name, 
	FirstName AS i_FirstName, 
	LastName AS i_LastName, 
	MiddleName AS i_MiddleName, 
	Title AS i_Title, 
	CustomerNum, 
	SICCode AS i_SICCode, 
	SICCodeDesc AS i_SICCodeDesc, 
	NAICSCode AS i_NAICSCode, 
	NAICSCodeDesc AS i_NAICSCodeDesc, 
	InceptionDate AS i_InceptionDate, 
	EntityType AS i_EntityType, 
	Association AS i_Association, 
	DoingBusinessAs AS i_DoingBusinessAs, 
	PhoneNumber AS PrimaryPhoneNumber, 
	i_Name AS o_Name, 
	i_FirstName AS o_FirstName, 
	i_LastName AS o_LastName, 
	i_MiddleName AS o_MiddleName, 
	i_Title AS o_PrefixTitleCode, 
	LTRIM(RTRIM(CustomerNum)) AS o_PartyNumber, 
	i_SICCode AS o_SicCode, 
	i_SICCodeDesc AS o_SicCodeTitle, 
	i_NAICSCode AS o_NaicsCode, 
	i_NAICSCodeDesc AS o_NaicsCodeTitle, 
	i_InceptionDate AS o_OriginalPolicyInceptionDate, 
	i_EntityType AS o_EntityType, 
	i_Association AS o_Association, 
	i_DoingBusinessAs AS o_DoingBusinessAs
	FROM SQ_WorkDCTPolicy
	GROUP BY o_PartyNumber
),
LKP_Party AS (
	SELECT
	PartyAKId,
	PrimaryPhoneNumber,
	OriginalPolicyInceptionDate,
	Name,
	DoingBusinessAs,
	LegalEntityTypeCode,
	SicCode,
	SicCodeTitle,
	NaicsCode,
	NaicsCodeTitle,
	PrefixTitleCode,
	FirstName,
	MiddleName,
	LastName,
	PartyNumber
	FROM (
		SELECT 
			PartyAKId,
			PrimaryPhoneNumber,
			OriginalPolicyInceptionDate,
			Name,
			DoingBusinessAs,
			LegalEntityTypeCode,
			SicCode,
			SicCodeTitle,
			NaicsCode,
			NaicsCodeTitle,
			PrefixTitleCode,
			FirstName,
			MiddleName,
			LastName,
			PartyNumber
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Party
		WHERE CurrentSnapshotFlag='1' AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where LTRIM(RTRIM(w.CustomerNum))=Party.PartyNumber)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PartyNumber ORDER BY PartyAKId) = 1
),
EXP_Detect_Change AS (
	SELECT
	LKP_Party.PartyAKId AS lkp_PartyAKId,
	LKP_Party.PrimaryPhoneNumber AS lkp_PrimaryPhoneNumber,
	LKP_Party.OriginalPolicyInceptionDate AS lkp_OriginalPolicyInceptionDate,
	LKP_Party.Name AS lkp_Name,
	LKP_Party.DoingBusinessAs AS lkp_DoingBusinessAs,
	LKP_Party.LegalEntityTypeCode AS lkp_LegalEntityTypeCode,
	LKP_Party.SicCode AS lkp_SicCode,
	LKP_Party.SicCodeTitle AS lkp_SicCodeTitle,
	LKP_Party.NaicsCode AS lkp_NaicsCode,
	LKP_Party.NaicsCodeTitle AS lkp_NaicsCodeTitle,
	LKP_Party.PrefixTitleCode AS lkp_PrefixTitleCode,
	LKP_Party.FirstName AS lkp_FirstName,
	LKP_Party.MiddleName AS lkp_MiddleName,
	LKP_Party.LastName AS lkp_LastName,
	AGG_Remove_Duplicates.PrimaryPhoneNumber AS i_PrimaryPhoneNumber,
	AGG_Remove_Duplicates.o_Name AS i_Name,
	AGG_Remove_Duplicates.o_FirstName AS i_FirstName,
	AGG_Remove_Duplicates.o_LastName AS i_LastName,
	AGG_Remove_Duplicates.o_MiddleName AS i_MiddleName,
	AGG_Remove_Duplicates.o_PrefixTitleCode AS i_PrefixTitleCode,
	AGG_Remove_Duplicates.o_PartyNumber AS i_PartyNumber,
	AGG_Remove_Duplicates.o_SicCode AS i_SicCode,
	AGG_Remove_Duplicates.o_SicCodeTitle AS i_SicCodeTitle,
	AGG_Remove_Duplicates.o_NaicsCode AS i_NaicsCode,
	AGG_Remove_Duplicates.o_NaicsCodeTitle AS i_NaicsCodeTitle,
	AGG_Remove_Duplicates.o_OriginalPolicyInceptionDate AS i_OriginalPolicyInceptionDate,
	AGG_Remove_Duplicates.o_EntityType AS i_EntityType,
	AGG_Remove_Duplicates.o_Association AS i_AssociationCode,
	AGG_Remove_Duplicates.o_DoingBusinessAs AS i_DoingBusinessAs,
	'TBD' AS i_PrimaryEmailAddress,
	'N/A' AS i_LegalName,
	'N/A' AS i_LegalEntityTypeDescription,
	'N/A' AS i_OrganizationLegalStatusName,
	'N/A' AS i_OrganizationLegalStatusAbbreviation,
	'N/A' AS i_PrimaryOperationsDescription,
	'N/A' AS i_Suffix,
	'TBD' AS i_SocialSecurityNumber,
	-- *INF*: IIF(ISNULL(i_PrimaryPhoneNumber) or IS_SPACES(i_PrimaryPhoneNumber) or LENGTH(i_PrimaryPhoneNumber)=0,'N/A',LTRIM(RTRIM(i_PrimaryPhoneNumber)))
	IFF(i_PrimaryPhoneNumber IS NULL OR IS_SPACES(i_PrimaryPhoneNumber) OR LENGTH(i_PrimaryPhoneNumber) = 0, 'N/A', LTRIM(RTRIM(i_PrimaryPhoneNumber))) AS v_PrimaryPhoneNumber,
	-- *INF*: IIF(ISNULL(i_Name) or IS_SPACES(i_Name) or LENGTH(i_Name)=0,'N/A',LTRIM(RTRIM(i_Name)))
	IFF(i_Name IS NULL OR IS_SPACES(i_Name) OR LENGTH(i_Name) = 0, 'N/A', LTRIM(RTRIM(i_Name))) AS v_Name,
	-- *INF*: IIF(ISNULL(i_DoingBusinessAs) or IS_SPACES(i_DoingBusinessAs) or LENGTH(i_DoingBusinessAs)=0,'N/A',LTRIM(RTRIM(i_DoingBusinessAs)))
	IFF(i_DoingBusinessAs IS NULL OR IS_SPACES(i_DoingBusinessAs) OR LENGTH(i_DoingBusinessAs) = 0, 'N/A', LTRIM(RTRIM(i_DoingBusinessAs))) AS v_DoingBusinessAs,
	-- *INF*: IIF(ISNULL(i_EntityType) OR IS_SPACES(i_EntityType) OR LENGTH(i_EntityType)=0,'N/A',LTRIM(RTRIM(i_EntityType)))
	IFF(i_EntityType IS NULL OR IS_SPACES(i_EntityType) OR LENGTH(i_EntityType) = 0, 'N/A', LTRIM(RTRIM(i_EntityType))) AS v_LegalEntityTypeCode,
	-- *INF*: IIF(ISNULL(i_SicCode) OR IS_SPACES(i_SicCode) OR LENGTH(i_SicCode)=0,'N/A',LTRIM(RTRIM(i_SicCode)))
	IFF(i_SicCode IS NULL OR IS_SPACES(i_SicCode) OR LENGTH(i_SicCode) = 0, 'N/A', LTRIM(RTRIM(i_SicCode))) AS v_SicCode,
	-- *INF*: IIF(ISNULL(i_SicCodeTitle) OR IS_SPACES(i_SicCodeTitle) OR LENGTH(i_SicCodeTitle)=0,'N/A',LTRIM(RTRIM(i_SicCodeTitle)))
	IFF(i_SicCodeTitle IS NULL OR IS_SPACES(i_SicCodeTitle) OR LENGTH(i_SicCodeTitle) = 0, 'N/A', LTRIM(RTRIM(i_SicCodeTitle))) AS v_SicCodeTitle,
	-- *INF*: IIF(ISNULL(i_NaicsCode) OR IS_SPACES(i_NaicsCode) OR LENGTH(i_NaicsCode)=0,'N/A',LTRIM(RTRIM(i_NaicsCode)))
	IFF(i_NaicsCode IS NULL OR IS_SPACES(i_NaicsCode) OR LENGTH(i_NaicsCode) = 0, 'N/A', LTRIM(RTRIM(i_NaicsCode))) AS v_NaicsCode,
	-- *INF*: IIF(ISNULL(i_NaicsCodeTitle) OR IS_SPACES(i_NaicsCodeTitle) OR LENGTH(i_NaicsCodeTitle)=0,'N/A',LTRIM(RTRIM(i_NaicsCodeTitle)))
	IFF(i_NaicsCodeTitle IS NULL OR IS_SPACES(i_NaicsCodeTitle) OR LENGTH(i_NaicsCodeTitle) = 0, 'N/A', LTRIM(RTRIM(i_NaicsCodeTitle))) AS v_NaicsCodeTitle,
	-- *INF*: IIF(ISNULL(i_PrefixTitleCode) OR IS_SPACES(i_PrefixTitleCode) OR LENGTH(i_PrefixTitleCode)=0,'N/A',LTRIM(RTRIM(i_PrefixTitleCode)))
	IFF(i_PrefixTitleCode IS NULL OR IS_SPACES(i_PrefixTitleCode) OR LENGTH(i_PrefixTitleCode) = 0, 'N/A', LTRIM(RTRIM(i_PrefixTitleCode))) AS v_PrefixTitleCode,
	-- *INF*: IIF(ISNULL(i_FirstName) OR IS_SPACES(i_FirstName) OR LENGTH(i_FirstName)=0,'N/A',LTRIM(RTRIM(i_FirstName)))
	IFF(i_FirstName IS NULL OR IS_SPACES(i_FirstName) OR LENGTH(i_FirstName) = 0, 'N/A', LTRIM(RTRIM(i_FirstName))) AS v_FirstName,
	-- *INF*: IIF(ISNULL(i_MiddleName) OR IS_SPACES(i_MiddleName) OR LENGTH(i_MiddleName)=0,'N/A',LTRIM(RTRIM(i_MiddleName)))
	IFF(i_MiddleName IS NULL OR IS_SPACES(i_MiddleName) OR LENGTH(i_MiddleName) = 0, 'N/A', LTRIM(RTRIM(i_MiddleName))) AS v_MiddleName,
	-- *INF*: IIF(ISNULL(i_LastName) OR IS_SPACES(i_LastName) OR LENGTH(i_LastName)=0,'N/A',LTRIM(RTRIM(i_LastName)))
	IFF(i_LastName IS NULL OR IS_SPACES(i_LastName) OR LENGTH(i_LastName) = 0, 'N/A', LTRIM(RTRIM(i_LastName))) AS v_LastName,
	-- *INF*: IIF(ISNULL(i_OriginalPolicyInceptionDate), TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'), i_OriginalPolicyInceptionDate)
	IFF(i_OriginalPolicyInceptionDate IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), i_OriginalPolicyInceptionDate) AS v_OriginalPolicyInceptionDate,
	-- *INF*: IIF(ISNULL(i_AssociationCode) OR IS_SPACES(i_AssociationCode) OR LENGTH(i_AssociationCode)=0,'N/A',LTRIM(RTRIM(i_AssociationCode)))
	IFF(i_AssociationCode IS NULL OR IS_SPACES(i_AssociationCode) OR LENGTH(i_AssociationCode) = 0, 'N/A', LTRIM(RTRIM(i_AssociationCode))) AS v_AssociationCode,
	-- *INF*: IIF(ISNULL(lkp_PartyAKId), 'NEW', 
	-- IIF(
	-- lkp_PrimaryPhoneNumber != v_PrimaryPhoneNumber OR
	-- lkp_OriginalPolicyInceptionDate != v_OriginalPolicyInceptionDate OR
	-- lkp_Name != v_Name OR	
	-- lkp_DoingBusinessAs != v_DoingBusinessAs OR 
	-- lkp_LegalEntityTypeCode != v_LegalEntityTypeCode OR 
	-- lkp_SicCode != v_SicCode OR
	-- lkp_SicCodeTitle != v_SicCodeTitle OR
	-- lkp_NaicsCode != v_NaicsCode OR
	-- lkp_NaicsCodeTitle != v_NaicsCodeTitle OR
	-- lkp_PrefixTitleCode != v_PrefixTitleCode OR
	-- lkp_FirstName != v_FirstName OR
	-- lkp_MiddleName != v_MiddleName OR
	-- lkp_LastName != v_LastName,
	-- 'UPDATE', 'NOCHANGE'))
	IFF(lkp_PartyAKId IS NULL, 'NEW', IFF(lkp_PrimaryPhoneNumber != v_PrimaryPhoneNumber OR lkp_OriginalPolicyInceptionDate != v_OriginalPolicyInceptionDate OR lkp_Name != v_Name OR lkp_DoingBusinessAs != v_DoingBusinessAs OR lkp_LegalEntityTypeCode != v_LegalEntityTypeCode OR lkp_SicCode != v_SicCode OR lkp_SicCodeTitle != v_SicCodeTitle OR lkp_NaicsCode != v_NaicsCode OR lkp_NaicsCodeTitle != v_NaicsCodeTitle OR lkp_PrefixTitleCode != v_PrefixTitleCode OR lkp_FirstName != v_FirstName OR lkp_MiddleName != v_MiddleName OR lkp_LastName != v_LastName, 'UPDATE', 'NOCHANGE')) AS o_ChangeFlag,
	lkp_PartyAKId AS o_PartyAKId,
	-- *INF*: IIF(ISNULL(i_PartyNumber),'N/A',i_PartyNumber)
	IFF(i_PartyNumber IS NULL, 'N/A', i_PartyNumber) AS o_PartyNumber,
	v_PrimaryPhoneNumber AS o_PrimaryPhoneNumber,
	v_OriginalPolicyInceptionDate AS o_OriginalPolicyInceptionDate,
	'Not Deleted' AS o_DeletedIndicator,
	'Active' AS o_StatusCode,
	v_Name AS o_Name,
	v_DoingBusinessAs AS o_DoingBusinessAs,
	v_LegalEntityTypeCode AS o_LegalEntityTypeCode,
	v_SicCode AS o_SicCode,
	v_SicCodeTitle AS o_SicCodeTitle,
	v_NaicsCode AS o_NaicsCode,
	v_NaicsCodeTitle AS o_NaicsCodeTitle,
	v_PrefixTitleCode AS o_PrefixTitleCode,
	v_FirstName AS o_FirstName,
	v_MiddleName AS o_MiddleName,
	v_LastName AS o_LastName,
	v_AssociationCode AS o_AssociationCode
	FROM AGG_Remove_Duplicates
	LEFT JOIN LKP_Party
	ON LKP_Party.PartyNumber = AGG_Remove_Duplicates.o_PartyNumber
),
LKP_Association AS (
	SELECT
	AssociationId,
	AssociationCode
	FROM (
		SELECT 
			AssociationId,
			AssociationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Association
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociationCode ORDER BY AssociationId) = 1
),
FIL_Insert AS (
	SELECT
	EXP_Detect_Change.o_ChangeFlag AS ChangeFlag, 
	EXP_Detect_Change.o_PartyAKId AS PartyAKId, 
	EXP_Detect_Change.o_PartyNumber AS PartyNumber, 
	EXP_Detect_Change.o_PrimaryPhoneNumber AS PrimaryPhoneNumber, 
	EXP_Detect_Change.o_OriginalPolicyInceptionDate AS OriginalPolicyInceptionDate, 
	EXP_Detect_Change.o_DeletedIndicator AS DeletedIndicator, 
	EXP_Detect_Change.o_StatusCode AS StatusCode, 
	EXP_Detect_Change.o_Name AS Name, 
	EXP_Detect_Change.o_DoingBusinessAs AS DoingBusinessAs, 
	EXP_Detect_Change.o_LegalEntityTypeCode AS LegalEntityTypeCode, 
	EXP_Detect_Change.o_SicCode AS SicCode, 
	EXP_Detect_Change.o_SicCodeTitle AS SicCodeTitle, 
	EXP_Detect_Change.o_NaicsCode AS NaicsCode, 
	EXP_Detect_Change.o_NaicsCodeTitle AS NaicsCodeTitle, 
	EXP_Detect_Change.o_PrefixTitleCode AS PrefixTitleCode, 
	EXP_Detect_Change.o_FirstName AS FirstName, 
	EXP_Detect_Change.o_MiddleName AS MiddleName, 
	EXP_Detect_Change.o_LastName AS LastName, 
	LKP_Association.AssociationId, 
	EXP_Detect_Change.o_AssociationCode AS AssociationCode
	FROM EXP_Detect_Change
	LEFT JOIN LKP_Association
	ON LKP_Association.AssociationCode = EXP_Detect_Change.o_AssociationCode
	WHERE (ChangeFlag='NEW' OR ChangeFlag='UPDATE')
),
SEQ_Party_AK_ID AS (
	CREATE SEQUENCE SEQ_Party_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK_ID AS (
	SELECT
	SEQ_Party_AK_ID.NEXTVAL,
	ChangeFlag AS i_ChangeFlag,
	PartyAKId AS i_PartyAKId,
	PartyNumber AS i_PartyNumber,
	PrimaryPhoneNumber AS i_PrimaryPhoneNumber,
	OriginalPolicyInceptionDate AS i_OriginalPolicyInceptionDate,
	DeletedIndicator AS i_DeletedIndicator,
	StatusCode AS i_StatusCode,
	Name AS i_Name,
	DoingBusinessAs AS i_DoingBusinessAs,
	LegalEntityTypeCode AS i_LegalEntityTypeCode,
	SicCode AS i_SicCode,
	SicCodeTitle AS i_SicCodeTitle,
	NaicsCode AS i_NaicsCode,
	NaicsCodeTitle AS i_NaicsCodeTitle,
	PrefixTitleCode AS i_PrefixTitleCode,
	FirstName AS i_FirstName,
	MiddleName AS i_MiddleName,
	LastName AS i_LastName,
	AssociationId AS i_AssociationId,
	AssociationCode AS i_AssociationCode,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: IIF(i_ChangeFlag='NEW',
	-- 	TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(i_ChangeFlag = 'NEW', TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(ISNULL(i_PartyAKId),NEXTVAL,i_PartyAKId)
	IFF(i_PartyAKId IS NULL, NEXTVAL, i_PartyAKId) AS o_PartyAKId,
	i_PartyNumber AS o_PartyNumber,
	i_PrimaryPhoneNumber AS o_PrimaryPhoneNumber,
	i_OriginalPolicyInceptionDate AS o_OriginalPolicyInceptionDate,
	i_DeletedIndicator AS o_DeletedIndicator,
	i_StatusCode AS o_StatusCode,
	i_Name AS o_Name,
	i_DoingBusinessAs AS o_DoingBusinessAs,
	i_LegalEntityTypeCode AS o_LegalEntityTypeCode,
	i_SicCode AS o_SicCode,
	i_SicCodeTitle AS o_SicCodeTitle,
	i_NaicsCode AS o_NaicsCode,
	i_NaicsCodeTitle AS o_NaicsCodeTitle,
	i_PrefixTitleCode AS o_PrefixTitleCode,
	i_FirstName AS o_FirstName,
	i_MiddleName AS o_MiddleName,
	i_LastName AS o_LastName,
	-- *INF*: IIF(ISNULL(i_AssociationId),-1,i_AssociationId)
	IFF(i_AssociationId IS NULL, - 1, i_AssociationId) AS o_AssociationId,
	i_AssociationCode AS o_AssociationCode
	FROM FIL_Insert
),
TGT_Party_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Party
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PartyAKId, PartyNumber, PrimaryPhoneNumber, OriginalPolicyInceptionDate, DeletedIndicator, StatusCode, Name, DoingBusinessAs, LegalEntityTypeCode, SICCode, SICCodeTitle, NAICSCode, NAICSCodeTitle, PrefixTitleCode, FirstName, MiddleName, LastName, AssociationId, AssociationCode)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_PartyAKId AS PARTYAKID, 
	o_PartyNumber AS PARTYNUMBER, 
	o_PrimaryPhoneNumber AS PRIMARYPHONENUMBER, 
	o_OriginalPolicyInceptionDate AS ORIGINALPOLICYINCEPTIONDATE, 
	o_DeletedIndicator AS DELETEDINDICATOR, 
	o_StatusCode AS STATUSCODE, 
	o_Name AS NAME, 
	o_DoingBusinessAs AS DOINGBUSINESSAS, 
	o_LegalEntityTypeCode AS LEGALENTITYTYPECODE, 
	o_SicCode AS SICCODE, 
	o_SicCodeTitle AS SICCODETITLE, 
	o_NaicsCode AS NAICSCODE, 
	o_NaicsCodeTitle AS NAICSCODETITLE, 
	o_PrefixTitleCode AS PREFIXTITLECODE, 
	o_FirstName AS FIRSTNAME, 
	o_MiddleName AS MIDDLENAME, 
	o_LastName AS LASTNAME, 
	o_AssociationId AS ASSOCIATIONID, 
	o_AssociationCode AS ASSOCIATIONCODE
	FROM EXP_Determine_AK_ID
),
SQ_Party AS (
	SELECT 
		PartyId,
		EffectiveDate, 
		ExpirationDate,
		PartyAKId
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.Party a
	WHERE  EXISTS
		 (SELECT 1
		 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Party b 
		   WHERE CurrentSnapshotFlag = 1 and SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		    and a.PartyAKId= b.PartyAKId
	GROUP BY  PartyAKId  HAVING count(*) > 1)
	ORDER BY  PartyAKId ,EffectiveDate  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	PartyId AS i_PartyId,
	EffectiveDate AS i_eff_from_date,
	ExpirationDate AS i_orig_eff_to_date,
	PartyAKId AS i_PartyAKID,
	-- *INF*: DECODE(TRUE,
	-- i_PartyAKID = v_PrevPartyAKID ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),i_orig_eff_to_date)
	DECODE(TRUE,
	i_PartyAKID = v_PrevPartyAKID, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	i_orig_eff_to_date) AS v_eff_to_date,
	i_PartyAKID AS v_PrevPartyAKID,
	i_eff_from_date AS v_prev_eff_from_date,
	i_PartyId AS o_PartyId,
	i_orig_eff_to_date AS o_orig_eff_to_date,
	0 AS o_crrnt_snpsht_flag,
	v_eff_to_date AS o_eff_to_date,
	SYSDATE AS o_modified_date
	FROM SQ_Party
),
FIL_FirstRowInAKGroup AS (
	SELECT
	o_PartyId AS PartyId, 
	o_orig_eff_to_date AS orig_eff_to_date, 
	o_crrnt_snpsht_flag AS crrnt_snpsht_flag, 
	o_eff_to_date AS eff_to_date, 
	o_modified_date AS modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Party AS (
	SELECT
	PartyId, 
	crrnt_snpsht_flag, 
	eff_to_date, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_Party_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.Party AS T
	USING UPD_Party AS S
	ON T.PartyId = S.PartyId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.crrnt_snpsht_flag, T.ExpirationDate = S.eff_to_date, T.ModifiedDate = S.modified_date
),