WITH
SQ_Party AS (
	SELECT
		PartyId,
		CreatedDate,
		PartyAKId,
		PartyNumber,
		PrimaryPhoneNumber,
		OriginalPolicyInceptionDate,
		DeletedIndicator,
		StatusCode,
		Name,
		DoingBusinessAs,
		LegalEntityTypeCode,
		SICCode AS SicCode,
		SICCodeTitle AS SicCodeTitle,
		NAICSCode AS NaicsCode,
		NAICSCodeTitle AS NaicsCodeTitle,
		PrefixTitleCode,
		FirstName,
		MiddleName,
		LastName,
		AssociationCode
	FROM Party
	WHERE CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' @{pipeline().parameters.WHERE_CLAUSE}
),
LKP_Association AS (
	SELECT
	AssociationDescription,
	AssociationCode
	FROM (
		SELECT 
			AssociationDescription,
			AssociationCode
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Association
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AssociationCode ORDER BY AssociationDescription) = 1
),
LKP_ExistingPartyDim AS (
	SELECT
	EDWPartyPKID,
	EDWPartyAKID
	FROM (
		SELECT 
			EDWPartyPKID,
			EDWPartyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWPartyAKID ORDER BY EDWPartyPKID) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_ExistingPartyDim.EDWPartyPKID AS lkp_EDWPartyPKID,
	SQ_Party.PartyId AS i_PartyId,
	SQ_Party.PartyAKId AS i_PartyAKId,
	SQ_Party.PartyNumber AS i_PartyNumber,
	SQ_Party.PrimaryPhoneNumber AS i_PrimaryPhoneNumber,
	SQ_Party.OriginalPolicyInceptionDate AS i_OriginalPolicyInceptionDate,
	SQ_Party.DeletedIndicator AS i_DeletedIndicator,
	SQ_Party.StatusCode AS i_StatusCode,
	SQ_Party.Name AS i_Name,
	SQ_Party.DoingBusinessAs AS i_DoingBusinessAs,
	SQ_Party.LegalEntityTypeCode AS i_LegalEntityTypeCode,
	SQ_Party.SicCode AS i_SicCode,
	SQ_Party.SicCodeTitle AS i_SicCodeTitle,
	SQ_Party.NaicsCode AS i_NaicsCode,
	SQ_Party.NaicsCodeTitle AS i_NaicsCodeTitle,
	SQ_Party.PrefixTitleCode AS i_PrefixTitleCode,
	SQ_Party.FirstName AS i_FirstName,
	SQ_Party.MiddleName AS i_MiddleName,
	SQ_Party.LastName AS i_LastName,
	SQ_Party.AssociationCode AS i_AssociationCode,
	LKP_Association.AssociationDescription AS i_AssociationDescription,
	-- *INF*: IIF(ISNULL(lkp_EDWPartyPKID), 'NEW', 'NOCHANGE')
	IFF(lkp_EDWPartyPKID IS NULL, 'NEW', 'NOCHANGE') AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- 	TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	i_PartyId AS PartyId,
	-- *INF*: IIF( ISNULL(i_PartyNumber ), 'N/A', i_PartyNumber )
	IFF(i_PartyNumber IS NULL, 'N/A', i_PartyNumber) AS PartyNumber,
	-- *INF*: IIF( ISNULL(i_PartyAKId), -1, i_PartyAKId)
	IFF(i_PartyAKId IS NULL, - 1, i_PartyAKId) AS PartyAKId,
	-- *INF*: IIF( ISNULL(i_PrimaryPhoneNumber ), 'N/A', i_PrimaryPhoneNumber )
	IFF(i_PrimaryPhoneNumber IS NULL, 'N/A', i_PrimaryPhoneNumber) AS PrimaryPhoneNumber,
	-- *INF*: IIF(ISNULL(i_OriginalPolicyInceptionDate), TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'), i_OriginalPolicyInceptionDate)
	IFF(
	    i_OriginalPolicyInceptionDate IS NULL,
	    TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    i_OriginalPolicyInceptionDate
	) AS OriginalPolicyInceptionDate,
	-- *INF*: IIF(ISNULL(i_DeletedIndicator), 'N/A', i_DeletedIndicator)
	IFF(i_DeletedIndicator IS NULL, 'N/A', i_DeletedIndicator) AS DeletedIndicator,
	-- *INF*: IIF(ISNULL(i_StatusCode), 'N/A', i_StatusCode)
	IFF(i_StatusCode IS NULL, 'N/A', i_StatusCode) AS StatusCode,
	-- *INF*: IIF(ISNULL(i_Name), 'N/A', i_Name)
	IFF(i_Name IS NULL, 'N/A', i_Name) AS Name,
	-- *INF*: IIF(ISNULL(i_DoingBusinessAs), 'N/A', i_DoingBusinessAs)
	IFF(i_DoingBusinessAs IS NULL, 'N/A', i_DoingBusinessAs) AS DoingBusinessAs,
	-- *INF*: IIF(ISNULL(i_LegalEntityTypeCode), 'N/A', i_LegalEntityTypeCode)
	IFF(i_LegalEntityTypeCode IS NULL, 'N/A', i_LegalEntityTypeCode) AS LegalEntityTypeCode,
	-- *INF*: IIF(ISNULL(i_SicCode), 'N/A', i_SicCode)
	IFF(i_SicCode IS NULL, 'N/A', i_SicCode) AS SicCode,
	-- *INF*: IIF(ISNULL(i_SicCodeTitle), 'N/A', i_SicCodeTitle)
	IFF(i_SicCodeTitle IS NULL, 'N/A', i_SicCodeTitle) AS SicCodeTitle,
	-- *INF*: IIF(ISNULL(i_NaicsCode), 'N/A', i_NaicsCode)
	IFF(i_NaicsCode IS NULL, 'N/A', i_NaicsCode) AS NaicsCode,
	-- *INF*: IIF(ISNULL(i_NaicsCodeTitle), 'N/A', i_NaicsCodeTitle)
	IFF(i_NaicsCodeTitle IS NULL, 'N/A', i_NaicsCodeTitle) AS NaicsCodeTitle,
	-- *INF*: IIF(ISNULL(i_PrefixTitleCode), 'N/A', i_PrefixTitleCode)
	IFF(i_PrefixTitleCode IS NULL, 'N/A', i_PrefixTitleCode) AS PrefixTitleCode,
	-- *INF*: IIF(ISNULL(i_FirstName), 'N/A', i_FirstName)
	IFF(i_FirstName IS NULL, 'N/A', i_FirstName) AS FirstName,
	-- *INF*: IIF(ISNULL(i_MiddleName), 'N/A', i_MiddleName)
	IFF(i_MiddleName IS NULL, 'N/A', i_MiddleName) AS MiddleName,
	-- *INF*: IIF(ISNULL(i_LastName), 'N/A', i_LastName)
	IFF(i_LastName IS NULL, 'N/A', i_LastName) AS LastName,
	-- *INF*: IIF(  ISNULL(i_AssociationCode), 'N/A', i_AssociationCode)
	IFF(i_AssociationCode IS NULL, 'N/A', i_AssociationCode) AS AssociationCode,
	-- *INF*: IIF(  ISNULL(i_AssociationDescription ), 'N/A', i_AssociationDescription )
	IFF(i_AssociationDescription IS NULL, 'N/A', i_AssociationDescription) AS AssociationDescription
	FROM SQ_Party
	LEFT JOIN LKP_Association
	ON LKP_Association.AssociationCode = SQ_Party.AssociationCode
	LEFT JOIN LKP_ExistingPartyDim
	ON LKP_ExistingPartyDim.EDWPartyAKID = SQ_Party.PartyAKId
),
FIL_insert AS (
	SELECT
	changed_flag, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	CreatedDate, 
	ModifiedDate, 
	PartyId AS EDWPartyPKID, 
	PartyNumber, 
	PartyAKId AS EDWPartyAKID, 
	PrimaryPhoneNumber, 
	OriginalPolicyInceptionDate, 
	DeletedIndicator, 
	StatusCode, 
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
	AssociationCode, 
	AssociationDescription
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'
),
TGT_PartyDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EDWPartyPKId, PartyNumber, EDWPartyAKId, PrimaryPhoneNumber, OriginalPolicyInceptionDate, DeletedIndicator, StatusCode, Name, DoingBusinessAs, LegalEntityTypeCode, SICCode, SICCodeTitle, NAICSCode, NAICSCodeTitle, PrefixTitleCode, FirstName, MiddleName, LastName, AssociationCode, AssociationDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AuditID AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	EDWPartyPKID AS EDWPARTYPKID, 
	PARTYNUMBER, 
	EDWPartyAKID AS EDWPARTYAKID, 
	PRIMARYPHONENUMBER, 
	ORIGINALPOLICYINCEPTIONDATE, 
	DELETEDINDICATOR, 
	STATUSCODE, 
	NAME, 
	DOINGBUSINESSAS, 
	LEGALENTITYTYPECODE, 
	SicCode AS SICCODE, 
	SicCodeTitle AS SICCODETITLE, 
	NaicsCode AS NAICSCODE, 
	NaicsCodeTitle AS NAICSCODETITLE, 
	PREFIXTITLECODE, 
	FIRSTNAME, 
	MIDDLENAME, 
	LASTNAME, 
	ASSOCIATIONCODE, 
	ASSOCIATIONDESCRIPTION
	FROM FIL_insert
),
SQ_PartyDim AS (
	SELECT 
		a.PartyDimID, 
		a.EffectiveDate,
		a.ExpirationDate, 
		a.EDWPartyAKID
	FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.PartyDim a
	WHERE  a.EDWPartyAKID  IN
		 ( SELECT EDWPartyAKID FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyDim
		   WHERE CurrentSnapshotFlag = 1 GROUP BY EDWPartyAKID HAVING count(*) > 1) 
	ORDER BY a.EDWPartyAKID,a.EffectiveDate DESC
	
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 EffectiveDate.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	PartyDimID,
	0 AS CurrentSnapshotFlag,
	EffectiveDate,
	ExpirationDate AS OriginalExpirationDate,
	EDWPartyAKID,
	-- *INF*: DECODE(TRUE,
	-- EDWPartyAKID = v_prev_EDWPartyAKID , ADD_TO_DATE(v_prev_EffectiveDate,'SS',-1),
	-- OriginalExpirationDate)
	DECODE(
	    TRUE,
	    EDWPartyAKID = v_prev_EDWPartyAKID, DATEADD(SECOND,- 1,v_prev_EffectiveDate),
	    OriginalExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EDWPartyAKID AS v_prev_EDWPartyAKID,
	EffectiveDate AS v_prev_EffectiveDate,
	SYSDATE AS ModifiedDate
	FROM SQ_PartyDim
),
FIL_FirstRowInAKGroup AS (
	SELECT
	PartyDimID, 
	CurrentSnapshotFlag, 
	OriginalExpirationDate, 
	ExpirationDate AS NewExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_eff_from_date
	WHERE OriginalExpirationDate != NewExpirationDate
),
UPD_OldRecord AS (
	SELECT
	PartyDimID, 
	CurrentSnapshotFlag, 
	NewExpirationDate AS ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
TGT_PartyDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyDim AS T
	USING UPD_OldRecord AS S
	ON T.PartyDimId = S.PartyDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),