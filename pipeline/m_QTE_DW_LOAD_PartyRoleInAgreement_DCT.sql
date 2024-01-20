WITH
SQ_PartyRoleInAgreement AS (
	SELECT
		WorkDCTPolicy.PolicyGUId,
		WorkDCTPolicy.CustomerNum,
		DCPartyAssociationStaging.PartyAssociationType
	FROM WorkDCTPolicy
	INNER JOIN DCPartyAssociationStaging
	ON WorkDCTPolicy.SessionId=DCPartyAssociationStaging.SessionId
	and
	WorkDCTPolicy.QuoteActionTimeStamp is not null
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Default AS (
	SELECT
	PolicyGUId,
	CustomerNum,
	PartyAssociationType AS i_PartyAssociationType,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(i_PartyAssociationType))='Account',
	-- 'Prospect',
	-- 'N/A'
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(i_PartyAssociationType)) = 'Account', 'Prospect',
	    'N/A'
	) AS o_PartyRoleInAgreementTypeCode
	FROM SQ_PartyRoleInAgreement
),
AGG_RemoveDuplicates AS (
	SELECT
	PolicyGUId,
	CustomerNum,
	o_PartyRoleInAgreementTypeCode AS PartyRoleInAgreementTypeCode
	FROM EXP_Default
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyGUId, CustomerNum, PartyRoleInAgreementTypeCode ORDER BY NULL) = 1
),
EXP_GetValues AS (
	SELECT
	PolicyGUId AS i_PolicyGUId,
	CustomerNum AS i_CustomerNum,
	PartyRoleInAgreementTypeCode AS i_PartyRoleInAgreementTypeCode,
	-- *INF*: LTRIM(RTRIM(i_PolicyGUId))
	LTRIM(RTRIM(i_PolicyGUId)) AS o_QuoteKey,
	-- *INF*: LTRIM(RTRIM(i_CustomerNum))
	LTRIM(RTRIM(i_CustomerNum)) AS o_PartyNumber,
	i_PartyRoleInAgreementTypeCode AS o_PartyRoleInAgreementTypeCode
	FROM AGG_RemoveDuplicates
),
LKP_Party AS (
	SELECT
	PartyAKId,
	PartyNumber
	FROM (
		SELECT 
			PartyAKId,
			PartyNumber
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Party
		WHERE CurrentSnapshotFlag=1 and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where LTRIM(RTRIM(w.CustomerNum))=Party.PartyNumber)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PartyNumber ORDER BY PartyAKId) = 1
),
LKP_Quote AS (
	SELECT
	QuoteAKId,
	QuoteKey
	FROM (
		SELECT 
			QuoteAKId,
			QuoteKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Quote
		WHERE CurrentSnapshotFlag=1 and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where w.PolicyGUId=Quote.QuoteKey)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteKey ORDER BY QuoteAKId) = 1
),
EXP_GetIds AS (
	SELECT
	LKP_Quote.QuoteAKId AS i_QuoteAKId,
	LKP_Party.PartyAKId AS i_PartyAKId,
	EXP_GetValues.o_PartyRoleInAgreementTypeCode AS i_PartyRoleInAgreementTypeCode,
	-- *INF*: IIF(ISNULL(i_QuoteAKId),-1,i_QuoteAKId)
	IFF(i_QuoteAKId IS NULL, - 1, i_QuoteAKId) AS o_QuoteAKId,
	-- *INF*: IIF(ISNULL(i_PartyAKId),-1,i_PartyAKId)
	IFF(i_PartyAKId IS NULL, - 1, i_PartyAKId) AS o_PartyAKId,
	i_PartyRoleInAgreementTypeCode AS o_PartyRoleInAgreementTypeCode
	FROM EXP_GetValues
	LEFT JOIN LKP_Party
	ON LKP_Party.PartyNumber = EXP_GetValues.o_PartyNumber
	LEFT JOIN LKP_Quote
	ON LKP_Quote.QuoteKey = EXP_GetValues.o_QuoteKey
),
LKP_PartyRoleInAgreement AS (
	SELECT
	PartyRoleInAgreementAkId,
	QuoteAkId,
	PartyAkId,
	PartyRoleInAgreementTypeCode
	FROM (
		SELECT a.PartyRoleInAgreementAkId as PartyRoleInAgreementAkId,
		a.QuoteAkId as QuoteAkId,
		a.PartyAkId as PartyAkId,
		a.PartyRoleInAgreementTypeCode as PartyRoleInAgreementTypeCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyRoleInAgreement a
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.Party b
		on
		a.PartyAKId=b.PartyAKId
		and b.CurrentSnapshotFlag=1 and b.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy w
		where LTRIM(RTRIM(w.CustomerNum))=b.PartyNumber)
		where a.CurrentSnapshotFlag=1 and a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by a.QuoteAkId,a.PartyAkId,a.PartyRoleInAgreementTypeCode
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAkId,PartyAkId,PartyRoleInAgreementTypeCode ORDER BY PartyRoleInAgreementAkId) = 1
),
FIL_AKIDIsNull AS (
	SELECT
	LKP_PartyRoleInAgreement.PartyRoleInAgreementAkId, 
	EXP_GetIds.o_QuoteAKId AS QuoteAKId, 
	EXP_GetIds.o_PartyAKId AS PartyAKId, 
	EXP_GetIds.o_PartyRoleInAgreementTypeCode AS PartyRoleInAgreementTypeCode
	FROM EXP_GetIds
	LEFT JOIN LKP_PartyRoleInAgreement
	ON LKP_PartyRoleInAgreement.QuoteAkId = EXP_GetIds.o_QuoteAKId AND LKP_PartyRoleInAgreement.PartyAkId = EXP_GetIds.o_PartyAKId AND LKP_PartyRoleInAgreement.PartyRoleInAgreementTypeCode = EXP_GetIds.o_PartyRoleInAgreementTypeCode
	WHERE ISNULL(PartyRoleInAgreementAkId)  AND  NOT  (QuoteAKId=-1 AND PartyAKId=-1)
),
SEQ_PartyRoleInAgreementAkId AS (
	CREATE SEQUENCE SEQ_PartyRoleInAgreementAkId
	START = 1
	INCREMENT = 1;
),
EXP_AssignMetadata AS (
	SELECT
	SEQ_PartyRoleInAgreementAkId.NEXTVAL AS i_NEXTVAL,
	QuoteAKId AS i_QuoteAKId,
	PartyAKId AS i_PartyAKId,
	PartyRoleInAgreementTypeCode AS i_PartyRoleInAgreementTypeCode,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_TIMESTAMP('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_NEXTVAL AS o_PartyRoleInAgreenmentAkId,
	i_QuoteAKId AS o_QuoteAkId,
	i_PartyAKId AS o_PartyAkId,
	i_PartyRoleInAgreementTypeCode AS o_PartyRoleInAgreementTypeCode
	FROM FIL_AKIDIsNull
),
TGT_PartyRoleInAgreement_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PartyRoleInAgreement
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, PartyRoleInAgreementAKId, QuoteAKId, PartyAKId, PartyRoleInAgreementTypeCode)
	SELECT 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_PartyRoleInAgreenmentAkId AS PARTYROLEINAGREEMENTAKID, 
	o_QuoteAkId AS QUOTEAKID, 
	o_PartyAkId AS PARTYAKID, 
	o_PartyRoleInAgreementTypeCode AS PARTYROLEINAGREEMENTTYPECODE
	FROM EXP_AssignMetadata
),