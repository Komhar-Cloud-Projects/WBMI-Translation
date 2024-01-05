WITH
LKP_ReleasedQuote AS (
	SELECT
	QuoteStatusCode,
	QuoteAKId
	FROM (
		SELECT 
			QuoteStatusCode,
			QuoteAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote
		WHERE exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteAKId=Quote.QuoteAKId
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		and
		QuoteStatusCode='Released Quote'
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote a
		where a.QuoteAKId=Quote.QuoteAKId
		and a.StatusDate>Quote.StatusDate
		and a.QuoteStatusCode='Reactivate')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAKId ORDER BY QuoteStatusCode) = 1
),
LKP_Bound AS (
	SELECT
	QuoteStatusCode,
	QuoteAKId
	FROM (
		SELECT 
			QuoteStatusCode,
			QuoteAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote
		WHERE exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteAKId=Quote.QuoteAKId
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		and
		QuoteStatusCode='Bound'
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote a
		where a.QuoteAKId=Quote.QuoteAKId
		and a.StatusDate>Quote.StatusDate
		and a.QuoteStatusCode='Reactivate')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAKId ORDER BY QuoteStatusCode) = 1
),
LKP_UnBound AS (
	SELECT
	QuoteStatusCode,
	QuoteAKId
	FROM (
		SELECT 
			QuoteStatusCode,
			QuoteAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote
		WHERE exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteAKId=Quote.QuoteAKId
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		and
		QuoteStatusCode='UnBound'
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote a
		where a.QuoteAKId=Quote.QuoteAKId
		and a.StatusDate>Quote.StatusDate
		and a.QuoteStatusCode='Reactivate')
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote b
		where b.QuoteAKId=Quote.QuoteAKId
		and b.StatusDate>Quote.StatusDate
		and b.QuoteStatusCode='Bound')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAKId ORDER BY QuoteStatusCode) = 1
),
LKP_InitializedQuote AS (
	SELECT
	InternalExternalIndicator,
	QuoteAKId
	FROM (
		SELECT 
			InternalExternalIndicator,
			QuoteAKId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote
		WHERE exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteAKId=Quote.QuoteAKId
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		and
		QuoteStatusCode='Initialized Quote'
		and not exists (
		select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote b
		where b.QuoteAKId=Quote.QuoteAKId
		and b.StatusDate>Quote.StatusDate
		and b.QuoteStatusCode='Initialized Quote')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY QuoteAKId ORDER BY InternalExternalIndicator) = 1
),
LKP_WBPolicyStaging AS (
	SELECT
	PolicyNumber,
	PolicyVersionFormatted
	FROM (
		select distinct W.PolicyNumber as PolicyNumber,
		 W.PolicyVersionFormatted as PolicyVersionFormatted
		from @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBPolicyStaging W with (nolock)
		where W.PenguinTechGenerated=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyNumber,PolicyVersionFormatted ORDER BY PolicyNumber) = 1
),
SQ_Quote AS (
	SELECT
		QuoteAKId,
		QuoteNumber,
		QuoteVersion,
		QuoteStatusCode,
		QuoteCreatedDate,
		QuoteIssueCode,
		QuoteIssueCodeChangeDate,
		IssuedUWID,
		IssuedUnderwriter,
		QuoteChannel,
		LCSurveyOrderedIndicator,
		LCSurveyOrderedDate,
		QuoteChannelOrigin,
		RolloverPolicyIndicator,
		RolloverPriorCarrier,
		ServCenterSupportCode
	FROM Quote
	ON Quote.CurrentSnapshotFlag=1
	and exists (
	select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
	where q.QuoteAKId=Quote.QuoteAKId
	and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Flags AS (
	SELECT
	QuoteAKId,
	QuoteNumber,
	QuoteVersion,
	QuoteStatusCode AS i_QuoteStatusCode,
	QuoteCreatedDate,
	QuoteIssueCode,
	-- *INF*: DECODE(TRUE,
	-- QuoteIssueCode='N','New',
	-- QuoteIssueCode='R','Renewal',
	-- QuoteIssueCode)
	DECODE(TRUE,
	QuoteIssueCode = 'N', 'New',
	QuoteIssueCode = 'R', 'Renewal',
	QuoteIssueCode) AS o_QuoteIssueCodeDesc,
	QuoteIssueCodeChangeDate AS QuoteIssueCodeDescChangeDate,
	-- *INF*: IIF(i_QuoteStatusCode!='Initialized Quote','1','0')
	IFF(i_QuoteStatusCode != 'Initialized Quote', '1', '0') AS SubmittedFlag,
	-- *INF*: DECODE(TRUE,
	-- i_QuoteStatusCode='Issued','1',
	-- i_QuoteStatusCode='Released Quote','1',
	-- i_QuoteStatusCode='Declined','0',
	-- i_QuoteStatusCode='Bound','1',
	-- i_QuoteStatusCode='Unbound','1',
	-- NOT ISNULL(:LKP.LKP_RELEASEDQUOTE(QuoteAKId)),'1',
	-- i_QuoteStatusCode='Closed' AND NOT ISNULL(:LKP.LKP_BOUND(QuoteAKId)),'1',
	-- '0')
	DECODE(TRUE,
	i_QuoteStatusCode = 'Issued', '1',
	i_QuoteStatusCode = 'Released Quote', '1',
	i_QuoteStatusCode = 'Declined', '0',
	i_QuoteStatusCode = 'Bound', '1',
	i_QuoteStatusCode = 'Unbound', '1',
	NOT LKP_RELEASEDQUOTE_QuoteAKId.QuoteStatusCode IS NULL, '1',
	i_QuoteStatusCode = 'Closed' AND NOT LKP_BOUND_QuoteAKId.QuoteStatusCode IS NULL, '1',
	'0') AS ReleasedQuoteFlag,
	-- *INF*: DECODE(TRUE,
	-- i_QuoteStatusCode='Bound','1',
	-- NOT ISNULL(:LKP.LKP_BOUND(QuoteAKId)) AND ISNULL(:LKP.LKP_UNBOUND(QuoteAKId)),'1',
	-- '0')
	DECODE(TRUE,
	i_QuoteStatusCode = 'Bound', '1',
	NOT LKP_BOUND_QuoteAKId.QuoteStatusCode IS NULL AND LKP_UNBOUND_QuoteAKId.QuoteStatusCode IS NULL, '1',
	'0') AS BoundFlag,
	-- *INF*: IIF(i_QuoteStatusCode='Issued','1','0')
	IFF(i_QuoteStatusCode = 'Issued', '1', '0') AS IssuedFlag,
	-- *INF*: IIF(i_QuoteStatusCode='Declined','1','0')
	IFF(i_QuoteStatusCode = 'Declined', '1', '0') AS DeclineFlag,
	-- *INF*: IIF(i_QuoteStatusCode='Closed','1','0')
	IFF(i_QuoteStatusCode = 'Closed', '1', '0') AS ClosedFlag,
	-- *INF*: IIF(ISNULL(:LKP.LKP_WBPOLICYSTAGING(QuoteNumber,QuoteVersion)),'0','1')
	-- --External = 1
	-- --Internal = 0
	IFF(LKP_WBPOLICYSTAGING_QuoteNumber_QuoteVersion.PolicyNumber IS NULL, '0', '1') AS v_ExtnternalInitializedFlag_Liferay,
	-- *INF*: DECODE(TRUE,
	-- :LKP.LKP_INITIALIZEDQUOTE(QuoteAKId)='External','1',
	-- v_ExtnternalInitializedFlag_Liferay='1','1',
	-- '0')
	DECODE(TRUE,
	LKP_INITIALIZEDQUOTE_QuoteAKId.InternalExternalIndicator = 'External', '1',
	v_ExtnternalInitializedFlag_Liferay = '1', '1',
	'0') AS ExternalInitializedQuoteFlag,
	IssuedUWID,
	IssuedUnderwriter,
	QuoteChannel,
	LCSurveyOrderedIndicator,
	-- *INF*: decode(LCSurveyOrderedIndicator,'T','1','F','0',NULL)
	decode(LCSurveyOrderedIndicator,
	'T', '1',
	'F', '0',
	NULL) AS o_LCSurveyOrderedIndicator,
	LCSurveyOrderedDate,
	QuoteChannelOrigin,
	RolloverPolicyIndicator,
	RolloverPriorCarrier,
	ServCenterSupportCode
	FROM SQ_Quote
	LEFT JOIN LKP_RELEASEDQUOTE LKP_RELEASEDQUOTE_QuoteAKId
	ON LKP_RELEASEDQUOTE_QuoteAKId.QuoteAKId = QuoteAKId

	LEFT JOIN LKP_BOUND LKP_BOUND_QuoteAKId
	ON LKP_BOUND_QuoteAKId.QuoteAKId = QuoteAKId

	LEFT JOIN LKP_UNBOUND LKP_UNBOUND_QuoteAKId
	ON LKP_UNBOUND_QuoteAKId.QuoteAKId = QuoteAKId

	LEFT JOIN LKP_WBPOLICYSTAGING LKP_WBPOLICYSTAGING_QuoteNumber_QuoteVersion
	ON LKP_WBPOLICYSTAGING_QuoteNumber_QuoteVersion.PolicyNumber = QuoteNumber
	AND LKP_WBPOLICYSTAGING_QuoteNumber_QuoteVersion.PolicyVersionFormatted = QuoteVersion

	LEFT JOIN LKP_INITIALIZEDQUOTE LKP_INITIALIZEDQUOTE_QuoteAKId
	ON LKP_INITIALIZEDQUOTE_QuoteAKId.QuoteAKId = QuoteAKId

),
LKP_QuoteDim AS (
	SELECT
	QuoteDimId,
	ChangeAttributes,
	EDWQuoteAKId
	FROM (
		SELECT a.QuoteDimId as QuoteDimId,
		convert(varchar(1),a.SubmittedFlag)+convert(varchar(1),a.ReleasedQuoteFlag)+convert(varchar(1),a.BoundFlag)+convert(varchar(1),a.IssuedFlag)+convert(varchar(1),a.DeclineFlag)+convert(varchar(1),a.ClosedFlag)+convert(varchar(1),a.ExternalInitializedQuoteFlag)+ISNULL(QuoteIssueCodeDesc,'')+ISNULL(IssuedUWID,'')+ISNULL(IssuedUnderwriter,'')+ISNULL(QuoteChannelOrigin,'') as ChangeAttributes,
		a.EDWQuoteAKId as EDWQuoteAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteDim a
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Quote q
		where q.QuoteAKId=a.EDWQuoteAKId
		and q.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}')
		@{pipeline().parameters.WHERE_CLAUSE}
		order by a.EDWQuoteAKId
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWQuoteAKId ORDER BY QuoteDimId) = 1
),
EXP_Change AS (
	SELECT
	LKP_QuoteDim.QuoteDimId,
	LKP_QuoteDim.ChangeAttributes AS lkp_ChangeAttributes,
	EXP_Flags.QuoteAKId,
	EXP_Flags.QuoteNumber,
	EXP_Flags.QuoteCreatedDate,
	EXP_Flags.SubmittedFlag,
	EXP_Flags.ReleasedQuoteFlag,
	EXP_Flags.BoundFlag,
	EXP_Flags.IssuedFlag,
	EXP_Flags.DeclineFlag,
	EXP_Flags.ClosedFlag,
	EXP_Flags.ExternalInitializedQuoteFlag,
	EXP_Flags.o_QuoteIssueCodeDesc AS QuoteIssueCodeDesc,
	-- *INF*: IIF (ISNULL(QuoteIssueCodeDesc),'New',QuoteIssueCodeDesc)
	IFF(QuoteIssueCodeDesc IS NULL, 'New', QuoteIssueCodeDesc) AS v_QuoteIssueCodeDesc,
	v_QuoteIssueCodeDesc AS o_QuoteIssueCodeDesc,
	EXP_Flags.QuoteIssueCodeDescChangeDate,
	EXP_Flags.IssuedUWID,
	-- *INF*: IIF (ISNULL(IssuedUWID) Or IssuedUWID='0','N/A',IssuedUWID)
	IFF(IssuedUWID IS NULL OR IssuedUWID = '0', 'N/A', IssuedUWID) AS v_IssuedUWID,
	v_IssuedUWID AS o_IssuedUWID,
	EXP_Flags.IssuedUnderwriter,
	-- *INF*: IIF (ISNULL(IssuedUnderwriter),'N/A',IssuedUnderwriter)
	IFF(IssuedUnderwriter IS NULL, 'N/A', IssuedUnderwriter) AS v_IssuedUnderwriter,
	v_IssuedUnderwriter AS o_IssuedUnderwriter,
	EXP_Flags.QuoteChannel,
	-- *INF*: IIF (ISNULL(QuoteChannel),'N/A',QuoteChannel)
	IFF(QuoteChannel IS NULL, 'N/A', QuoteChannel) AS v_QuoteChannel,
	v_QuoteChannel AS o_QuoteChannel,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(QuoteDimId),1,
	-- lkp_ChangeAttributes<>SubmittedFlag||ReleasedQuoteFlag||BoundFlag||IssuedFlag||DeclineFlag||ClosedFlag||ExternalInitializedQuoteFlag||v_QuoteIssueCodeDesc||v_IssuedUWID||v_IssuedUnderwriter || v_QuoteChannel,2,
	-- 0)
	DECODE(TRUE,
	QuoteDimId IS NULL, 1,
	lkp_ChangeAttributes <> SubmittedFlag || ReleasedQuoteFlag || BoundFlag || IssuedFlag || DeclineFlag || ClosedFlag || ExternalInitializedQuoteFlag || v_QuoteIssueCodeDesc || v_IssuedUWID || v_IssuedUnderwriter || v_QuoteChannel, 2,
	0) AS o_ChangeFlag,
	EXP_Flags.o_LCSurveyOrderedIndicator AS LCSurveyOrderedIndicator,
	EXP_Flags.LCSurveyOrderedDate,
	EXP_Flags.QuoteChannelOrigin AS i_QuoteChannelOrigin,
	-- *INF*: IIF (ISNULL(i_QuoteChannelOrigin),'N/A',i_QuoteChannelOrigin)
	IFF(i_QuoteChannelOrigin IS NULL, 'N/A', i_QuoteChannelOrigin) AS v_QuoteChannelOrigin,
	v_QuoteChannelOrigin AS o_QuoteChannelOrigin,
	EXP_Flags.RolloverPolicyIndicator AS i_RolloverPolicyIndicator,
	EXP_Flags.RolloverPriorCarrier,
	-- *INF*: DECODE(i_RolloverPolicyIndicator, 'T',1,'F',0,0)
	DECODE(i_RolloverPolicyIndicator,
	'T', 1,
	'F', 0,
	0) AS o_rolloverPolicyIndicator,
	EXP_Flags.ServCenterSupportCode
	FROM EXP_Flags
	LEFT JOIN LKP_QuoteDim
	ON LKP_QuoteDim.EDWQuoteAKId = EXP_Flags.QuoteAKId
),
RTR_InsertElseUpdate AS (
	SELECT
	QuoteDimId,
	QuoteAKId,
	QuoteNumber,
	QuoteCreatedDate,
	SubmittedFlag,
	ReleasedQuoteFlag,
	BoundFlag,
	IssuedFlag,
	DeclineFlag,
	ClosedFlag,
	ExternalInitializedQuoteFlag,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_ChangeFlag AS ChangeFlag,
	o_QuoteIssueCodeDesc AS QuoteIssueCodeDesc,
	QuoteIssueCodeDescChangeDate,
	o_IssuedUWID AS IssuedUWID,
	o_IssuedUnderwriter AS IssuedUnderwriter,
	o_QuoteChannel,
	LCSurveyOrderedIndicator,
	LCSurveyOrderedDate,
	o_QuoteChannelOrigin AS QuoteChannelOrigin,
	o_rolloverPolicyIndicator AS RolloverPolicyIndicator,
	RolloverPriorCarrier,
	ServCenterSupportCode
	FROM EXP_Change
),
RTR_InsertElseUpdate_INSERT AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag=1),
RTR_InsertElseUpdate_UPDATE AS (SELECT * FROM RTR_InsertElseUpdate WHERE ChangeFlag=2),
TGT_QuoteDim_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteDim
	(AuditId, CreatedDate, ModifiedDate, EDWQuoteAKId, QuoteNumber, QuoteCreatedDate, SubmittedFlag, ReleasedQuoteFlag, BoundFlag, IssuedFlag, DeclineFlag, ClosedFlag, ExternalInitializedQuoteFlag, QuoteIssueCodeDesc, QuoteIssueCodeDescChangeDate, IssuedUWID, IssuedUnderwriter, QuoteChannel, LCSurveyOrderedIndicator, LCSurveyOrderedDate, QuoteChannelOrigin, ServCenterSupportCode, RolloverPolicyIndicator, RolloverPriorCarrier)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	QuoteAKId AS EDWQUOTEAKID, 
	QUOTENUMBER, 
	QUOTECREATEDDATE, 
	SUBMITTEDFLAG, 
	RELEASEDQUOTEFLAG, 
	BOUNDFLAG, 
	ISSUEDFLAG, 
	DECLINEFLAG, 
	CLOSEDFLAG, 
	EXTERNALINITIALIZEDQUOTEFLAG, 
	QUOTEISSUECODEDESC, 
	QUOTEISSUECODEDESCCHANGEDATE, 
	ISSUEDUWID, 
	ISSUEDUNDERWRITER, 
	o_QuoteChannel AS QUOTECHANNEL, 
	LCSURVEYORDEREDINDICATOR, 
	LCSURVEYORDEREDDATE, 
	QUOTECHANNELORIGIN, 
	SERVCENTERSUPPORTCODE, 
	ROLLOVERPOLICYINDICATOR, 
	ROLLOVERPRIORCARRIER
	FROM RTR_InsertElseUpdate_INSERT
),
UPD_Target AS (
	SELECT
	QuoteDimId, 
	ModifiedDate, 
	SubmittedFlag, 
	ReleasedQuoteFlag, 
	BoundFlag, 
	IssuedFlag, 
	DeclineFlag, 
	ClosedFlag, 
	ExternalInitializedQuoteFlag, 
	AuditId, 
	QuoteIssueCodeDesc AS QuoteIssueCodeDesc3, 
	QuoteIssueCodeDescChangeDate AS QuoteIssueCodeDescChangeDate3, 
	IssuedUWID AS IssuedUWID3, 
	IssuedUnderwriter AS IssuedUnderwriter3, 
	o_QuoteChannel, 
	LCSurveyOrderedIndicator AS LCSurveyOrderedIndicator3, 
	LCSurveyOrderedDate AS LCSurveyOrderedDate3, 
	QuoteChannelOrigin AS QuoteChannelOrigin3, 
	RolloverPolicyIndicator AS RolloverPolicyIndicator3, 
	RolloverPriorCarrier AS RolloverPriorCarrier3, 
	ServCenterSupportCode AS ServCenterSupportCode3
	FROM RTR_InsertElseUpdate_UPDATE
),
TGT_QuoteDim_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.QuoteDim AS T
	USING UPD_Target AS S
	ON T.QuoteDimId = S.QuoteDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.ModifiedDate = S.ModifiedDate, T.SubmittedFlag = S.SubmittedFlag, T.ReleasedQuoteFlag = S.ReleasedQuoteFlag, T.BoundFlag = S.BoundFlag, T.IssuedFlag = S.IssuedFlag, T.DeclineFlag = S.DeclineFlag, T.ClosedFlag = S.ClosedFlag, T.ExternalInitializedQuoteFlag = S.ExternalInitializedQuoteFlag, T.QuoteIssueCodeDesc = S.QuoteIssueCodeDesc3, T.QuoteIssueCodeDescChangeDate = S.QuoteIssueCodeDescChangeDate3, T.IssuedUWID = S.IssuedUWID3, T.IssuedUnderwriter = S.IssuedUnderwriter3, T.QuoteChannel = S.o_QuoteChannel, T.LCSurveyOrderedIndicator = S.LCSurveyOrderedIndicator3, T.LCSurveyOrderedDate = S.LCSurveyOrderedDate3, T.QuoteChannelOrigin = S.QuoteChannelOrigin3, T.ServCenterSupportCode = S.ServCenterSupportCode3, T.RolloverPolicyIndicator = S.RolloverPolicyIndicator3, T.RolloverPriorCarrier = S.RolloverPriorCarrier3
),