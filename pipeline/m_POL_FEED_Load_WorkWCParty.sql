WITH
SQ_DC_Party AS (
	select 
	DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	DPA.EntityType,
	DPA.EntityOtherType,
	DWA.LegalNatureOfEntity AOIEntityType,
	DWA.LegalNatureOfEntityOtherType AOIEntityOtherType,
	DPA.PartyAssociationType,
	WPT.FEIN,
	WPT.DoingBusinessAs,
	DPT.Name,
	DC.PhoneNumber,
	DWA.BureauNumber,
	DWLA.WC_LocationId,
	ISNULL(WCUA.BusinessOrIndividual,WCP.BusinessOrIndividual) BusinessOrIndividual,
	WPT.FirstName,
	WPT.MiddleName,
	WPT.LastName,
	WWA.BureauUnemploymentNumberState,
	DWA.StateUnemploymentNumber,
	P.PolicyNumber+P.PolicyVersionFormatted PolicyKey,
	CASE WHEN DWA.Deleted=1 THEN '1' ELSE '0' END Deleted,
	DT.Type TransactionType,
	DC.Email
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party DPT with(nolock)
	on DL.SessionId=DPT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation DPA with(nolock)
	on DPT.SessionId=DPA.SessionId
	and DPT.PartyId=DPA.PartyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy P with(nolock)
	on P.SessionID=DL.SessionID
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_UniqueApplicant WUA with(nolock)
	on DPT.PartyId=WUA.PartyId
	and DPT.SessionId=WUA.SessionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_UniqueApplicant WCUA with(nolock)
	on WUA.WB_UniqueApplicantId=WCUA.WB_UniqueApplicantId
	and WUA.SessionId=WCUA.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Party WPT with(nolock)
	on DPT.SessionId=WPT.SessionId
	and DPT.PartyId=WPT.PartyId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CL_Party WCP with(nolock)
	on WPT.WB_PartyId=WCP.WB_PartyId
	and WPT.SessionId=WCP.SessionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Contact DC with(nolock)
	on DPT.Sessionid=DC.Sessionid
	and DPT.PartyId=DC.PartyId
	and DC.Type = 'Primary'
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency WA with(nolock)
	on DPT.SessionId=WA.SessionId
	and DPT.PartyId=WA.PartyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DL.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DL.SessionId=DT.SessionId
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_AdditionalOtherInterest DWA with(nolock)
	on DPA.Sessionid=DWA.Sessionid
	and DPA.Objectid=DWA.WC_AdditionalOtherInterestId
	and DPA.ObjectName='DC_WC_AdditionalOtherInterest'
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_AdditionalOtherInterest WWA with(nolock)
	on DWA.Sessionid=WWA.Sessionid
	and WWA.WC_AdditionalOtherInterestId=DWA.WC_AdditionalOtherInterestId
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_LocationAOI DWLA with(nolock)
	on DPA.Sessionid=DWLA.Sessionid
	and DPA.Objectid=DWLA.WC_AdditionalOtherInterestId
	and DPA.ObjectName='DC_WC_AdditionalOtherInterest'
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DT.State='Committed'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	EntityType,
	EntityOtherType,
	AOIEntityType,
	AOIEntityOtherType,
	PartyAssociationType,
	FEIN,
	DoingBusinessAs,
	Name,
	PhoneNumber,
	BureauNumber,
	WC_LocationId,
	BusinessOrIndividual,
	FirstName,
	MiddleName,
	LastName,
	BureauUnemploymentNumberState,
	StateUnemploymentNumber,
	PolicyKey,
	Deleted,
	TransactionType,
	Email
	FROM SQ_DC_Party
),
LKP_LatestSession AS (
	SELECT
	SessionId,
	Purpose,
	HistoryID
	FROM (
		Select distinct DT.HistoryID AS HistoryID,
		DS.Purpose AS Purpose,
		Max(DS.Sessionid) AS Sessionid
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
		on DT.Sessionid=DS.Sessionid
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
		on DT.Sessionid=DL.Sessionid
		where DL.Type='WorkersCompensation'
		and DS.Purpose='Onset'
		and DT.State='Committed'
		and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		group by DT.HistoryID,DS.Purpose
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,Purpose,HistoryID ORDER BY SessionId) = 1
),
LKP_WorkWCTrackHistory AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	HistoryID,
	Purpose
	FROM (
		SELECT 
		WorkWCTrackHistory.WCTrackHistoryID as WCTrackHistoryID, 
		WorkWCTrackHistory.Auditid as Auditid, 
		WorkWCTrackHistory.HistoryID as HistoryID, 
		WorkWCTrackHistory.Purpose as Purpose 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory
		order by WorkWCTrackHistory.HistoryID,WorkWCTrackHistory.Purpose,WorkWCTrackHistory.Auditid ASC
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,Purpose ORDER BY WCTrackHistoryID) = 1
),
EXP_RecordFlagging AS (
	SELECT
	LKP_WorkWCTrackHistory.WCTrackHistoryID AS lkp_WCTrackHistoryID,
	LKP_WorkWCTrackHistory.Auditid AS lkp_Auditid,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (NOT ISNULL(lkp_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (lkp_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	EXP_SRCDataCollect.EntityType,
	EXP_SRCDataCollect.EntityOtherType,
	EXP_SRCDataCollect.AOIEntityType,
	EXP_SRCDataCollect.AOIEntityOtherType,
	EXP_SRCDataCollect.PartyAssociationType,
	-- *INF*: UPPER(LTRIM(RTRIM(PartyAssociationType)))
	UPPER(LTRIM(RTRIM(PartyAssociationType))) AS PartyAssociationType_JNR,
	EXP_SRCDataCollect.FEIN,
	EXP_SRCDataCollect.DoingBusinessAs,
	EXP_SRCDataCollect.Name,
	-- *INF*: UPPER(LTRIM(RTRIM(Name)))
	UPPER(LTRIM(RTRIM(Name))) AS Name_JNR,
	EXP_SRCDataCollect.PhoneNumber,
	EXP_SRCDataCollect.BureauNumber,
	EXP_SRCDataCollect.WC_LocationId,
	EXP_SRCDataCollect.BusinessOrIndividual,
	LKP_LatestSession.SessionId AS lkp_SessionId,
	EXP_SRCDataCollect.FirstName,
	EXP_SRCDataCollect.MiddleName,
	EXP_SRCDataCollect.LastName,
	EXP_SRCDataCollect.BureauUnemploymentNumberState,
	EXP_SRCDataCollect.StateUnemploymentNumber,
	EXP_SRCDataCollect.PolicyKey,
	EXP_SRCDataCollect.Deleted,
	EXP_SRCDataCollect.TransactionType,
	EXP_SRCDataCollect.Email
	FROM EXP_SRCDataCollect
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = EXP_SRCDataCollect.SessionId AND LKP_LatestSession.Purpose = EXP_SRCDataCollect.Purpose AND LKP_LatestSession.HistoryID = EXP_SRCDataCollect.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_SRCDataCollect.HistoryID AND LKP_WorkWCTrackHistory.Purpose = EXP_SRCDataCollect.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	EntityType, 
	EntityOtherType, 
	AOIEntityType, 
	AOIEntityOtherType, 
	PartyAssociationType, 
	PartyAssociationType_JNR, 
	FEIN, 
	DoingBusinessAs, 
	Name, 
	Name_JNR, 
	PhoneNumber, 
	BureauNumber, 
	WC_LocationId, 
	BusinessOrIndividual, 
	FirstName, 
	MiddleName, 
	LastName, 
	BureauUnemploymentNumberState, 
	StateUnemploymentNumber, 
	PolicyKey, 
	Deleted, 
	TransactionType, 
	Email
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
SQ_DC_Party_Deleted AS (
	Select distinct D.PolKey,DT.HistoryID,DPA.PartyAssociationType,DPT.Name,CASE WHEN DWA.Deleted='1' then 1 ELSE '0' END Deleted
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party DPT with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation DPA with(nolock)
	on DPT.SessionId=DPA.SessionId
	and DPT.PartyId=DPA.PartyId
	inner join DC_Transaction DT
	on DPT.SessionId=DT.SessionId
	inner join WB_Policy P
	on P.SessionId=DT.SessionId
	INNER Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_AdditionalOtherInterest DWA with(nolock)
	on DPA.Sessionid=DWA.Sessionid
	and DPA.Objectid=DWA.WC_AdditionalOtherInterestId
	and DPA.ObjectName='DC_WC_AdditionalOtherInterest'
	inner JOIN
	(Select distinct WP.PolicyNumber+WP.PolicyVersionFormatted PolKey from WB_Policy WP
	inner join DC_Transaction T with(nolock)
	on WP.SessionId=T.SessionId
	inner join DC_Line DL with(nolock)
	on T.Sessionid=DL.Sessionid
	inner join DC_Session S
	on WP.SessionID=S.SessionID
	WHERE T.State='Committed'
	and DL.Type='WorkersCompensation'
	and S.Purpose='Onset'
	and T.State='Committed'
	and S.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_DELETED}
	) D
	on D.PolKey=(P.PolicyNumber+P.PolicyVersionFormatted)
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History HI with (NOLOCK)
	on HI.HistoryID=DT.HistoryID 
	and HI.DeprecatedBy IS NULL
	where DT.State='Committed'
),
EXPTRANS AS (
	SELECT
	PolKey,
	HistoryID,
	PartyAssociationType,
	Name,
	Deleted
	FROM SQ_DC_Party_Deleted
),
LKP_TrackHistory AS (
	SELECT
	HistoryID,
	PolicyKey,
	IN_PolKey,
	IN_HistoryID
	FROM (
		Select distinct HistoryID as HistoryID, PolicyKey as PolicyKey  from WorkWCTrackHistory
		where AuditID<>@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		order by 2,1--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,HistoryID ORDER BY HistoryID) = 1
),
EXP_Filter AS (
	SELECT
	EXPTRANS.PolKey,
	EXPTRANS.HistoryID,
	EXPTRANS.PartyAssociationType,
	EXPTRANS.Name,
	EXPTRANS.Deleted,
	LKP_TrackHistory.HistoryID AS HistoryID_LKP,
	LKP_TrackHistory.PolicyKey AS PolicyKey_LKP,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(HistoryID_LKP))),'NEW','EXISTS')
	IFF(LTRIM(RTRIM(HistoryID_LKP)) IS NULL, 'NEW', 'EXISTS') AS FilterFlag
	FROM EXPTRANS
	LEFT JOIN LKP_TrackHistory
	ON LKP_TrackHistory.PolicyKey = EXPTRANS.PolKey AND LKP_TrackHistory.HistoryID = EXPTRANS.HistoryID
),
FIL_NewTxns AS (
	SELECT
	PolKey, 
	HistoryID, 
	PartyAssociationType, 
	Name, 
	Deleted, 
	FilterFlag
	FROM EXP_Filter
	WHERE LTRIM(RTRIM(FilterFlag))='EXISTS'
),
SRT_MaxHistID AS (
	SELECT
	PolKey, 
	HistoryID, 
	PartyAssociationType, 
	Name, 
	Deleted, 
	FilterFlag
	FROM FIL_NewTxns
	ORDER BY PolKey ASC, HistoryID DESC
),
EXP_ExistingTxns AS (
	SELECT
	PolKey,
	HistoryID,
	PartyAssociationType,
	-- *INF*: UPPER(LTRIM(RTRIM(PartyAssociationType)))
	UPPER(LTRIM(RTRIM(PartyAssociationType))) AS o_PartyAssociationType,
	Name,
	-- *INF*: UPPER(LTRIM(RTRIM(Name)))
	UPPER(LTRIM(RTRIM(Name))) AS o_Name,
	Deleted,
	-- *INF*: DECODE(TRUE,
	-- PolKey<>v_PriorPolicyKey,HistoryID,
	-- PolKey=v_PriorPolicyKey and HistoryID=v_MaxHistID,v_MaxHistID,
	-- 0)
	DECODE(
	    TRUE,
	    PolKey <> v_PriorPolicyKey, HistoryID,
	    PolKey = v_PriorPolicyKey and HistoryID = v_MaxHistID, v_MaxHistID,
	    0
	) AS v_MaxHistID,
	PolKey AS v_PriorPolicyKey,
	HistoryID AS v_PriorHistoryID,
	-- *INF*: IIF(HistoryID=v_MaxHistID,'1','0')
	IFF(HistoryID = v_MaxHistID, '1', '0') AS v_MaxHistIDFilterFlag,
	v_MaxHistIDFilterFlag AS MaxHistIDFilterFlag
	FROM SRT_MaxHistID
),
FIL_MaxHistID AS (
	SELECT
	PolKey, 
	HistoryID, 
	o_PartyAssociationType AS PartyAssociationType, 
	o_Name AS Name, 
	Deleted, 
	MaxHistIDFilterFlag
	FROM EXP_ExistingTxns
	WHERE MaxHistIDFilterFlag='1'
),
JNR_DeletedTxns AS (SELECT
	FIL_ExcludeSubmittedRecords.WCTrackHistoryID, 
	FIL_ExcludeSubmittedRecords.ExtractDate, 
	FIL_ExcludeSubmittedRecords.Auditid, 
	FIL_ExcludeSubmittedRecords.EntityType, 
	FIL_ExcludeSubmittedRecords.EntityOtherType, 
	FIL_ExcludeSubmittedRecords.AOIEntityType, 
	FIL_ExcludeSubmittedRecords.AOIEntityOtherType, 
	FIL_ExcludeSubmittedRecords.PartyAssociationType, 
	FIL_ExcludeSubmittedRecords.PartyAssociationType_JNR, 
	FIL_ExcludeSubmittedRecords.FEIN, 
	FIL_ExcludeSubmittedRecords.DoingBusinessAs, 
	FIL_ExcludeSubmittedRecords.Name, 
	FIL_ExcludeSubmittedRecords.Name_JNR, 
	FIL_ExcludeSubmittedRecords.PhoneNumber, 
	FIL_ExcludeSubmittedRecords.BureauNumber, 
	FIL_ExcludeSubmittedRecords.WC_LocationId, 
	FIL_ExcludeSubmittedRecords.BusinessOrIndividual, 
	FIL_ExcludeSubmittedRecords.FirstName, 
	FIL_ExcludeSubmittedRecords.MiddleName, 
	FIL_ExcludeSubmittedRecords.LastName, 
	FIL_ExcludeSubmittedRecords.BureauUnemploymentNumberState, 
	FIL_ExcludeSubmittedRecords.StateUnemploymentNumber, 
	FIL_ExcludeSubmittedRecords.PolicyKey, 
	FIL_ExcludeSubmittedRecords.Deleted, 
	FIL_ExcludeSubmittedRecords.TransactionType, 
	FIL_ExcludeSubmittedRecords.Email, 
	FIL_MaxHistID.PolKey AS PolKey_Name, 
	FIL_MaxHistID.HistoryID AS HistoryID_Name, 
	FIL_MaxHistID.PartyAssociationType AS PartyAssociationType_Name, 
	FIL_MaxHistID.Name AS Name_Name, 
	FIL_MaxHistID.Deleted AS Deleted_Name
	FROM FIL_ExcludeSubmittedRecords
	LEFT OUTER JOIN FIL_MaxHistID
	ON FIL_MaxHistID.PolKey = FIL_ExcludeSubmittedRecords.PolicyKey AND FIL_MaxHistID.PartyAssociationType = FIL_ExcludeSubmittedRecords.PartyAssociationType_JNR AND FIL_MaxHistID.Name = FIL_ExcludeSubmittedRecords.Name_JNR
),
EXP_OutputFilter AS (
	SELECT
	WCTrackHistoryID,
	ExtractDate,
	Auditid,
	EntityType,
	EntityOtherType,
	AOIEntityType,
	AOIEntityOtherType,
	PartyAssociationType,
	PartyAssociationType_JNR,
	FEIN,
	DoingBusinessAs,
	Name,
	Name_JNR,
	PhoneNumber,
	BureauNumber,
	WC_LocationId,
	BusinessOrIndividual,
	FirstName,
	MiddleName,
	LastName,
	BureauUnemploymentNumberState,
	StateUnemploymentNumber,
	PolicyKey,
	Deleted,
	-- *INF*: DECODE(TRUE,
	-- Deleted='0','0',
	-- '1')
	DECODE(
	    TRUE,
	    Deleted = '0', '0',
	    '1'
	) AS o_Deleted,
	TransactionType,
	Email,
	PolKey_Name,
	HistoryID_Name,
	PartyAssociationType_Name,
	Name_Name,
	Deleted_Name,
	-- *INF*: DECODE(TRUE,
	-- Deleted_Name='1','1',
	-- IN(TransactionType,'New','Reissue','Renew','Rewrite') AND Deleted='1','1',
	-- '0')
	DECODE(
	    TRUE,
	    Deleted_Name = '1', '1',
	    TransactionType IN ('New','Reissue','Renew','Rewrite') AND Deleted = '1', '1',
	    '0'
	) AS FilterFlag
	FROM JNR_DeletedTxns
),
FIL_Output AS (
	SELECT
	WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	EntityType, 
	EntityOtherType, 
	AOIEntityType, 
	AOIEntityOtherType, 
	PartyAssociationType, 
	PartyAssociationType_JNR, 
	FEIN, 
	DoingBusinessAs, 
	Name, 
	Name_JNR, 
	PhoneNumber, 
	BureauNumber, 
	WC_LocationId, 
	BusinessOrIndividual, 
	FirstName, 
	MiddleName, 
	LastName, 
	BureauUnemploymentNumberState, 
	StateUnemploymentNumber, 
	o_Deleted AS Deleted, 
	Email, 
	FilterFlag
	FROM EXP_OutputFilter
	WHERE FilterFlag='0'
),
WorkWCParty AS (
	TRUNCATE TABLE WorkWCParty;
	INSERT INTO WorkWCParty
	(Auditid, ExtractDate, WCTrackHistoryID, EntityType, EntityOtherType, AOIEntityType, AOIEntityOtherType, PartyAssociationType, FEIN, DoingBusinessAs, Name, PhoneNumber, BureauNumber, WC_LocationId, BusinessOrIndividual, FirstName, MiddleName, LastName, BureauUnemploymentNumberState, StateUnemploymentNumber, Deleted, Email)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	ENTITYTYPE, 
	ENTITYOTHERTYPE, 
	AOIENTITYTYPE, 
	AOIENTITYOTHERTYPE, 
	PARTYASSOCIATIONTYPE, 
	FEIN, 
	DOINGBUSINESSAS, 
	NAME, 
	PHONENUMBER, 
	BUREAUNUMBER, 
	WC_LOCATIONID, 
	BUSINESSORINDIVIDUAL, 
	FIRSTNAME, 
	MIDDLENAME, 
	LASTNAME, 
	BUREAUUNEMPLOYMENTNUMBERSTATE, 
	STATEUNEMPLOYMENTNUMBER, 
	DELETED, 
	EMAIL
	FROM FIL_Output
),