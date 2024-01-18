WITH
SQ_DC_Location_Deleted AS (
	select DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	(ISNULL(DLOC.Address1,'')) Address1,
	(ISNULL(DLOC.Address2,'')) Address2,
	(ISNULL(DLOC.City,'')) City,
	(ISNULL(DLOC.County,'')) County,
	(ISNULL(DLOC.StateProv,'')) StateProv,
	ISNULL(DLOC.PostalCode,'') PostalCode,
	(ISNULL(DLOC.Country,'')) Country, 
	CASE WHEN DLOC.Deleted='1' THEN '1' ELSE '0' END LocationDeletedIndicator,
	(ISNULL(DLOC.Description,'')) LocationDescription,
	(ISNULL(DLOCA.LocationAssociationType,'')) LocationType,
	WP.PolicyNumber+WP.PolicyVersionFormatted PolicyKey
	
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with  (NOLOCK)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with (NOLOCK)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with (NOLOCK)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with (NOLOCK)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location DLOC with (NOLOCK)
	on DP.SessionId=DLOC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Location WLOC with (NOLOCK)
	on DLOC.SessionId=WLOC.SessionId
	and DLOC.LocationId=WLOC.LocationId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLOCA with (NOLOCK)
	on DLOC.SessionId=DLOCA.SessionId
	and DLOC.LocationId=DLOCA.LocationId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with (NOLOCK)
	on WP.SessionId=DL.SessionId
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History H with (NOLOCK)
	on H.HistoryID=DT.HistoryID
	and H.DeprecatedBy IS NULL
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLOCAWC with (NOLOCK)
	on DLOCA.SessionId=DLOCAWC.SessionId
	and DLOCA.LocationId=DLOCAWC.LocationId
	and DLOCAWC.ObjectName='DC_WC_Location'
	inner JOIN
	(Select distinct WP.PolicyNumber+WP.PolicyVersionFormatted PolKey,LocationXmlId from WB_Policy WP
	inner join DC_Transaction T with (NOLOCK)
	on WP.SessionId=T.SessionId
	inner join DC_Line DL with (NOLOCK)
	on T.Sessionid=DL.Sessionid
	inner join DC_Session S
	on WP.SessionID=S.SessionID
	inner join dbo.DC_Location LOC with (NOLOCK)
	on S.SessionId=LOC.SessionId and LOC.Deleted='1'
	where S.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and T.State='Committed'
	and DL.Type='WorkersCompensation'
	and S.Purpose='Onset'
	and T.State='Committed'
	and LOC.Deleted='1'
	@{pipeline().parameters.WHERE_CLAUSE_DELETED}
	) D
	on D.PolKey=(WP.PolicyNumber+WP.PolicyVersionFormatted)
	and D.LocationXmlId=DLOC.LocationXmlId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and H.DeprecatedBy IS NULL
	and DT.State='Committed'
	and DLOCA.ObjectName<>'DC_WC_Location'
	and DLOC.Deleted='1'
),
EXP_Source AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	LocationDeletedIndicator,
	LocationDescription,
	LocationType,
	PolicyKey
	FROM SQ_DC_Location_Deleted
),
LKP_TrackHistory AS (
	SELECT
	HistoryID,
	PolicyKey,
	IN_PolicyKey,
	IN_HistoryID
	FROM (
		Select distinct HistoryID as HistoryID, PolicyKey as PolicyKey  from WorkWCTrackHistory
		where AuditID<>@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		order by 2,1--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,HistoryID ORDER BY HistoryID) = 1
),
EXP_Remove AS (
	SELECT
	EXP_Source.HistoryID,
	EXP_Source.Purpose,
	EXP_Source.SessionId,
	EXP_Source.Address1,
	EXP_Source.Address2,
	EXP_Source.City,
	EXP_Source.County,
	EXP_Source.StateProv,
	EXP_Source.PostalCode,
	EXP_Source.Country,
	EXP_Source.LocationDeletedIndicator,
	EXP_Source.LocationDescription,
	EXP_Source.LocationType,
	EXP_Source.PolicyKey,
	LKP_TrackHistory.HistoryID AS LKP_HistoryID,
	LKP_TrackHistory.PolicyKey AS LKP_PolicyKey,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(LKP_HistoryID))),'NEW','EXISTS')
	IFF(LTRIM(RTRIM(LKP_HistoryID)) IS NULL, 'NEW', 'EXISTS') AS FilterFlag
	FROM EXP_Source
	LEFT JOIN LKP_TrackHistory
	ON LKP_TrackHistory.PolicyKey = EXP_Source.PolicyKey AND LKP_TrackHistory.HistoryID = EXP_Source.HistoryID
),
FIL_NewTxns AS (
	SELECT
	HistoryID, 
	Purpose, 
	SessionId, 
	Address1, 
	Address2, 
	City, 
	County, 
	StateProv, 
	PostalCode, 
	Country, 
	LocationDeletedIndicator, 
	LocationDescription, 
	LocationType, 
	PolicyKey, 
	FilterFlag
	FROM EXP_Remove
	WHERE LTRIM(RTRIM(FilterFlag))='EXISTS'
),
SRT_MaxHistID AS (
	SELECT
	PolicyKey, 
	HistoryID, 
	Purpose, 
	SessionId, 
	Address1, 
	Address2, 
	City, 
	County, 
	StateProv, 
	PostalCode, 
	Country, 
	LocationDeletedIndicator, 
	LocationDescription, 
	LocationType, 
	FilterFlag
	FROM FIL_NewTxns
	ORDER BY PolicyKey ASC, HistoryID DESC
),
EXP_ExistingTxns AS (
	SELECT
	PolicyKey,
	HistoryID,
	Purpose,
	SessionId,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	LocationDeletedIndicator,
	LocationDescription,
	LocationType,
	-- *INF*: DECODE(TRUE,
	-- PolicyKey<>v_PriorPolicyKey,HistoryID,
	-- PolicyKey=v_PriorPolicyKey and HistoryID=v_MaxHistID,v_MaxHistID,
	-- 0)
	DECODE(
	    TRUE,
	    PolicyKey <> v_PriorPolicyKey, HistoryID,
	    PolicyKey = v_PriorPolicyKey and HistoryID = v_MaxHistID, v_MaxHistID,
	    0
	) AS v_MaxHistID,
	PolicyKey AS v_PriorPolicyKey,
	HistoryID AS v_PriorHistoryID,
	-- *INF*: IIF(HistoryID=v_MaxHistID,'1','0')
	IFF(HistoryID = v_MaxHistID, '1', '0') AS v_MaxHistIDFilterFlag,
	v_MaxHistIDFilterFlag AS MaxHistIDFilterFlag
	FROM SRT_MaxHistID
),
FIL_MaxHistID AS (
	SELECT
	PolicyKey, 
	HistoryID, 
	Purpose, 
	SessionId, 
	Address1, 
	Address2, 
	City, 
	County, 
	StateProv, 
	PostalCode, 
	Country, 
	LocationDeletedIndicator, 
	LocationDescription, 
	LocationType, 
	MaxHistIDFilterFlag
	FROM EXP_ExistingTxns
	WHERE MaxHistIDFilterFlag='1'
),
EXP_Comp AS (
	SELECT
	PolicyKey,
	HistoryID,
	Purpose,
	SessionId,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	-- *INF*: SUBSTR(LTRIM(RTRIM(PostalCode)),1,5)
	SUBSTR(LTRIM(RTRIM(PostalCode)), 1, 5) AS v_PostalCode,
	Country,
	LocationDeletedIndicator,
	LocationDescription,
	LocationType,
	MaxHistIDFilterFlag,
	-- *INF*: LTRIM(RTRIM(Address1))||LTRIM(RTRIM(Address2))||LTRIM(RTRIM(City))||LTRIM(RTRIM(County))||LTRIM(RTRIM(StateProv))||LTRIM(RTRIM(v_PostalCode))||LTRIM(RTRIM(Country))||LTRIM(RTRIM(LocationDescription))||LTRIM(RTRIM(LocationType))
	LTRIM(RTRIM(Address1)) || LTRIM(RTRIM(Address2)) || LTRIM(RTRIM(City)) || LTRIM(RTRIM(County)) || LTRIM(RTRIM(StateProv)) || LTRIM(RTRIM(v_PostalCode)) || LTRIM(RTRIM(Country)) || LTRIM(RTRIM(LocationDescription)) || LTRIM(RTRIM(LocationType)) AS v_Location_Compare,
	-- *INF*: LTRIM(RTRIM(v_Location_Compare))
	LTRIM(RTRIM(v_Location_Compare)) AS Location_Compare
	FROM FIL_MaxHistID
),
SQ_DC_Location AS (
	select DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	DLOC.Address1,
	DLOC.Address2,
	DLOC.City,
	DLOC.County,
	DLOC.StateProv,
	DLOC.PostalCode,
	DLOC.Country,
	DLOC.Deleted LocationDeletedIndicator,
	DLOC.Description LocationDescription,
	Wloc.LocationNumber,
	DLOCA.LocationAssociationType LocationType,
	DLOC.LocationId,
	DLOCAWC.Objectid DC_WC_LocationId,
	WP.PolicyNumber+WP.PolicyVersionFormatted PolicyKey,
	DT.Type as TransactionType
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location DLOC with(nolock)
	on DP.SessionId=DLOC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Location WLOC with(Nolock)
	on DLOC.SessionId=WLOC.SessionId
	and DLOC.LocationId=WLOC.LocationId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with(Nolock)
	on WP.SessionID=DP.SessionID
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLOCA with(nolock)
	on DLOC.SessionId=DLOCA.SessionId
	and DLOC.LocationId=DLOCA.LocationId
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLOCAWC with(nolock)
	on DLOCA.SessionId=DLOCAWC.SessionId
	and DLOCA.LocationId=DLOCAWC.LocationId
	and DLOCAWC.ObjectName='DC_WC_Location'
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	and DLOCA.ObjectName<>'DC_WC_Location'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	LocationDeletedIndicator,
	LocationDescription,
	LocationNumber,
	LocationType,
	LocationId,
	WC_LocationId,
	PolicyKey,
	TransactionType
	FROM SQ_DC_Location
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
	EXP_SRCDataCollect.Address1,
	EXP_SRCDataCollect.Address2,
	EXP_SRCDataCollect.City,
	EXP_SRCDataCollect.County,
	EXP_SRCDataCollect.StateProv,
	EXP_SRCDataCollect.PostalCode,
	EXP_SRCDataCollect.Country,
	EXP_SRCDataCollect.LocationDeletedIndicator,
	EXP_SRCDataCollect.LocationDescription,
	EXP_SRCDataCollect.LocationNumber,
	EXP_SRCDataCollect.LocationType,
	EXP_SRCDataCollect.LocationId,
	EXP_SRCDataCollect.WC_LocationId,
	LKP_LatestSession.SessionId AS lkp_SessionId,
	EXP_SRCDataCollect.PolicyKey,
	EXP_SRCDataCollect.TransactionType
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
	Address1, 
	Address2, 
	City, 
	County, 
	StateProv, 
	PostalCode, 
	Country, 
	LocationDeletedIndicator, 
	LocationDescription, 
	LocationNumber, 
	LocationType, 
	LocationId, 
	WC_LocationId, 
	PolicyKey, 
	TransactionType
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
EXP_Compare AS (
	SELECT
	WCTrackHistoryID,
	ExtractDate,
	Auditid,
	FilterFlag,
	Address1,
	-- *INF*: IIF(ISNULL(Address1),'',Address1)
	IFF(Address1 IS NULL, '', Address1) AS v_Address1,
	Address2,
	-- *INF*: IIF(ISNULL(Address2),'',Address2)
	IFF(Address2 IS NULL, '', Address2) AS v_Address2,
	City,
	-- *INF*: IIF(ISNULL(City),'',City)
	IFF(City IS NULL, '', City) AS v_City,
	County,
	-- *INF*: IIF(ISNULL(County),'',County)
	IFF(County IS NULL, '', County) AS v_County,
	StateProv,
	-- *INF*: IIF(ISNULL(StateProv),'',StateProv)
	IFF(StateProv IS NULL, '', StateProv) AS v_StateProv,
	PostalCode,
	-- *INF*: IIF(ISNULL(PostalCode),'',SUBSTR(PostalCode,1,5))
	IFF(PostalCode IS NULL, '', SUBSTR(PostalCode, 1, 5)) AS v_PostalCode,
	Country,
	-- *INF*: IIF(ISNULL(Country),'',Country)
	IFF(Country IS NULL, '', Country) AS v_Country,
	LocationDeletedIndicator,
	LocationDescription,
	-- *INF*: IIF(ISNULL(LocationDescription),'',LocationDescription)
	IFF(LocationDescription IS NULL, '', LocationDescription) AS v_LocationDescription,
	LocationNumber,
	LocationType,
	-- *INF*: IIF(ISNULL(LocationType),'',LocationType)
	IFF(LocationType IS NULL, '', LocationType) AS v_LocationType,
	LocationId,
	WC_LocationId,
	PolicyKey,
	-- *INF*: LTRIM(RTRIM(v_Address1))||LTRIM(RTRIM(v_Address2))||LTRIM(RTRIM(v_City))||LTRIM(RTRIM(v_County))||LTRIM(RTRIM(v_StateProv))||LTRIM(RTRIM(v_PostalCode))||LTRIM(RTRIM(v_Country))||LTRIM(RTRIM(v_LocationDescription))||LTRIM(RTRIM(v_LocationType))
	LTRIM(RTRIM(v_Address1)) || LTRIM(RTRIM(v_Address2)) || LTRIM(RTRIM(v_City)) || LTRIM(RTRIM(v_County)) || LTRIM(RTRIM(v_StateProv)) || LTRIM(RTRIM(v_PostalCode)) || LTRIM(RTRIM(v_Country)) || LTRIM(RTRIM(v_LocationDescription)) || LTRIM(RTRIM(v_LocationType)) AS v_Location_Compare,
	TransactionType,
	-- *INF*: LTRIM(RTRIM(v_Location_Compare))
	LTRIM(RTRIM(v_Location_Compare)) AS Location_Compare
	FROM FIL_ExcludeSubmittedRecords
),
JNR_Deleted AS (SELECT
	EXP_Comp.PolicyKey AS PolicyKey_Deleted, 
	EXP_Comp.Location_Compare AS Location_Compare_Deleted, 
	EXP_Comp.LocationDeletedIndicator AS LocationDeletedIndicator_Deleted, 
	EXP_Compare.WCTrackHistoryID, 
	EXP_Compare.ExtractDate, 
	EXP_Compare.Auditid, 
	EXP_Compare.Address1, 
	EXP_Compare.Address2, 
	EXP_Compare.City, 
	EXP_Compare.County, 
	EXP_Compare.StateProv, 
	EXP_Compare.PostalCode, 
	EXP_Compare.Country, 
	EXP_Compare.LocationDeletedIndicator, 
	EXP_Compare.LocationDescription, 
	EXP_Compare.LocationNumber, 
	EXP_Compare.LocationType, 
	EXP_Compare.LocationId, 
	EXP_Compare.WC_LocationId, 
	EXP_Compare.PolicyKey, 
	EXP_Compare.TransactionType, 
	EXP_Compare.Location_Compare
	FROM EXP_Compare
	LEFT OUTER JOIN EXP_Comp
	ON EXP_Comp.Location_Compare = EXP_Compare.Location_Compare AND EXP_Comp.PolicyKey = EXP_Compare.PolicyKey
),
EXP_Output AS (
	SELECT
	WCTrackHistoryID,
	ExtractDate,
	Auditid,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	LocationDeletedIndicator,
	LocationDescription,
	LocationNumber,
	LocationType,
	LocationId,
	WC_LocationId,
	PolicyKey,
	TransactionType,
	Location_Compare,
	PolicyKey_Deleted,
	Location_Compare_Deleted,
	LocationDeletedIndicator_Deleted,
	-- *INF*: DECODE(TRUE,
	-- LocationDeletedIndicator_Deleted='1','1',
	-- IN(TransactionType,'New','Reissue','Renew','Rewrite') AND LocationDeletedIndicator='T','1',
	-- '0')
	DECODE(
	    TRUE,
	    LocationDeletedIndicator_Deleted = '1', '1',
	    TransactionType IN ('New','Reissue','Renew','Rewrite') AND LocationDeletedIndicator = 'T', '1',
	    '0'
	) AS Filter_Flag
	FROM JNR_Deleted
),
FIL_Target AS (
	SELECT
	Auditid, 
	ExtractDate, 
	WCTrackHistoryID, 
	Address1, 
	Address2, 
	City, 
	County, 
	StateProv, 
	PostalCode, 
	Country, 
	LocationDeletedIndicator, 
	LocationDescription, 
	LocationNumber, 
	LocationType, 
	LocationId, 
	WC_LocationId, 
	PolicyKey, 
	Location_Compare, 
	PolicyKey_Deleted, 
	Location_Compare_Deleted, 
	LocationDeletedIndicator_Deleted, 
	Filter_Flag
	FROM EXP_Output
	WHERE Filter_Flag='0'
),
WorkWCLocation AS (
	TRUNCATE TABLE WorkWCLocation;
	INSERT INTO WorkWCLocation
	(Auditid, ExtractDate, WCTrackHistoryID, Address1, Address2, City, County, StateProv, PostalCode, Country, LocationDeletedIndicator, LocationDescription, LocationNumber, LocationType, LocationId, WC_LocationId)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	ADDRESS1, 
	ADDRESS2, 
	CITY, 
	COUNTY, 
	STATEPROV, 
	POSTALCODE, 
	COUNTRY, 
	LOCATIONDELETEDINDICATOR, 
	LOCATIONDESCRIPTION, 
	LOCATIONNUMBER, 
	LOCATIONTYPE, 
	LOCATIONID, 
	WC_LOCATIONID
	FROM FIL_Target
),