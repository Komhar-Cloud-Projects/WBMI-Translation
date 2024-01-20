WITH
LKP_SupWCPOLS AS (
	SELECT
	WCPOLSCode,
	SourcesystemID,
	SourceCode,
	TableName,
	ProcessName,
	i_SourcesystemID,
	i_SourceCode,
	i_TableName,
	i_ProcessName
	FROM (
		SELECT
		     WCPOLSCode as WCPOLSCode
			,SourcesystemID as SourcesystemID
			,SourceCode as SourceCode
			,TableName as TableName
			,ProcessName as ProcessName
		FROM SupWCPOLS
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourcesystemID,SourceCode,TableName,ProcessName ORDER BY WCPOLSCode) = 1
),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	     AuditId
	FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
SQ_WorkWCForms AS (
	SELECT DISTINCT
		ST.WCTrackHistoryID
		,F.FormName
		,ST.[State]
		,L.AnniversaryRatingDate
		,PT.Name
		,Pol.TransactionEffectiveDate
		
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	AND F.FormName like 'WC000402%' AND (F.OnPolicy=1 or F.[Add]=1) AND (F.Remove is NULL or F.Remove=0)
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy Pol
		ON Pol.WCTrackHistoryID = ST.WCTrackHistoryID
	
	WHERE 1 = 1
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_09}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_09_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCForms.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms.FormName, 
	SQ_WorkWCForms.State, 
	SQ_WorkWCForms.AnniversaryRatingDate, 
	SQ_WorkWCForms.Name, 
	SQ_WorkWCForms.TransactionEffectiveDate
	FROM SQ_WorkWCForms
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = SQ_WorkWCForms.WCTrackHistoryID
),
EXP_09_Format_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS o_ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	FormName,
	State,
	-- *INF*: :LKP.LKP_SupWCPOLS('DCT',State,'WCPOLS09Record','StateCodeRecord09')
	-- 
	-- 
	-- --IIF((IsNull(FoundFlag) and StateCount > 1),'99', :LKP.LKP_SupWCPOLS('DCT',State,'WCPOLS09Record','StateCodeRecord09'))
	LKP_SUPWCPOLS__DCT_State_WCPOLS09Record_StateCodeRecord09.WCPOLSCode AS o_StateCode,
	'09' AS o_RecordTypeCode,
	'WC000402' AS o_EndorsementNumber,
	-- *INF*: SUBSTR(FormName, Length(FormName)-4, 1)
	SUBSTR(FormName, Length(FormName) - 4, 1) AS v_BureauID,
	-- *INF*: IIF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID,' ')
	IFF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID, ' ') AS o_BureauVersionIdentifierEditionIdentifier,
	-- *INF*: SUBSTR(FormName, Length(FormName)-3, 4)
	SUBSTR(FormName, Length(FormName) - 3, 4) AS o_CarrierVersionIdentifier,
	-- *INF*: To_Char(ExperienceModEffectiveDate,'YYMMDD')
	To_Char(ExperienceModEffectiveDate, 'YYMMDD') AS o_ContingentModificationEffectiveDate,
	TransactionEffectiveDate,
	-- *INF*: To_Char(TransactionEffectiveDate, 'YYMMDD')
	To_Char(TransactionEffectiveDate, 'YYMMDD') AS o_EndorsementEffectiveDate,
	Name AS NameOfInsured,
	AnniversaryRatingDate,
	-- *INF*: To_Char(AnniversaryRatingDate, 'YYMMDD')
	To_Char(AnniversaryRatingDate, 'YYMMDD') AS o_AnniversaryRatingDate
	FROM JNR_09_Record
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_State_WCPOLS09Record_StateCodeRecord09
	ON LKP_SUPWCPOLS__DCT_State_WCPOLS09Record_StateCodeRecord09.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS09Record_StateCodeRecord09.SourceCode = State
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS09Record_StateCodeRecord09.TableName = 'WCPOLS09Record'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS09Record_StateCodeRecord09.ProcessName = 'StateCodeRecord09'

),
WCPols09Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols09Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols09Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, AnniversaryRatingDate, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	o_StateCode AS STATECODE, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_EndorsementNumber AS ENDORSEMENTNUMBER, 
	o_BureauVersionIdentifierEditionIdentifier AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	o_CarrierVersionIdentifier AS CARRIERVERSIONIDENTIFIER, 
	o_AnniversaryRatingDate AS ANNIVERSARYRATINGDATE, 
	NAMEOFINSURED, 
	o_EndorsementEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_09_Format_Output
),