WITH
LKP_SupWCPols_StateCode AS (
	SELECT
	WCPOLSCode,
	SourcesystemID,
	SourceCode,
	TableName,
	ProcessName
	FROM (
		SELECT 
			WCPOLSCode,
			SourcesystemID,
			SourceCode,
			TableName,
			ProcessName
		FROM SupWCPOLS
		WHERE Tablename='WCPOLS40Record' AND ProcessName='StateCodeRecord40' and SourceSystemId='DCT' and CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceCode ORDER BY WCPOLSCode) = 1
),
SQ_WCPols00Record AS (
	SELECT
		WCTrackHistoryID,
		LinkData,
	      AuditId
	FROM dbo.WCPols00Record
	WHERE 
	 AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
SQ_WorkWCTrackHistory AS (
	SELECT 
	DISTINCT
	Track.WCTrackHistoryID as WCTrackHistoryID, 
	'WC000303' as ParsedFormName,
	Forms.FormName as FormName, 
	ST.EmployersLiabilityCoverageEndorsementStateListExcludingOH as EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	Party.Name as Name,
	Policy.TransactionEffectiveDate as TransactionEffectiveDate  
	FROM
	WorkWCForms Forms
	inner join WorkWCTrackHistory Track 	on 
		Forms.WCTrackHistoryID=Track.WCTrackHistoryID and 
		Forms.FormName like 'WC000303%' and 
		((Forms.OnPolicy=1 OR Forms.[Add] = 1) AND (Forms.Remove is null OR Forms.Remove = 0))
	inner join WorkWCParty Party on 
		Party.WCTrackHistoryID=Track.WCTrackHistoryID AND 
		Party.PartyAssociationType='Account'
	inner join WorkWCPolicy Policy on 
		Policy.WCTrackHistoryID=Track.WCTrackHistoryID
	inner join WorkWCStateTerm ST on
		ST.WCTrackHistoryID=Track.WCTrackHistoryID AND ST.EmployersLiabilityCoverageEndorsementStateListExcludingOH is not null
	WHERE
	Forms.Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_40}
	order by 1
),
JNR_DataCollect AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCTrackHistory.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCTrackHistory.ParsedFormName, 
	SQ_WorkWCTrackHistory.FormName, 
	SQ_WorkWCTrackHistory.EmployersLiabilityCoverageEndorsementStateListExcludingOH, 
	SQ_WorkWCTrackHistory.Name, 
	SQ_WorkWCTrackHistory.TransactionEffectiveDate
	FROM SQ_WCPols00Record
	INNER JOIN SQ_WorkWCTrackHistory
	ON SQ_WorkWCTrackHistory.WCTrackHistoryID = SQ_WCPols00Record.WCTrackHistoryID
),
EXP_PrepSplit AS (
	SELECT
	EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	',' AS Delimiter
	FROM JNR_DataCollect
),
jtx_split_string AS (-- jtx_split_string

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
mplt_Parse_FormNameField AS (WITH
	INPUT_FormName AS (
		
	),
	EXPTRANS AS (
		SELECT
		ParsedNameOfForm,
		FormNameFromSource,
		-- *INF*: REVERSE(FormNameFromSource)
		REVERSE(FormNameFromSource) AS vReversedFromNameFromSource,
		-- *INF*: REVERSE(substr(vReversedFromNameFromSource,1,4))
		REVERSE(substr(vReversedFromNameFromSource, 1, 4)) AS vFormEdition,
		-- *INF*: DECODE(TRUE,
		-- substr(vReversedFromNameFromSource,5,1) >='A' and substr(vReversedFromNameFromSource,5,1) <='Z', substr(vReversedFromNameFromSource,5,1),
		-- ' '
		-- )
		-- 
		-- -- check if within A and Z, if not then space
		DECODE(
		    TRUE,
		    substr(vReversedFromNameFromSource, 5, 1) >= 'A' and substr(vReversedFromNameFromSource, 5, 1) <= 'Z', substr(vReversedFromNameFromSource, 5, 1),
		    ' '
		) AS vBureauCode,
		vFormEdition AS oFormEdition,
		vBureauCode AS oBureauCode
		FROM INPUT_FormName
	),
	OUTPUT_FormName AS (
		SELECT
		ParsedNameOfForm, 
		FormNameFromSource, 
		oFormEdition AS FormEdition, 
		oBureauCode AS BureauCode
		FROM EXPTRANS
	),
),
EXP_DataCollect AS (
	SELECT
	JNR_DataCollect.WCTrackHistoryID,
	JNR_DataCollect.AuditId,
	CURRENT_TIMESTAMP AS ExtractDate,
	JNR_DataCollect.LinkData,
	'40' AS RecordTypeCode,
	mplt_Parse_FormNameField.ParsedNameOfForm1 AS ParsedNameOfForm,
	mplt_Parse_FormNameField.FormEdition,
	mplt_Parse_FormNameField.BureauCode,
	JNR_DataCollect.Name,
	JNR_DataCollect.TransactionEffectiveDate AS i_TransactionEffectiveDate,
	-- *INF*: TO_CHAR(i_TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(i_TransactionEffectiveDate, 'YYMMDD') AS o_TransactionEffectiveDate,
	jtx_split_string.OUTPUT_Field1 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	-- *INF*: IIF(isnull(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	jtx_split_string.OUTPUT_Field2 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH2,
	jtx_split_string.OUTPUT_Field3 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH3,
	jtx_split_string.OUTPUT_Field4 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH4,
	jtx_split_string.OUTPUT_Field5 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH5,
	jtx_split_string.OUTPUT_Field6 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH6,
	jtx_split_string.OUTPUT_Field7 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH7,
	jtx_split_string.OUTPUT_Field8 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH8,
	jtx_split_string.OUTPUT_Field9 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH9,
	jtx_split_string.OUTPUT_Field10 AS i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10,
	-- *INF*: IIF(ISNULL(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10),'',:LKP.LKP_SUPWCPOLS_STATECODE(i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10))
	IFF(
	    i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10 IS NULL, '',
	    LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10.WCPOLSCode
	) AS o_EmployersLiabilityCoverageEndorsementStateListExcludingOH10
	FROM JNR_DataCollect
	 -- Manually join with jtx_split_string
	 -- Manually join with mplt_Parse_FormNameField
	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH2

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH3

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH4

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH5

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH6

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH7

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH8

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH9

	LEFT JOIN LKP_SUPWCPOLS_STATECODE LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10
	ON LKP_SUPWCPOLS_STATECODE_i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10.SourceCode = i_EmployersLiabilityCoverageEndorsementStateListExcludingOH10

),
WCPols40Record AS (
	INSERT INTO WCPols40Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, StateCode, StateCode2, StateCode3, StateCode4, StateCode5, StateCode6, StateCode7, StateCode8, StateCode9, StateCode10, NameOfInsured, EndorsementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	RECORDTYPECODE, 
	ParsedNameOfForm AS ENDORSEMENTNUMBER, 
	BureauCode AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER, 
	FormEdition AS CARRIERVERSIONIDENTIFIER, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH AS STATECODE, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH2 AS STATECODE2, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH3 AS STATECODE3, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH4 AS STATECODE4, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH5 AS STATECODE5, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH6 AS STATECODE6, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH7 AS STATECODE7, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH8 AS STATECODE8, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH9 AS STATECODE9, 
	o_EmployersLiabilityCoverageEndorsementStateListExcludingOH10 AS STATECODE10, 
	Name AS NAMEOFINSURED, 
	o_TransactionEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_DataCollect
),