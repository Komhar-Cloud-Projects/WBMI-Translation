WITH
LKP_SUPWCPOLS_State AS (
	SELECT
	WCPOLSCode,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MIRequiredFlag,
	MNRequiredFlag,
	SourcesystemID,
	TableName,
	ProcessName,
	SourceCode
	FROM (
		SELECT 
			WCPOLSCode,
			NCCIRequiredFlag,
			WIRequiredFlag,
			MIRequiredFlag,
			MNRequiredFlag,
			SourcesystemID,
			TableName,
			ProcessName,
			SourceCode
		FROM SupWCPOLS
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourcesystemID,TableName,ProcessName,SourceCode ORDER BY WCPOLSCode) = 1
),
SQ_WorkWCForms AS (
	SELECT
	     C.WCTrackHistoryID
		,C.FormName
		,B.TransactionEffectiveDate
		,B.TransactionExpirationDate
	
	
	FROM dbo.WorkWCPolicy B
	
	INNER JOIN dbo.WorkWCForms C
		ON B.WCTrackHistoryID = C.WCTrackHistoryID
		AND (C.OnPolicy = '1'OR C.[Add] = '1')
		AND (C.[Remove] is NULL or C.[Remove] = '0')
		AND C.FormName is NOT NULL
	
	WHERE 1=1
	AND C.FormName like 'WC%'
	AND B.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_07}
	ORDER BY C.WCTrackHistoryID
),
EXP_StateCode AS (
	SELECT
	WCTrackHistoryID,
	FormName,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	-- *INF*: SUBSTR(FormName, Length(FormName)-4, 1)
	SUBSTR(FormName, Length(FormName) - 4, 1) AS v_BureauID,
	-- *INF*: IIF(v_BureauID >= 'A' and v_BureauID <= 'Z', SUBSTR(FormName,1, Length(FormName)-5),SUBSTR(FormName,1, Length(FormName)-4))
	IFF(
	    v_BureauID >= 'A' and v_BureauID <= 'Z', SUBSTR(FormName, 1, Length(FormName) - 5),
	    SUBSTR(FormName, 1, Length(FormName) - 4)
	) AS v_FormName,
	-- *INF*: :LKP.LKP_SUPWCPOLS_STATE('DCT','WCPOLS07Record','StateCodeRecord07',SUBSTR(FormName,3,2))
	LKP_SUPWCPOLS_STATE__DCT_WCPOLS07Record_StateCodeRecord07_SUBSTR_FormName_3_2.WCPOLSCode AS v_LKP_StateCode,
	-- *INF*: Decode(TRUE,
	-- v_FormName='WC400','Y',
	-- ISNULL(v_LKP_StateCode) and substr(FormName,3,2)='00','N',
	-- ISNULL(v_LKP_StateCode),'Y','N')
	Decode(
	    TRUE,
	    v_FormName = 'WC400', 'Y',
	    v_LKP_StateCode IS NULL and substr(FormName, 3, 2) = '00', 'N',
	    v_LKP_StateCode IS NULL, 'Y',
	    'N'
	) AS Form_FilterFlag
	FROM SQ_WorkWCForms
	LEFT JOIN LKP_SUPWCPOLS_STATE LKP_SUPWCPOLS_STATE__DCT_WCPOLS07Record_StateCodeRecord07_SUBSTR_FormName_3_2
	ON LKP_SUPWCPOLS_STATE__DCT_WCPOLS07Record_StateCodeRecord07_SUBSTR_FormName_3_2.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS_STATE__DCT_WCPOLS07Record_StateCodeRecord07_SUBSTR_FormName_3_2.TableName = 'WCPOLS07Record'
	AND LKP_SUPWCPOLS_STATE__DCT_WCPOLS07Record_StateCodeRecord07_SUBSTR_FormName_3_2.ProcessName = 'StateCodeRecord07'
	AND LKP_SUPWCPOLS_STATE__DCT_WCPOLS07Record_StateCodeRecord07_SUBSTR_FormName_3_2.SourceCode = SUBSTR(FormName, 3, 2)

),
FIL_Forms AS (
	SELECT
	WCTrackHistoryID, 
	FormName, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	Form_FilterFlag
	FROM EXP_StateCode
	WHERE Form_FilterFlag='N'
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
JNR_00_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	FIL_Forms.WCTrackHistoryID AS WCTrackHistoryID1, 
	FIL_Forms.FormName, 
	FIL_Forms.TransactionEffectiveDate, 
	FIL_Forms.TransactionExpirationDate
	FROM FIL_Forms
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = FIL_Forms.WCTrackHistoryID
),
SRT_Control_Break AS (
	SELECT
	WCTrackHistoryID, 
	LinkData, 
	AuditId, 
	FormName, 
	TransactionEffectiveDate, 
	TransactionExpirationDate
	FROM JNR_00_Record
	ORDER BY WCTrackHistoryID ASC, FormName ASC
),
EXP_Aggr_Format AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	AuditId,
	FormName,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	-- *INF*: TO_CHAR(WCTrackHistoryID) || SUBSTR(FormName,1,4)
	TO_CHAR(WCTrackHistoryID) || SUBSTR(FormName, 1, 4) AS Grouping_Key,
	Grouping_Key AS o_Grouping_Key,
	-- *INF*: IIF(SUBSTR(FormName,3,2)='00','  ',SUBSTR(FormName,3,2))
	IFF(SUBSTR(FormName, 3, 2) = '00', '  ', SUBSTR(FormName, 3, 2)) AS o_StateCode,
	-- *INF*: IIF (IsNull(old_Grouping_Key) or old_Grouping_Key <> Grouping_Key, 1, Grouping_Cnt + 1)
	IFF(old_Grouping_Key IS NULL or old_Grouping_Key <> Grouping_Key, 1, Grouping_Cnt + 1) AS Grouping_Cnt,
	-- *INF*: TRUNC((Grouping_Cnt - 1) / 11)
	TRUNC((Grouping_Cnt - 1) / 11) AS o_Record_Cnt,
	-- *INF*: IIF(MOD(Grouping_Cnt, 11) = 0, 11, MOD(Grouping_Cnt, 11))
	IFF(MOD(Grouping_Cnt, 11) = 0, 11, MOD(Grouping_Cnt, 11)) AS o_Field_Cnt,
	Grouping_Key AS old_Grouping_Key
	FROM SRT_Control_Break
),
EXPTRANS AS (
	SELECT
	o_Grouping_Key AS Grouping_Key,
	o_StateCode AS StateCode,
	o_Record_Cnt AS Record_Cnt,
	WCTrackHistoryID,
	LinkData,
	AuditId,
	FormName,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	o_Field_Cnt AS Field_Cnt,
	-- *INF*: IIF (Field_Cnt = 1, FormName, '')
	IFF(Field_Cnt = 1, FormName, '') AS o_FormName_01,
	-- *INF*: IIF (Field_Cnt = 2, FormName, '')
	IFF(Field_Cnt = 2, FormName, '') AS o_FormName_02,
	-- *INF*: IIF (Field_Cnt = 3, FormName, '')
	IFF(Field_Cnt = 3, FormName, '') AS o_FormName_03,
	-- *INF*: IIF (Field_Cnt = 4, FormName, '')
	IFF(Field_Cnt = 4, FormName, '') AS o_FormName_04,
	-- *INF*: IIF (Field_Cnt = 5, FormName, '')
	IFF(Field_Cnt = 5, FormName, '') AS o_FormName_05,
	-- *INF*: IIF (Field_Cnt = 6, FormName, '')
	IFF(Field_Cnt = 6, FormName, '') AS o_FormName_06,
	-- *INF*: IIF (Field_Cnt = 7, FormName, '')
	IFF(Field_Cnt = 7, FormName, '') AS o_FormName_07,
	-- *INF*: IIF (Field_Cnt = 8, FormName, '')
	IFF(Field_Cnt = 8, FormName, '') AS o_FormName_08,
	-- *INF*: IIF (Field_Cnt = 9, FormName, '')
	IFF(Field_Cnt = 9, FormName, '') AS o_FormName_09,
	-- *INF*: IIF (Field_Cnt = 10, FormName, '')
	IFF(Field_Cnt = 10, FormName, '') AS o_FormName_10,
	-- *INF*: IIF (Field_Cnt = 11, FormName, '')
	IFF(Field_Cnt = 11, FormName, '') AS o_FormName_11
	FROM EXP_Aggr_Format
),
SRT_Aggr_Records AS (
	SELECT
	Grouping_Key, 
	Record_Cnt, 
	StateCode, 
	WCTrackHistoryID, 
	LinkData, 
	AuditId, 
	TransactionEffectiveDate, 
	TransactionExpirationDate, 
	o_FormName_01 AS FormName_01, 
	o_FormName_02 AS FormName_02, 
	o_FormName_03 AS FormName_03, 
	o_FormName_04 AS FormName_04, 
	o_FormName_05 AS FormName_05, 
	o_FormName_06 AS FormName_06, 
	o_FormName_07 AS FormName_07, 
	o_FormName_08 AS FormName_08, 
	o_FormName_09 AS FormName_09, 
	o_FormName_10 AS FormName_10, 
	o_FormName_11 AS FormName_11
	FROM EXPTRANS
	ORDER BY Grouping_Key ASC, Record_Cnt ASC
),
AGG_07_Records AS (
	SELECT
	Grouping_Key,
	Record_Cnt,
	StateCode,
	WCTrackHistoryID,
	LinkData,
	AuditId,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	FormName_01,
	FormName_02,
	FormName_03,
	FormName_04,
	FormName_05,
	FormName_06,
	FormName_07,
	FormName_08,
	FormName_09,
	FormName_10,
	FormName_11,
	-- *INF*: MAX(FormName_01)
	MAX(FormName_01) AS o_FormName_01,
	-- *INF*: MAX(FormName_02)
	MAX(FormName_02) AS o_FormName_02,
	-- *INF*: MAX(FormName_03)
	MAX(FormName_03) AS o_FormName_03,
	-- *INF*: MAX(FormName_04)
	MAX(FormName_04) AS o_FormName_04,
	-- *INF*: MAX(FormName_05)
	MAX(FormName_05) AS o_FormName_05,
	-- *INF*: MAX(FormName_06)
	MAX(FormName_06) AS o_FormName_06,
	-- *INF*: MAX(FormName_07)
	MAX(FormName_07) AS o_FormName_07,
	-- *INF*: MAX(FormName_08)
	MAX(FormName_08) AS o_FormName_08,
	-- *INF*: MAX(FormName_09)
	MAX(FormName_09) AS o_FormName_09,
	-- *INF*: MAX(FormName_10)
	MAX(FormName_10) AS o_FormName_10,
	-- *INF*: MAX(FormName_11)
	MAX(FormName_11) AS o_FormName_11
	FROM SRT_Aggr_Records
	GROUP BY Grouping_Key, Record_Cnt
),
EXP_Format_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS o_ExtractDate,
	StateCode,
	WCTrackHistoryID,
	LinkData,
	AuditId,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	'07' AS o_RecordTypeCode,
	o_FormName_01 AS FormName_01,
	o_FormName_02 AS FormName_02,
	o_FormName_03 AS FormName_03,
	o_FormName_04 AS FormName_04,
	o_FormName_05 AS FormName_05,
	o_FormName_06 AS FormName_06,
	o_FormName_07 AS FormName_07,
	o_FormName_08 AS FormName_08,
	o_FormName_09 AS FormName_09,
	o_FormName_10 AS FormName_10,
	o_FormName_11 AS FormName_11,
	-- *INF*: IIF(v_BureauID_01 >= 'A' and v_BureauID_01 <= 'Z', SUBSTR(FormName_01,1, Length(FormName_01)-5),SUBSTR(FormName_01,1, Length(FormName_01)-4))
	-- 
	IFF(
	    v_BureauID_01 >= 'A' and v_BureauID_01 <= 'Z',
	    SUBSTR(FormName_01, 1, Length(FormName_01) - 5),
	    SUBSTR(FormName_01, 1, Length(FormName_01) - 4)
	) AS o_EndorsementNumber01,
	-- *INF*: SUBSTR(FormName_01, Length(FormName_01)-4, 1)
	SUBSTR(FormName_01, Length(FormName_01) - 4, 1) AS v_BureauID_01,
	-- *INF*: IIF(v_BureauID_01 >= 'A' and v_BureauID_01 <= 'Z', v_BureauID_01,' ')
	IFF(v_BureauID_01 >= 'A' and v_BureauID_01 <= 'Z', v_BureauID_01, ' ') AS o_BureauVersionIdentifierEditionIdentifier01,
	-- *INF*: SUBSTR(FormName_01, Length(FormName_01)-3, 4)
	SUBSTR(FormName_01, Length(FormName_01) - 3, 4) AS o_CarrierVersionIdentifier01,
	-- *INF*: IIF(v_BureauID_02 >= 'A' and v_BureauID_02 <= 'Z', SUBSTR(FormName_02,1, Length(FormName_02)-5),SUBSTR(FormName_02,1, Length(FormName_02)-4))
	IFF(
	    v_BureauID_02 >= 'A' and v_BureauID_02 <= 'Z',
	    SUBSTR(FormName_02, 1, Length(FormName_02) - 5),
	    SUBSTR(FormName_02, 1, Length(FormName_02) - 4)
	) AS o_EndorsementNumber02,
	-- *INF*: SUBSTR(FormName_02, Length(FormName_02)-4, 1)
	SUBSTR(FormName_02, Length(FormName_02) - 4, 1) AS v_BureauID_02,
	-- *INF*: IIF(v_BureauID_02 >= 'A' and v_BureauID_02 <= 'Z', v_BureauID_02,' ')
	IFF(v_BureauID_02 >= 'A' and v_BureauID_02 <= 'Z', v_BureauID_02, ' ') AS o_BureauVersionIdentifierEditionIdentifier02,
	-- *INF*: SUBSTR(FormName_02, Length(FormName_02)-3, 4)
	SUBSTR(FormName_02, Length(FormName_02) - 3, 4) AS o_CarrierVersionIdentifier02,
	-- *INF*: IIF(v_BureauID_03 >= 'A' and v_BureauID_03 <= 'Z', SUBSTR(FormName_03,1, Length(FormName_03)-5),SUBSTR(FormName_03,1, Length(FormName_03)-4))
	IFF(
	    v_BureauID_03 >= 'A' and v_BureauID_03 <= 'Z',
	    SUBSTR(FormName_03, 1, Length(FormName_03) - 5),
	    SUBSTR(FormName_03, 1, Length(FormName_03) - 4)
	) AS o_EndorsementNumber03,
	-- *INF*: SUBSTR(FormName_03, Length(FormName_03)-4, 1)
	SUBSTR(FormName_03, Length(FormName_03) - 4, 1) AS v_BureauID_03,
	-- *INF*: IIF(v_BureauID_03 >= 'A' and v_BureauID_03 <= 'Z', v_BureauID_03,' ')
	IFF(v_BureauID_03 >= 'A' and v_BureauID_03 <= 'Z', v_BureauID_03, ' ') AS o_BureauVersionIdentifierEditionIdentifier03,
	-- *INF*: SUBSTR(FormName_03, Length(FormName_03)-3, 4)
	SUBSTR(FormName_03, Length(FormName_03) - 3, 4) AS o_CarrierVersionIdentifier03,
	-- *INF*: IIF(v_BureauID_04 >= 'A' and v_BureauID_04 <= 'Z', SUBSTR(FormName_04,1, Length(FormName_04)-5),SUBSTR(FormName_04,1, Length(FormName_04)-4))
	IFF(
	    v_BureauID_04 >= 'A' and v_BureauID_04 <= 'Z',
	    SUBSTR(FormName_04, 1, Length(FormName_04) - 5),
	    SUBSTR(FormName_04, 1, Length(FormName_04) - 4)
	) AS o_EndorsementNumber04,
	-- *INF*: SUBSTR(FormName_04, Length(FormName_04)-4, 1)
	SUBSTR(FormName_04, Length(FormName_04) - 4, 1) AS v_BureauID_04,
	-- *INF*: IIF(v_BureauID_04 >= 'A' and v_BureauID_04 <= 'Z', v_BureauID_04,' ')
	IFF(v_BureauID_04 >= 'A' and v_BureauID_04 <= 'Z', v_BureauID_04, ' ') AS o_BureauVersionIdentifierEditionIdentifier04,
	-- *INF*: SUBSTR(FormName_04, Length(FormName_04)-3, 4)
	SUBSTR(FormName_04, Length(FormName_04) - 3, 4) AS o_CarrierVersionIdentifier04,
	-- *INF*: IIF(v_BureauID_05 >= 'A' and v_BureauID_05 <= 'Z', SUBSTR(FormName_05,1, Length(FormName_05)-5),SUBSTR(FormName_05,1, Length(FormName_05)-4))
	IFF(
	    v_BureauID_05 >= 'A' and v_BureauID_05 <= 'Z',
	    SUBSTR(FormName_05, 1, Length(FormName_05) - 5),
	    SUBSTR(FormName_05, 1, Length(FormName_05) - 4)
	) AS o_EndorsementNumber05,
	-- *INF*: SUBSTR(FormName_05, Length(FormName_05)-4, 1)
	SUBSTR(FormName_05, Length(FormName_05) - 4, 1) AS v_BureauID_05,
	-- *INF*: IIF(v_BureauID_05 >= 'A' and v_BureauID_05 <= 'Z', v_BureauID_05,' ')
	IFF(v_BureauID_05 >= 'A' and v_BureauID_05 <= 'Z', v_BureauID_05, ' ') AS o_BureauVersionIdentifierEditionIdentifier05,
	-- *INF*: SUBSTR(FormName_05, Length(FormName_05)-3, 4)
	SUBSTR(FormName_05, Length(FormName_05) - 3, 4) AS o_CarrierVersionIdentifier05,
	-- *INF*: IIF(v_BureauID_06 >= 'A' and v_BureauID_06 <= 'Z', SUBSTR(FormName_06,1, Length(FormName_06)-5),SUBSTR(FormName_06,1, Length(FormName_06)-4))
	IFF(
	    v_BureauID_06 >= 'A' and v_BureauID_06 <= 'Z',
	    SUBSTR(FormName_06, 1, Length(FormName_06) - 5),
	    SUBSTR(FormName_06, 1, Length(FormName_06) - 4)
	) AS o_EndorsementNumber06,
	-- *INF*: SUBSTR(FormName_06, Length(FormName_06)-4, 1)
	SUBSTR(FormName_06, Length(FormName_06) - 4, 1) AS v_BureauID_06,
	-- *INF*: IIF(v_BureauID_06 >= 'A' and v_BureauID_06 <= 'Z', v_BureauID_06,' ')
	IFF(v_BureauID_06 >= 'A' and v_BureauID_06 <= 'Z', v_BureauID_06, ' ') AS o_BureauVersionIdentifierEditionIdentifier06,
	-- *INF*: SUBSTR(FormName_06, Length(FormName_06)-3, 4)
	SUBSTR(FormName_06, Length(FormName_06) - 3, 4) AS o_CarrierVersionIdentifier06,
	-- *INF*: IIF(v_BureauID_07 >= 'A' and v_BureauID_07 <= 'Z', SUBSTR(FormName_07,1, Length(FormName_07)-5),SUBSTR(FormName_07,1, Length(FormName_07)-4))
	IFF(
	    v_BureauID_07 >= 'A' and v_BureauID_07 <= 'Z',
	    SUBSTR(FormName_07, 1, Length(FormName_07) - 5),
	    SUBSTR(FormName_07, 1, Length(FormName_07) - 4)
	) AS o_EndorsementNumber07,
	-- *INF*: SUBSTR(FormName_07, Length(FormName_07)-4, 1)
	SUBSTR(FormName_07, Length(FormName_07) - 4, 1) AS v_BureauID_07,
	-- *INF*: IIF(v_BureauID_07 >= 'A' and v_BureauID_07 <= 'Z', v_BureauID_07,' ')
	IFF(v_BureauID_07 >= 'A' and v_BureauID_07 <= 'Z', v_BureauID_07, ' ') AS o_BureauVersionIdentifierEditionIdentifier07,
	-- *INF*: SUBSTR(FormName_07, Length(FormName_07)-3, 4)
	SUBSTR(FormName_07, Length(FormName_07) - 3, 4) AS o_CarrierVersionIdentifier07,
	-- *INF*: IIF(v_BureauID_08 >= 'A' and v_BureauID_08 <= 'Z', SUBSTR(FormName_08,1, Length(FormName_08)-5),SUBSTR(FormName_08,1, Length(FormName_08)-4))
	IFF(
	    v_BureauID_08 >= 'A' and v_BureauID_08 <= 'Z',
	    SUBSTR(FormName_08, 1, Length(FormName_08) - 5),
	    SUBSTR(FormName_08, 1, Length(FormName_08) - 4)
	) AS o_EndorsementNumber08,
	-- *INF*: SUBSTR(FormName_08, Length(FormName_08)-4, 1)
	SUBSTR(FormName_08, Length(FormName_08) - 4, 1) AS v_BureauID_08,
	-- *INF*: IIF(v_BureauID_08 >= 'A' and v_BureauID_08 <= 'Z', v_BureauID_08,' ')
	IFF(v_BureauID_08 >= 'A' and v_BureauID_08 <= 'Z', v_BureauID_08, ' ') AS o_BureauVersionIdentifierEditionIdentifier08,
	-- *INF*: SUBSTR(FormName_08, Length(FormName_08)-3, 4)
	SUBSTR(FormName_08, Length(FormName_08) - 3, 4) AS o_CarrierVersionIdentifier08,
	-- *INF*: IIF(v_BureauID_09 >= 'A' and v_BureauID_09 <= 'Z', SUBSTR(FormName_09,1, Length(FormName_09)-5),SUBSTR(FormName_09,1, Length(FormName_09)-4))
	IFF(
	    v_BureauID_09 >= 'A' and v_BureauID_09 <= 'Z',
	    SUBSTR(FormName_09, 1, Length(FormName_09) - 5),
	    SUBSTR(FormName_09, 1, Length(FormName_09) - 4)
	) AS o_EndorsementNumber09,
	-- *INF*: SUBSTR(FormName_09, Length(FormName_09)-4, 1)
	SUBSTR(FormName_09, Length(FormName_09) - 4, 1) AS v_BureauID_09,
	-- *INF*: IIF(v_BureauID_09 >= 'A' and v_BureauID_09 <= 'Z', v_BureauID_09,' ')
	IFF(v_BureauID_09 >= 'A' and v_BureauID_09 <= 'Z', v_BureauID_09, ' ') AS o_BureauVersionIdentifierEditionIdentifier09,
	-- *INF*: SUBSTR(FormName_09, Length(FormName_09)-3, 4)
	SUBSTR(FormName_09, Length(FormName_09) - 3, 4) AS o_CarrierVersionIdentifier09,
	-- *INF*: IIF(v_BureauID_10 >= 'A' and v_BureauID_10 <= 'Z', SUBSTR(FormName_10,1, Length(FormName_10)-5),SUBSTR(FormName_10,1, Length(FormName_10)-4))
	IFF(
	    v_BureauID_10 >= 'A' and v_BureauID_10 <= 'Z',
	    SUBSTR(FormName_10, 1, Length(FormName_10) - 5),
	    SUBSTR(FormName_10, 1, Length(FormName_10) - 4)
	) AS o_EndorsementNumber10,
	-- *INF*: SUBSTR(FormName_10, Length(FormName_10)-4, 1)
	SUBSTR(FormName_10, Length(FormName_10) - 4, 1) AS v_BureauID_10,
	-- *INF*: IIF(v_BureauID_10 >= 'A' and v_BureauID_10 <= 'Z', v_BureauID_10,' ')
	IFF(v_BureauID_10 >= 'A' and v_BureauID_10 <= 'Z', v_BureauID_10, ' ') AS o_BureauVersionIdentifierEditionIdentifier10,
	-- *INF*: SUBSTR(FormName_10, Length(FormName_10)-3, 4)
	SUBSTR(FormName_10, Length(FormName_10) - 3, 4) AS o_CarrierVersionIdentifier10,
	-- *INF*: IIF(v_BureauID_11 >= 'A' and v_BureauID_11 <= 'Z', SUBSTR(FormName_11,1, Length(FormName_11)-5),SUBSTR(FormName_11,1, Length(FormName_11)-4))
	IFF(
	    v_BureauID_11 >= 'A' and v_BureauID_11 <= 'Z',
	    SUBSTR(FormName_11, 1, Length(FormName_11) - 5),
	    SUBSTR(FormName_11, 1, Length(FormName_11) - 4)
	) AS o_EndorsementNumber11,
	-- *INF*: SUBSTR(FormName_11, Length(FormName_11)-4, 1)
	SUBSTR(FormName_11, Length(FormName_11) - 4, 1) AS v_BureauID_11,
	-- *INF*: IIF(v_BureauID_11 >= 'A' and v_BureauID_11 <= 'Z', v_BureauID_11,' ')
	IFF(v_BureauID_11 >= 'A' and v_BureauID_11 <= 'Z', v_BureauID_11, ' ') AS o_BureauVersionIdentifierEditionIdentifier11,
	-- *INF*: SUBSTR(FormName_11, Length(FormName_11)-3, 4)
	SUBSTR(FormName_11, Length(FormName_11) - 3, 4) AS o_CarrierVersionIdentifier11,
	-- *INF*: TO_CHAR(TransactionEffectiveDate,'YYMMDD')
	TO_CHAR(TransactionEffectiveDate, 'YYMMDD') AS o_PolicyChangeEffectiveDate,
	-- *INF*: TO_CHAR(TransactionExpirationDate,'YYMMDD')
	TO_CHAR(TransactionExpirationDate, 'YYMMDD') AS o_PolicyChangeExpirationDate
	FROM AGG_07_Records
),
WCPols07Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols07Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols07Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber01, BureauVersionIdentifierEditionIdentifier01, CarrierVersionIdentifier01, EndorsementNumber02, BureauVersionIdentifierEditionIdentifier02, CarrierVersionIdentifier02, EndorsementNumber03, BureauVersionIdentifierEditionIdentifier03, CarrierVersionIdentifier03, EndorsementNumber04, BureauVersionIdentifierEditionIdentifier04, CarrierVersionIdentifier04, EndorsementNumber05, BureauVersionIdentifierEditionIdentifier05, CarrierVersionIdentifier05, EndorsementNumber06, BureauVersionIdentifierEditionIdentifier06, CarrierVersionIdentifier06, EndorsementNumber07, BureauVersionIdentifierEditionIdentifier07, CarrierVersionIdentifier07, EndorsementNumber08, BureauVersionIdentifierEditionIdentifier08, CarrierVersionIdentifier08, EndorsementNumber09, BureauVersionIdentifierEditionIdentifier09, CarrierVersionIdentifier09, EndorsementNumber10, BureauVersionIdentifierEditionIdentifier10, CarrierVersionIdentifier10, EndorsementNumber11, BureauVersionIdentifierEditionIdentifier11, CarrierVersionIdentifier11, PolicyChangeEffectiveDate, PolicyChangeExpirationDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	o_RecordTypeCode AS RECORDTYPECODE, 
	o_EndorsementNumber01 AS ENDORSEMENTNUMBER01, 
	o_BureauVersionIdentifierEditionIdentifier01 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER01, 
	o_CarrierVersionIdentifier01 AS CARRIERVERSIONIDENTIFIER01, 
	o_EndorsementNumber02 AS ENDORSEMENTNUMBER02, 
	o_BureauVersionIdentifierEditionIdentifier02 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER02, 
	o_CarrierVersionIdentifier02 AS CARRIERVERSIONIDENTIFIER02, 
	o_EndorsementNumber03 AS ENDORSEMENTNUMBER03, 
	o_BureauVersionIdentifierEditionIdentifier03 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER03, 
	o_CarrierVersionIdentifier03 AS CARRIERVERSIONIDENTIFIER03, 
	o_EndorsementNumber04 AS ENDORSEMENTNUMBER04, 
	o_BureauVersionIdentifierEditionIdentifier04 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER04, 
	o_CarrierVersionIdentifier04 AS CARRIERVERSIONIDENTIFIER04, 
	o_EndorsementNumber05 AS ENDORSEMENTNUMBER05, 
	o_BureauVersionIdentifierEditionIdentifier05 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER05, 
	o_CarrierVersionIdentifier05 AS CARRIERVERSIONIDENTIFIER05, 
	o_EndorsementNumber06 AS ENDORSEMENTNUMBER06, 
	o_BureauVersionIdentifierEditionIdentifier06 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER06, 
	o_CarrierVersionIdentifier06 AS CARRIERVERSIONIDENTIFIER06, 
	o_EndorsementNumber07 AS ENDORSEMENTNUMBER07, 
	o_BureauVersionIdentifierEditionIdentifier07 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER07, 
	o_CarrierVersionIdentifier07 AS CARRIERVERSIONIDENTIFIER07, 
	o_EndorsementNumber08 AS ENDORSEMENTNUMBER08, 
	o_BureauVersionIdentifierEditionIdentifier08 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER08, 
	o_CarrierVersionIdentifier08 AS CARRIERVERSIONIDENTIFIER08, 
	o_EndorsementNumber09 AS ENDORSEMENTNUMBER09, 
	o_BureauVersionIdentifierEditionIdentifier09 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER09, 
	o_CarrierVersionIdentifier09 AS CARRIERVERSIONIDENTIFIER09, 
	o_EndorsementNumber10 AS ENDORSEMENTNUMBER10, 
	o_BureauVersionIdentifierEditionIdentifier10 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER10, 
	o_CarrierVersionIdentifier10 AS CARRIERVERSIONIDENTIFIER10, 
	o_EndorsementNumber11 AS ENDORSEMENTNUMBER11, 
	o_BureauVersionIdentifierEditionIdentifier11 AS BUREAUVERSIONIDENTIFIEREDITIONIDENTIFIER11, 
	o_CarrierVersionIdentifier11 AS CARRIERVERSIONIDENTIFIER11, 
	o_PolicyChangeEffectiveDate AS POLICYCHANGEEFFECTIVEDATE, 
	o_PolicyChangeExpirationDate AS POLICYCHANGEEXPIRATIONDATE
	FROM EXP_Format_Output
),