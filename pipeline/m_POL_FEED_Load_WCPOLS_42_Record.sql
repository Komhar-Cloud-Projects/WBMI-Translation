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
	WITH ValueCnt_CTE
	AS
	(SELECT
			ST1.WCTrackHistoryID
			,COUNT(DISTINCT ST1.ModifierValue) ModValueCnt
	
		FROM dbo.WorkWCStateTerm ST1
	
		INNER JOIN dbo.WorkWCLine L1
			ON L1.WCTrackHistoryID = ST1.WCTrackHistoryID
	
		WHERE 1 = 1
		AND L1.InterstateRiskID > 0
		AND ST1.ExperienceModType = 'Contingent'
		AND ST1.[State] NOT IN ('DE', 'MI', 'NJ', 'NY', 'PA')
		AND ST1.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	
		GROUP BY ST1.WCTrackHistoryID
	)
	
	SELECT
		ST.WCTrackHistoryID
		,F.FormName
		,ST.ModifierValue
		,ST.[State]
		,P.TransactionEffectiveDate
		,PT.Name
		,ISNULL(L.InterstateRiskID, 0) AS InterstateRiskID
		,ST.ExperienceModEffectiveDate
		,ST.PeriodStartDate
		,ST.TermType
		,ISNULL(VAL.ModValueCnt, 0) AS ModValueCnt
	
	FROM dbo.WorkWCStateTerm ST
	
	INNER JOIN dbo.WorkWCForms F
		ON ST.WCTrackHistoryID = F.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCPolicy P
		ON P.WCTrackHistoryID = ST.WCTrackHistoryID
	
	INNER JOIN dbo.WorkWCParty PT
		ON PT.WCTrackHistoryID = ST.WCTrackHistoryID
			AND PT.PartyAssociationType = 'Account'
	
	INNER JOIN dbo.WorkWCLine L
		ON L.WCTrackHistoryID = ST.WCTrackHistoryID
	
	LEFT JOIN ValueCnt_CTE VAL
		ON VAL.WCTrackHistoryID = ST.WCTrackHistoryID
	
	WHERE 1 = 1
	AND ST.ExperienceModType = 'Contingent'
	AND ST.[State] NOT IN ('DE', 'MI', 'NJ', 'NY', 'PA')
	AND ST.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	AND FormName LIKE 'WC000412%'
	AND ST.TermType in ('ORG', 'EMF')
	@{pipeline().parameters.WHERE_CLAUSE_42}
	
	ORDER BY ST.WCTrackHistoryID
),
JNR_42_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WorkWCForms.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWCForms.FormName, 
	SQ_WorkWCForms.ModifierValue, 
	SQ_WorkWCForms.State, 
	SQ_WorkWCForms.TransactionEffectiveDate, 
	SQ_WorkWCForms.Name, 
	SQ_WorkWCForms.InterstateRiskID, 
	SQ_WorkWCForms.ExperienceModEffectiveDate, 
	SQ_WorkWCForms.PeriodStartDate, 
	SQ_WorkWCForms.TermType, 
	SQ_WorkWCForms.ModValueCnt
	FROM SQ_WorkWCForms
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = SQ_WorkWCForms.WCTrackHistoryID
),
RTRTRANS AS (
	SELECT
	WCTrackHistoryID,
	LinkData,
	AuditId,
	FormName,
	ModifierValue,
	State,
	TransactionEffectiveDate,
	Name,
	ExperienceModEffectiveDate,
	PeriodStartDate,
	TermType,
	ModValueCnt
	FROM JNR_42_Record
),
RTRTRANS_Build_99_State AS (SELECT * FROM RTRTRANS WHERE ModValueCnt = 1),
RTRTRANS_Passthrough_State AS (SELECT * FROM RTRTRANS WHERE ModValueCnt <> 1),
AGGTRANS AS (
	SELECT
	WCTrackHistoryID AS WCTrackHistoryID1,
	LinkData AS LinkData1,
	AuditId AS AuditId1,
	FormName AS FormName1,
	ModifierValue AS ModifierValue1,
	State AS State1,
	TransactionEffectiveDate AS TransactionEffectiveDate1,
	Name AS Name1,
	ExperienceModEffectiveDate AS ExperienceModEffectiveDate1,
	PeriodStartDate AS PeriodStartDate1,
	TermType AS TermType1,
	ModValueCnt AS ModValueCnt1
	FROM RTRTRANS_Build_99_State
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WCTrackHistoryID1 ORDER BY NULL) = 1
),
Union AS (
	SELECT WCTrackHistoryID1, LinkData1, AuditId1, FormName1, ModifierValue1, State1, TransactionEffectiveDate1, Name1, ExperienceModEffectiveDate1, ModValueCnt1, PeriodStartDate1, TermType1
	FROM AGGTRANS
	UNION
	SELECT WCTrackHistoryID AS WCTrackHistoryID1, LinkData AS LinkData1, AuditId AS AuditId1, FormName AS FormName1, ModifierValue AS ModifierValue1, State AS State1, TransactionEffectiveDate AS TransactionEffectiveDate1, Name AS Name1, ExperienceModEffectiveDate AS ExperienceModEffectiveDate1, ModValueCnt AS ModValueCnt1, PeriodStartDate AS PeriodStartDate1, TermType AS TermType1
	FROM RTRTRANS_Passthrough_State
),
EXP_42_Format_Output AS (
	SELECT
	CURRENT_TIMESTAMP AS o_ExtractDate,
	AuditId1 AS AuditId,
	WCTrackHistoryID1 AS WCTrackHistoryID,
	LinkData1 AS LinkData,
	FormName1 AS FormName,
	State1 AS State,
	-- *INF*: IIF(ModValueCnt=1,'99',:LKP.LKP_SupWCPOLS('DCT',State,'WCPOLS42Record','StateCodeRecord42'))
	IFF(
	    ModValueCnt = 1, '99', LKP_SUPWCPOLS__DCT_State_WCPOLS42Record_StateCodeRecord42.WCPOLSCode
	) AS o_StateCode,
	'42' AS o_RecordTypeCode,
	'WC000412' AS o_EndorsementNumber,
	-- *INF*: SUBSTR(FormName, Length(FormName)-4, 1)
	SUBSTR(FormName, Length(FormName) - 4, 1) AS v_BureauID,
	-- *INF*: IIF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID,' ')
	IFF(v_BureauID >= 'A' and v_BureauID <= 'Z', v_BureauID, ' ') AS o_BureauVersionIdentifierEditionIdentifier,
	-- *INF*: SUBSTR(FormName, Length(FormName)-3, 4)
	SUBSTR(FormName, Length(FormName) - 3, 4) AS o_CarrierVersionIdentifier,
	ExperienceModEffectiveDate1 AS ExperienceModEffectiveDate,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(TermType))='EMF',To_Char(PeriodStartDate,'YYMMDD'),
	-- LTRIM(RTRIM(TermType))='ORG',To_Char(PeriodStartDate,'YYMMDD'),
	-- LPAD('',8,' ')
	-- )
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(TermType)) = 'EMF', To_Char(PeriodStartDate, 'YYMMDD'),
	    LTRIM(RTRIM(TermType)) = 'ORG', To_Char(PeriodStartDate, 'YYMMDD'),
	    LPAD('', 8, ' ')
	) AS o_ContingentModificationEffectiveDate,
	ModifierValue1 AS ContingentExperienceModificationFactor,
	-- *INF*: TO_CHAR(TO_DECIMAL(ContingentExperienceModificationFactor,3)*1000)
	TO_CHAR(CAST(ContingentExperienceModificationFactor AS FLOAT) * 1000) AS v_ContingentExperienceModificationFactor,
	v_ContingentExperienceModificationFactor AS o_ContingentExperienceModificationFactor,
	TransactionEffectiveDate1 AS TransactionEffectiveDate,
	-- *INF*: To_Char(TransactionEffectiveDate, 'YYMMDD')
	To_Char(TransactionEffectiveDate, 'YYMMDD') AS o_EndorsementEffectiveDate,
	Name1 AS NameOfInsured,
	ModValueCnt1 AS ModValueCnt,
	PeriodStartDate1 AS PeriodStartDate,
	TermType1 AS TermType
	FROM Union
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_State_WCPOLS42Record_StateCodeRecord42
	ON LKP_SUPWCPOLS__DCT_State_WCPOLS42Record_StateCodeRecord42.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS42Record_StateCodeRecord42.SourceCode = State
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS42Record_StateCodeRecord42.TableName = 'WCPOLS42Record'
	AND LKP_SUPWCPOLS__DCT_State_WCPOLS42Record_StateCodeRecord42.ProcessName = 'StateCodeRecord42'

),
WCPols42Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols42Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols42Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, EndorsementNumber, BureauVersionIdentifierEditionIdentifier, CarrierVersionIdentifier, ContingentModificationEffectiveDate, ContingentExperienceModificationFactor, NameOfInsured, EndorsementEffectiveDate)
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
	o_ContingentModificationEffectiveDate AS CONTINGENTMODIFICATIONEFFECTIVEDATE, 
	o_ContingentExperienceModificationFactor AS CONTINGENTEXPERIENCEMODIFICATIONFACTOR, 
	NAMEOFINSURED, 
	o_EndorsementEffectiveDate AS ENDORSEMENTEFFECTIVEDATE
	FROM EXP_42_Format_Output
),