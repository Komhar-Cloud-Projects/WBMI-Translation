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
	     AuditId,
		TransactionCode
	FROM dbo.WCPols00Record
	WHERE 1=1
	AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	ORDER BY WCTrackHistoryID
),
SQ_WorkWC_08_Record AS (
	SELECT DISTINCT
		st.WCTrackHistoryID
		,th.HistoryID
		,th.TransactionType
		,th.ReasonCode
		,pol.PolicyEffectiveDate
		,pol.PolicyExpirationDate
		,pol.TransactionEffectiveDate
		,pol.TransactionExpirationDate
		,pty.Name
		,pty.DoingBusinessAs
		,L.Address1
		,L.Address2
		,L.City
		,L.StateProv
		,L.PostalCode
	    ,pol.Division
	    ,pol.TransactionDate
		,pol.PolicyKey
	     ,th.MIRequiredFlag
	  ,th.NCRequiredFlag
	,th.MNRequiredFlag
	,th.WIRequiredFlag
	FROM dbo.WorkWCStateTerm st
	
	INNER JOIN dbo.WorkWCTrackHistory th
		ON th.WCTrackHistoryID = st.WCTrackHistoryID
	     AND 
				th.TransactionType in ('Cancel','CancelPending','Reinstate','RescindCancelPending','Nonrenew')
	
	LEFT JOIN dbo.WorkWCParty pty
		ON st.WCTrackHistoryID = pty.WCTrackHistoryID
			AND pty.PartyAssociationType = 'Account'
	
	LEFT JOIN dbo.WorkWCLocation L
		ON st.WCTrackHistoryID = L.WCTrackHistoryID
			AND L.LocationType = 'Account'
	
	INNER JOIN dbo.WorkWCPolicy pol
		ON st.WCTrackHistoryID = pol.WCTrackHistoryID
	
	WHERE 1 = 1
	AND st.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_08}
	
	ORDER BY st.WCTrackHistoryID
),
JNR_08_Record AS (SELECT
	SQ_WCPols00Record.WCTrackHistoryID, 
	SQ_WCPols00Record.LinkData, 
	SQ_WCPols00Record.AuditId, 
	SQ_WCPols00Record.TransactionCode, 
	SQ_WorkWC_08_Record.WCTrackHistoryID AS WCTrackHistoryID1, 
	SQ_WorkWC_08_Record.HistoryID, 
	SQ_WorkWC_08_Record.TransactionType, 
	SQ_WorkWC_08_Record.ReasonCode, 
	SQ_WorkWC_08_Record.PolicyEffectiveDate, 
	SQ_WorkWC_08_Record.PolicyExpirationDate, 
	SQ_WorkWC_08_Record.TransactionEffectiveDate, 
	SQ_WorkWC_08_Record.TransactionExpirationDate, 
	SQ_WorkWC_08_Record.Name, 
	SQ_WorkWC_08_Record.DoingBusinessAs, 
	SQ_WorkWC_08_Record.Address1, 
	SQ_WorkWC_08_Record.Address2, 
	SQ_WorkWC_08_Record.City, 
	SQ_WorkWC_08_Record.StateProv, 
	SQ_WorkWC_08_Record.PostalCode, 
	SQ_WorkWC_08_Record.Division, 
	SQ_WorkWC_08_Record.TransactionDate, 
	SQ_WorkWC_08_Record.PolicyKey, 
	SQ_WorkWC_08_Record.MIRequiredFlag, 
	SQ_WorkWC_08_Record.NCRequiredFlag, 
	SQ_WorkWC_08_Record.MNRequiredFlag, 
	SQ_WorkWC_08_Record.WIRequiredFlag
	FROM SQ_WorkWC_08_Record
	INNER JOIN SQ_WCPols00Record
	ON SQ_WCPols00Record.WCTrackHistoryID = SQ_WorkWC_08_Record.WCTrackHistoryID
),
EXP_08_Format AS (
	SELECT
	CURRENT_TIMESTAMP AS ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	PolicyKey,
	HistoryID,
	TransactionType,
	ReasonCode,
	PolicyEffectiveDate,
	PolicyExpirationDate,
	TransactionEffectiveDate,
	TransactionExpirationDate,
	Name,
	DoingBusinessAs,
	Address1,
	Address2,
	City,
	StateProv,
	PostalCode,
	Division,
	TransactionDate,
	'08' AS o_RecordTypeCode,
	-- *INF*: IIF (Division = 'WCPool' and TransactionType='NonRenew' and ReasonCode = 'NonPaymentofRenewal','Cancel',TransactionType)
	IFF(
	    Division = 'WCPool' and TransactionType = 'NonRenew' and ReasonCode = 'NonPaymentofRenewal',
	    'Cancel',
	    TransactionType
	) AS v_Lookup_TransType,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',TransactionType,'WCPOLS08Record','CancellationReinstatementIDCode')
	LKP_SUPWCPOLS__DCT_TransactionType_WCPOLS08Record_CancellationReinstatementIDCode.WCPOLSCode AS v_CancellationReinstatementIDCode,
	-- *INF*:  :LKP.LKP_SupWCPOLS('DCT',v_Lookup_TransType,'WCPOLS08Record','CancellationReinstatementIDCode')
	LKP_SUPWCPOLS__DCT_v_Lookup_TransType_WCPOLS08Record_CancellationReinstatementIDCode.WCPOLSCode AS v_CancellationReinstatementIDCode_WI,
	v_CancellationReinstatementIDCode AS o_CancellationReinstatementIDCode,
	v_CancellationReinstatementIDCode_WI AS o_CancellationReinstatementIDCode_WI,
	-- *INF*: IIF(v_CancellationReinstatementIDCode <> '1','0',
	-- IIF(PolicyEffectiveDate = TransactionEffectiveDate, :LKP.LKP_SupWCPOLS('DCT','FlatCancel','WCPOLS08Record','CancellationTypeCode'),
	-- :LKP.LKP_SupWCPOLS('DCT','MidTermCancel','WCPOLS08Record','CancellationTypeCode')))
	IFF(
	    v_CancellationReinstatementIDCode <> '1', '0',
	    IFF(
	        PolicyEffectiveDate = TransactionEffectiveDate,
	        LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode.WCPOLSCode,
	        LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode.WCPOLSCode
	    )
	) AS o_CancellationTypeCode,
	-- *INF*: IIF(v_CancellationReinstatementIDCode_WI<> '1','0',
	-- IIF(PolicyEffectiveDate = TransactionEffectiveDate, :LKP.LKP_SupWCPOLS('DCT','FlatCancel','WCPOLS08Record','CancellationTypeCode'),
	-- :LKP.LKP_SupWCPOLS('DCT','MidTermCancel','WCPOLS08Record','CancellationTypeCode')))
	IFF(
	    v_CancellationReinstatementIDCode_WI <> '1', '0',
	    IFF(
	        PolicyEffectiveDate = TransactionEffectiveDate,
	        LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode.WCPOLSCode,
	        LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode.WCPOLSCode
	    )
	) AS o_CancellationTypeCode_WI,
	-- *INF*: IIF(v_CancellationReinstatementIDCode_WI <> '1',NULL, :LKP.LKP_SupWCPOLS('DCT',ReasonCode,'WCPOLS08Record','ReasonForCancellationCode'))
	IFF(
	    v_CancellationReinstatementIDCode_WI <> '1', NULL,
	    LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode.WCPOLSCode
	) AS o_ReasonForCancellationCode_WI,
	-- *INF*: IIF(v_CancellationReinstatementIDCode <> '2','0',
	-- IIF(PolicyEffectiveDate = TransactionEffectiveDate, :LKP.LKP_SupWCPOLS('DCT','ReinstateFlatCancel','WCPOLS08Record','ReinstatementTypeCode'),
	-- :LKP.LKP_SupWCPOLS('DCT','ReinstateMidTermCancel','WCPOLS08Record','ReinstatementTypeCode')))
	IFF(
	    v_CancellationReinstatementIDCode <> '2', '0',
	    IFF(
	        PolicyEffectiveDate = TransactionEffectiveDate,
	        LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode.WCPOLSCode,
	        LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode.WCPOLSCode
	    )
	) AS o_ReinstatementTypeCode,
	-- *INF*: IIF(v_CancellationReinstatementIDCode_WI <> '2','0', IIF(PolicyEffectiveDate = TransactionEffectiveDate, :LKP.LKP_SupWCPOLS('DCT','ReinstateFlatCancel','WCPOLS08Record','ReinstatementTypeCode'), :LKP.LKP_SupWCPOLS('DCT','ReinstateMidTermCancel','WCPOLS08Record','ReinstatementTypeCode')))
	IFF(
	    v_CancellationReinstatementIDCode_WI <> '2', '0',
	    IFF(
	        PolicyEffectiveDate = TransactionEffectiveDate,
	        LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode.WCPOLSCode,
	        LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode.WCPOLSCode
	    )
	) AS o_ReinstatementTypeCode_WI,
	-- *INF*: IIF (IsNull(DoingBusinessAs) or RTrim(DoingBusinessAs) = '', RTrim(Name),  RTrim(Name) || ' DBA ' || RTrim(DoingBusinessAs))
	IFF(
	    DoingBusinessAs IS NULL or RTrim(DoingBusinessAs) = '', RTrim(Name),
	    RTrim(Name) || ' DBA ' || RTrim(DoingBusinessAs)
	) AS o_NameOfInsured,
	-- *INF*: Address1 || RTRIM(' ' || Address2) || ' ' || City || ' ' || StateProv || ' ' || PostalCode
	Address1 || RTRIM(' ' || Address2) || ' ' || City || ' ' || StateProv || ' ' || PostalCode AS o_AddressOfInsured,
	-- *INF*: IIF(in(v_CancellationReinstatementIDCode, '1','3'),To_Char(TransactionDate,'YYMMDD'),NULL)
	IFF(
	    v_CancellationReinstatementIDCode IN ('1','3'), To_Char(TransactionDate, 'YYMMDD'), NULL
	) AS o_CancellationMailedtoInsuredDate,
	-- *INF*: IIF(IN(v_CancellationReinstatementIDCode_WI, '1','3'),To_Char(TransactionDate,'YYMMDD'),NULL)
	IFF(
	    v_CancellationReinstatementIDCode_WI IN ('1','3'), To_Char(TransactionDate, 'YYMMDD'), NULL
	) AS o_CancellationMailedtoInsuredDate_WI,
	-- *INF*: IIF(v_CancellationReinstatementIDCode = '2','01','00')
	-- 
	IFF(v_CancellationReinstatementIDCode = '2', '01', '00') AS o_ReasonForReinstatementCode,
	-- *INF*: IIF(v_CancellationReinstatementIDCode_WI = '2','01','00')
	IFF(v_CancellationReinstatementIDCode_WI = '2', '01', '00') AS o_ReasonForReinstatementCode_WI,
	-- *INF*: IIF(v_CancellationReinstatementIDCode = '2',To_Char(TransactionEffectiveDate,'YYMMDD'),NULL)
	IFF(
	    v_CancellationReinstatementIDCode = '2', To_Char(TransactionEffectiveDate, 'YYMMDD'), NULL
	) AS o_CorrespondingCancellationEffectiveDate,
	-- *INF*: IIF(v_CancellationReinstatementIDCode_WI = '2',To_Char(TransactionEffectiveDate,'YYMMDD'),NULL)
	IFF(
	    v_CancellationReinstatementIDCode_WI = '2', To_Char(TransactionEffectiveDate, 'YYMMDD'),
	    NULL
	) AS o_CorrespondingCancellationEffectiveDate_WI,
	-- *INF*: To_Char(TransactionEffectiveDate,'YYMMDD')
	To_Char(TransactionEffectiveDate, 'YYMMDD') AS o_CancellationReinstatementEffectiveDate,
	-- *INF*: To_Char(TransactionEffectiveDate,'YYMMDD')
	To_Char(TransactionEffectiveDate, 'YYMMDD') AS o_CancellationReinstatementEffectiveDate_WI,
	MIRequiredFlag,
	MNRequiredFlag,
	NCRequiredFlag,
	WIRequiredFlag,
	-- *INF*: --IIF(v_CancellationReinstatementIDCode = '2',NULL,:LKP.LKP_SupWCPOLS('DCT',ReasonCode,'WCPOLS08Record','ReasonForCancellationCode'))
	-- 
	-- decode( true,
	-- v_CancellationReinstatementIDCode = '2',NULL,
	-- (v_CancellationReinstatementIDCode = '3' and MNRequiredFlag = 'T') , '00',
	-- (v_CancellationReinstatementIDCode = '3' and NCRequiredFlag = 'T') , '00',
	-- (v_CancellationReinstatementIDCode = '3' and WIRequiredFlag = 'T') , '00',
	-- :LKP.LKP_SupWCPOLS('DCT',ReasonCode,'WCPOLS08Record','ReasonForCancellationCode'))
	decode(
	    true,
	    v_CancellationReinstatementIDCode = '2', NULL,
	    (v_CancellationReinstatementIDCode = '3' and MNRequiredFlag = 'T'), '00',
	    (v_CancellationReinstatementIDCode = '3' and NCRequiredFlag = 'T'), '00',
	    (v_CancellationReinstatementIDCode = '3' and WIRequiredFlag = 'T'), '00',
	    LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode.WCPOLSCode
	) AS v_ReasonForCancellationCode,
	-- *INF*: IIF( (MIRequiredFlag = 'T'  and v_ReasonForCancellationCode ='99' ) , '19' ,v_ReasonForCancellationCode) 
	-- 
	-- --previous logic
	-- --IIF(v_CancellationReinstatementIDCode <> '1',NULL,:LKP.LKP_SupWCPOLS('DCT',ReasonCode,'WCPOLS08Record','ReasonForCancellationCode'))
	IFF(
	    (MIRequiredFlag = 'T' and v_ReasonForCancellationCode = '99'), '19',
	    v_ReasonForCancellationCode
	) AS o_ReasonForCancellationCode
	FROM JNR_08_Record
	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_TransactionType_WCPOLS08Record_CancellationReinstatementIDCode
	ON LKP_SUPWCPOLS__DCT_TransactionType_WCPOLS08Record_CancellationReinstatementIDCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_TransactionType_WCPOLS08Record_CancellationReinstatementIDCode.SourceCode = TransactionType
	AND LKP_SUPWCPOLS__DCT_TransactionType_WCPOLS08Record_CancellationReinstatementIDCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_TransactionType_WCPOLS08Record_CancellationReinstatementIDCode.ProcessName = 'CancellationReinstatementIDCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_v_Lookup_TransType_WCPOLS08Record_CancellationReinstatementIDCode
	ON LKP_SUPWCPOLS__DCT_v_Lookup_TransType_WCPOLS08Record_CancellationReinstatementIDCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_v_Lookup_TransType_WCPOLS08Record_CancellationReinstatementIDCode.SourceCode = v_Lookup_TransType
	AND LKP_SUPWCPOLS__DCT_v_Lookup_TransType_WCPOLS08Record_CancellationReinstatementIDCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_v_Lookup_TransType_WCPOLS08Record_CancellationReinstatementIDCode.ProcessName = 'CancellationReinstatementIDCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode
	ON LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode.SourceCode = 'FlatCancel'
	AND LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_FlatCancel_WCPOLS08Record_CancellationTypeCode.ProcessName = 'CancellationTypeCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode
	ON LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode.SourceCode = 'MidTermCancel'
	AND LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_MidTermCancel_WCPOLS08Record_CancellationTypeCode.ProcessName = 'CancellationTypeCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode
	ON LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode.SourceCode = ReasonCode
	AND LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_ReasonCode_WCPOLS08Record_ReasonForCancellationCode.ProcessName = 'ReasonForCancellationCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode
	ON LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode.SourceCode = 'ReinstateFlatCancel'
	AND LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_ReinstateFlatCancel_WCPOLS08Record_ReinstatementTypeCode.ProcessName = 'ReinstatementTypeCode'

	LEFT JOIN LKP_SUPWCPOLS LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode
	ON LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode.SourcesystemID = 'DCT'
	AND LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode.SourceCode = 'ReinstateMidTermCancel'
	AND LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode.TableName = 'WCPOLS08Record'
	AND LKP_SUPWCPOLS__DCT_ReinstateMidTermCancel_WCPOLS08Record_ReinstatementTypeCode.ProcessName = 'ReinstatementTypeCode'

),
SRT_TransSeq_Number AS (
	SELECT
	ExtractDate, 
	AuditId, 
	WCTrackHistoryID, 
	LinkData, 
	PolicyKey, 
	HistoryID, 
	o_RecordTypeCode AS RecordTypeCode, 
	o_CancellationReinstatementIDCode, 
	o_CancellationReinstatementIDCode_WI, 
	o_CancellationTypeCode, 
	o_CancellationTypeCode_WI, 
	o_ReasonForCancellationCode, 
	o_ReasonForCancellationCode_WI, 
	o_ReinstatementTypeCode, 
	o_ReinstatementTypeCode_WI, 
	o_NameOfInsured, 
	o_AddressOfInsured, 
	o_CancellationMailedtoInsuredDate, 
	o_CancellationMailedtoInsuredDate_WI, 
	o_ReasonForReinstatementCode, 
	o_ReasonForReinstatementCode_WI, 
	o_CorrespondingCancellationEffectiveDate, 
	o_CorrespondingCancellationEffectiveDate_WI, 
	o_CancellationReinstatementEffectiveDate, 
	o_CancellationReinstatementEffectiveDate_WI, 
	ReasonCode
	FROM EXP_08_Format
	ORDER BY PolicyKey ASC, HistoryID ASC, o_CancellationReinstatementIDCode ASC, o_CancellationReinstatementIDCode_WI ASC
),
EXP_Create_TransSeq_Number AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	PolicyKey,
	HistoryID,
	'99' AS StateCode,
	RecordTypeCode,
	o_CancellationReinstatementIDCode AS CancellationReinstatementIDCode,
	o_CancellationTypeCode AS CancellationTypeCode,
	o_ReasonForCancellationCode AS ReasonForCancellationCode,
	o_ReinstatementTypeCode AS ReinstatementTypeCode,
	o_NameOfInsured AS NameOfInsured,
	o_AddressOfInsured AS AddressOfInsured,
	o_CancellationMailedtoInsuredDate AS CancellationMailedtoInsuredDate,
	o_ReasonForReinstatementCode AS ReasonForReinstatementCode,
	o_CorrespondingCancellationEffectiveDate AS CorrespondingCancellationEffectiveDate,
	o_CancellationReinstatementEffectiveDate AS CancellationReinstatementEffectiveDate,
	-- *INF*: PolicyKey
	-- --PolicyKey|| '~' || CancellationReinstatementIDCode
	PolicyKey AS v_Grouping_Key,
	-- *INF*: IIF (IsNull(old_Grouping_Key) or old_Grouping_Key <> v_Grouping_Key, 1, v_Grouping_Cnt + 1)
	IFF(old_Grouping_Key IS NULL or old_Grouping_Key <> v_Grouping_Key, 1, v_Grouping_Cnt + 1) AS v_Grouping_Cnt,
	v_Grouping_Cnt AS o_CancellationReinstatementTransactionSequenceNumber,
	v_Grouping_Key AS old_Grouping_Key
	FROM SRT_TransSeq_Number
),
WCPols08Record AS (

	------------ PRE SQL ----------
	DELETE
	  FROM dbo.WCPols08Record
	  WHERE 1=1
	  AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	-------------------------------


	INSERT INTO WCPols08Record
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, CancellationReinstatementIDCode, CancellationTypeCode, ReasonForCancellationCode, ReinstatementTypeCode, NameOfInsured, AddressOfInsured, CancellationMailedtoInsuredDate, CancellationReinstatementTransactionSequenceNumber, ReasonForReinstatementCode, CorrespondingCancellationEffectiveDate, CancellationReinstatementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	CANCELLATIONREINSTATEMENTIDCODE, 
	CANCELLATIONTYPECODE, 
	REASONFORCANCELLATIONCODE, 
	REINSTATEMENTTYPECODE, 
	NAMEOFINSURED, 
	ADDRESSOFINSURED, 
	CANCELLATIONMAILEDTOINSUREDDATE, 
	o_CancellationReinstatementTransactionSequenceNumber AS CANCELLATIONREINSTATEMENTTRANSACTIONSEQUENCENUMBER, 
	REASONFORREINSTATEMENTCODE, 
	CORRESPONDINGCANCELLATIONEFFECTIVEDATE, 
	CANCELLATIONREINSTATEMENTEFFECTIVEDATE
	FROM EXP_Create_TransSeq_Number
),
EXP_Create_TransSeq_Number_WI AS (
	SELECT
	ExtractDate,
	AuditId,
	WCTrackHistoryID,
	LinkData,
	PolicyKey,
	HistoryID,
	'99' AS StateCode,
	RecordTypeCode,
	o_CancellationReinstatementIDCode_WI AS CancellationReinstatementIDCode,
	o_CancellationTypeCode_WI AS CancellationTypeCode,
	o_ReasonForCancellationCode_WI AS ReasonForCancellationCode,
	ReasonCode,
	-- *INF*: IIF(ReasonForCancellationCode='20' and LTRIM(RTRIM(ReasonCode))='NPFC','05',ReasonForCancellationCode)
	IFF(
	    ReasonForCancellationCode = '20' and LTRIM(RTRIM(ReasonCode)) = 'NPFC', '05',
	    ReasonForCancellationCode
	) AS v_ReasonForCancellationCode,
	-- *INF*: IIF(ReasonForCancellationCode='04' and LTRIM(RTRIM(ReasonCode))='RewriteReissue','07',v_ReasonForCancellationCode)
	IFF(
	    ReasonForCancellationCode = '04' and LTRIM(RTRIM(ReasonCode)) = 'RewriteReissue', '07',
	    v_ReasonForCancellationCode
	) AS o_ReasonForCancellationCode,
	o_ReinstatementTypeCode_WI AS ReinstatementTypeCode,
	o_NameOfInsured AS NameOfInsured,
	o_AddressOfInsured AS AddressOfInsured,
	o_CancellationMailedtoInsuredDate_WI AS CancellationMailedtoInsuredDate,
	o_ReasonForReinstatementCode_WI AS ReasonForReinstatementCode,
	o_CorrespondingCancellationEffectiveDate_WI AS CorrespondingCancellationEffectiveDate,
	o_CancellationReinstatementEffectiveDate_WI AS CancellationReinstatementEffectiveDate,
	-- *INF*: PolicyKey
	-- 
	-- --PolicyKey|| '~' || CancellationReinstatementIDCode
	PolicyKey AS v_Grouping_Key,
	-- *INF*: IIF (IsNull(old_Grouping_Key) or old_Grouping_Key <> v_Grouping_Key, 1, v_Grouping_Cnt + 1)
	IFF(old_Grouping_Key IS NULL or old_Grouping_Key <> v_Grouping_Key, 1, v_Grouping_Cnt + 1) AS v_Grouping_Cnt,
	v_Grouping_Cnt AS o_CancellationReinstatementTransactionSequenceNumber,
	v_Grouping_Key AS old_Grouping_Key
	FROM SRT_TransSeq_Number
),
WCPols08RecordWI AS (
	INSERT INTO WCPols08RecordWI
	(ExtractDate, AuditId, WCTrackHistoryID, LinkData, StateCode, RecordTypeCode, CancellationReinstatementIDCode, CancellationTypeCode, ReasonForCancellationCode, ReinstatementTypeCode, NameOfInsured, AddressOfInsured, CancellationMailedtoInsuredDate, CancellationReinstatementTransactionSequenceNumber, ReasonForReinstatementCode, CorrespondingCancellationEffectiveDate, CancellationReinstatementEffectiveDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WCTRACKHISTORYID, 
	LINKDATA, 
	STATECODE, 
	RECORDTYPECODE, 
	CANCELLATIONREINSTATEMENTIDCODE, 
	CANCELLATIONTYPECODE, 
	o_ReasonForCancellationCode AS REASONFORCANCELLATIONCODE, 
	REINSTATEMENTTYPECODE, 
	NAMEOFINSURED, 
	ADDRESSOFINSURED, 
	CANCELLATIONMAILEDTOINSUREDDATE, 
	o_CancellationReinstatementTransactionSequenceNumber AS CANCELLATIONREINSTATEMENTTRANSACTIONSEQUENCENUMBER, 
	REASONFORREINSTATEMENTCODE, 
	CORRESPONDINGCANCELLATIONEFFECTIVEDATE, 
	CANCELLATIONREINSTATEMENTEFFECTIVEDATE
	FROM EXP_Create_TransSeq_Number_WI
),