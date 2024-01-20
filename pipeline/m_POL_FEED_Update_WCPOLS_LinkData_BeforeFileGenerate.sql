WITH
SQ_WorkWCTrackHistoryState AS (
	Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,A.OriginalTransactionType,A.Auditid
	,'WorkWCTrackHistory' TableName,'TransactionType' ProcessName from WorkWCTrackHistory A
	inner join WorkWCTrackHistoryState B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.StateDeletedFlag=A.StateDeletedFlag
	inner join WCPols00Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
	and A.StateDeletedFlag='1'
	and B.State in (@{pipeline().parameters.FILENAME})
	and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	
	UNION
	
	Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,A.OriginalTransactionType,A.Auditid
	,'WCPols00Record' TableName,'LinkData' ProcessName from WorkWCTrackHistory A
	inner join WorkWCTrackHistoryState B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.StateDeletedFlag=A.StateDeletedFlag
	inner join WCPols00Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
	and A.StateDeletedFlag='1'
	and B.State in (@{pipeline().parameters.FILENAME})
	and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	
	UNION
	
	Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,A.OriginalTransactionType,A.Auditid
	,'WCPols00Record' TableName,'TransactionCode' ProcessName from WorkWCTrackHistory A
	inner join WorkWCTrackHistoryState B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.StateDeletedFlag=A.StateDeletedFlag
	inner join WCPols00Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
	and A.StateDeletedFlag='1'
	and B.State in (@{pipeline().parameters.FILENAME})
	and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	
	UNION
	
	Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,
	'48' OriginalTransactionType ,A.Auditid,'WCPols08RecordWI' TableName,'StateCode' ProcessName 
	from WorkWCTrackHistory A
	inner join WorkWCTrackHistoryState B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.StateDeletedFlag=A.StateDeletedFlag
	inner join WCPols00Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
	and A.StateDeletedFlag='1'
	and B.State in (@{pipeline().parameters.FILENAME})
	and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.LOGIC_REQUIRED}
	
	UNION
	
	Select *
	from (
		Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,
		case
			when B.State = 'NC'
			and 'NC' in (@{pipeline().parameters.FILENAME})
			then '32'
			when B.State = 'MN'
			and 'MN' in (@{pipeline().parameters.FILENAME})
			then '22'
			else '99'
		end OriginalTransactionType ,A.Auditid,'WCPols08Record' TableName,'StateCode' ProcessName 
		from WorkWCTrackHistory A
		inner join WorkWCTrackHistoryState B
		on A.WCTrackHistoryID=B.WCTrackHistoryID
		and B.StateDeletedFlag=A.StateDeletedFlag
		inner join WCPols00Record C
		on A.WCTrackHistoryID=C.WCTrackHistoryID
		where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
		and A.StateDeletedFlag='1'
		and B.State in (@{pipeline().parameters.FILENAME})
		and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
		and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		@{pipeline().parameters.LOGIC_REQUIRED}
	) A
	where A.OriginalTransactionType <> '99'
	
	UNION
	
	Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,
	'03' OriginalTransactionType ,A.Auditid,'WCPols08RecordWI' TableName,'ReasonForCancellationCode' ProcessName 
	from WorkWCTrackHistory A
	inner join WorkWCTrackHistoryState B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.StateDeletedFlag=A.StateDeletedFlag
	inner join WCPols00Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
	and A.StateDeletedFlag='1'
	and B.State in (@{pipeline().parameters.FILENAME})
	and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.LOGIC_REQUIRED}
	
	UNION
	
	Select A.WCTrackHistoryID,B.State,A.StateDeletedFlag,C.OriginalLinkData,
	'03' OriginalTransactionType ,A.Auditid,'WCPols08Record' TableName,'ReasonForCancellationCode' ProcessName 
	from WorkWCTrackHistory A
	inner join WorkWCTrackHistoryState B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.StateDeletedFlag=A.StateDeletedFlag
	inner join WCPols00Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	where (A.@{pipeline().parameters.FILENAMEREQUIREDFLAG1}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG2}=1 or A.@{pipeline().parameters.FILENAMEREQUIREDFLAG3}=1)
	and A.StateDeletedFlag='1'
	and B.State in (@{pipeline().parameters.FILENAME})
	and A.OriginalTransactionType IN (@{pipeline().parameters.TRANSACTION_TYPE})
	and A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.LOGIC_REQUIRED}
),
EXP_Source AS (
	SELECT
	WCTrackHistoryID,
	-- *INF*: TO_CHAR(WCTrackHistoryID)
	TO_CHAR(WCTrackHistoryID) AS o_WCTrackHistoryID,
	State,
	StateDeletedFlag,
	OriginalLinkData,
	OriginalTransactionType,
	Auditid,
	TableName,
	ProcessName,
	-- *INF*: SUBSTR(OriginalLinkData,1,41)
	SUBSTR(OriginalLinkData, 1, 41) AS v_OriginalLinkData,
	-- *INF*: DECODE(TRUE,
	-- LTRIM(RTRIM(OriginalTransactionType))='EndorseAddDeleteState' AND StateDeletedFlag='T','05',
	-- '15')
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(OriginalTransactionType)) = 'EndorseAddDeleteState' AND StateDeletedFlag = 'T', '05',
	    '15'
	) AS v_TransactionCode,
	-- *INF*: DECODE(Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID},
	-- LTRIM(RTRIM(TableName))='WorkWCTrackHistory'
	-- AND @{pipeline().parameters.SET_REVERT}='SET','Cancel',
	-- LTRIM(RTRIM(TableName))='WorkWCTrackHistory'
	-- AND @{pipeline().parameters.SET_REVERT}='REVERT',OriginalTransactionType,
	-- LTRIM(RTRIM(TableName))='WCPols00Record' AND LTRIM(RTRIM(ProcessName))='LinkData' 
	-- AND @{pipeline().parameters.SET_REVERT}='SET',LTRIM(RTRIM(v_OriginalLinkData))||LTRIM(RTRIM(v_TransactionCode)),
	-- LTRIM(RTRIM(TableName))='WCPols00Record' AND LTRIM(RTRIM(ProcessName))='LinkData' 
	-- AND @{pipeline().parameters.SET_REVERT}='REVERT',OriginalLinkData,
	-- LTRIM(RTRIM(TableName))='WCPols00Record' AND LTRIM(RTRIM(ProcessName))='TransactionCode' 
	-- AND @{pipeline().parameters.SET_REVERT}='SET','05',
	-- LTRIM(RTRIM(TableName))='WCPols00Record' AND LTRIM(RTRIM(ProcessName))='TransactionCode' 
	-- AND @{pipeline().parameters.SET_REVERT}='REVERT','15',
	-- LTRIM(RTRIM(TableName))='WCPols08RecordWI',
	-- LTRIM(RTRIM(OriginalTransactionType)),
	-- LTRIM(RTRIM(TableName))='WCPols08Record',
	-- LTRIM(RTRIM(OriginalTransactionType))
	-- )
	DECODE(
	    Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID},
	    LTRIM(RTRIM(TableName)) = 'WorkWCTrackHistory' AND @{pipeline().parameters.SET_REVERT} = 'SET', 'Cancel',
	    LTRIM(RTRIM(TableName)) = 'WorkWCTrackHistory' AND @{pipeline().parameters.SET_REVERT} = 'REVERT', OriginalTransactionType,
	    LTRIM(RTRIM(TableName)) = 'WCPols00Record' AND LTRIM(RTRIM(ProcessName)) = 'LinkData' AND @{pipeline().parameters.SET_REVERT} = 'SET', LTRIM(RTRIM(v_OriginalLinkData)) || LTRIM(RTRIM(v_TransactionCode)),
	    LTRIM(RTRIM(TableName)) = 'WCPols00Record' AND LTRIM(RTRIM(ProcessName)) = 'LinkData' AND @{pipeline().parameters.SET_REVERT} = 'REVERT', OriginalLinkData,
	    LTRIM(RTRIM(TableName)) = 'WCPols00Record' AND LTRIM(RTRIM(ProcessName)) = 'TransactionCode' AND @{pipeline().parameters.SET_REVERT} = 'SET', '05',
	    LTRIM(RTRIM(TableName)) = 'WCPols00Record' AND LTRIM(RTRIM(ProcessName)) = 'TransactionCode' AND @{pipeline().parameters.SET_REVERT} = 'REVERT', '15',
	    LTRIM(RTRIM(TableName)) = 'WCPols08RecordWI', LTRIM(RTRIM(OriginalTransactionType)),
	    LTRIM(RTRIM(TableName)) = 'WCPols08Record', LTRIM(RTRIM(OriginalTransactionType))
	) AS v_TableUpdate,
	v_TableUpdate AS UpdateValue
	FROM SQ_WorkWCTrackHistoryState
),
SQL AS (-- SQL

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_SQLOutput AS (
	SELECT
	SQLError,
	NumRowsAffected,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(SQLError),'ErrorFile',
	-- (ISNULL(SQLError) OR length(rtrim(ltrim(SQLError)))=0) AND NOT ISNULL(UpdateValue),'UpdateList',
	-- '')
	DECODE(
	    TRUE,
	    SQLError IS NOT NULL, 'ErrorFile',
	    (SQLError IS NULL OR length(rtrim(ltrim(SQLError))) = 0) AND UpdateValue IS NOT NULL, 'UpdateList',
	    ''
	) AS v_FileName,
	@{pipeline().parameters.FILENAME}||@{pipeline().parameters.SET_REVERT}||v_FileName||'.txt' AS FileName,
	o_WCTrackHistoryID AS WCTrackHistoryID,
	o_State AS State,
	o_StateDeletedFlag AS StateDeletedFlag,
	o_OriginalLinkData AS OriginalLinkData,
	o_OriginalTransactionType AS OriginalTransactionType,
	o_Auditid AS Auditid,
	o_TableName AS TableName,
	o_ProcessName AS ProcessName,
	o_UpdateValue AS UpdateValue,
	-- *INF*: 'Update dbo.'||TableName||' A Set A.'||ProcessName||'='||UpdateValue||' where A.WCTrackHistoryID='||WCTrackHistoryID|| '----- '||@{pipeline().parameters.FILENAME}||' - '||@{pipeline().parameters.SET_REVERT}||' - '||@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}||' NumRowsAffected - '||NumRowsAffected
	'Update dbo.' || TableName || ' A Set A.' || ProcessName || '=' || UpdateValue || ' where A.WCTrackHistoryID=' || WCTrackHistoryID || '----- ' || @{pipeline().parameters.FILENAME} || ' - ' || @{pipeline().parameters.SET_REVERT} || ' - ' || @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} || ' NumRowsAffected - ' || NumRowsAffected AS SQLUpdate
	FROM SQL
),
RTR_SQLError AS (
	SELECT
	SQLError,
	NumRowsAffected,
	FileName,
	WCTrackHistoryID,
	State,
	StateDeletedFlag,
	OriginalLinkData,
	OriginalTransactionType,
	Auditid,
	TableName,
	ProcessName,
	UpdateValue,
	SQLUpdate
	FROM EXP_SQLOutput
),
RTR_SQLError_Error_Group AS (SELECT * FROM RTR_SQLError WHERE NOT ISNULL(SQLError)),
RTR_SQLError_Update_Scripts AS (SELECT * FROM RTR_SQLError WHERE (ISNULL(SQLError) OR length(rtrim(ltrim(SQLError)))=0) AND NOT ISNULL(UpdateValue)),
WCPOLS_UpdateStatements_FlatFile AS (
	INSERT INTO WCPOLS_UpdateStatements_FlatFile
	(FlatFileComments)
	SELECT 
	SQLUpdate AS FLATFILECOMMENTS
	FROM RTR_SQLError_Update_Scripts
),
WCPOLS_UpdateStatements_FlatFile_SQLError AS (
	INSERT INTO WCPOLS_UpdateStatements_FlatFile
	(FlatFileComments)
	SELECT 
	SQLError AS FLATFILECOMMENTS
	FROM RTR_SQLError_Error_Group
),
SQ_WorkWCTrackHistoryState_DeletedAttributes AS (
	Select B.WCPols02RecordID,NULL UpdatedValue,
	A.Auditid,'WCPols02Record' TableName,'PolicyChangeEffectiveDate' ProcessName ,'WCPols02RecordID' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WCPols02Record B
	on A.WCTrackHistoryID=B.WCTrackHistoryID and A.TransactionType like 'Endorse%'
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and A.TransactionType like 'Endorse%'
	and B.PolicyChangeEffectiveDate=B.PolicyChangeExpirationDate
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.DELETED_ATRIBUTES}
	
	UNION
	
	Select B.WCPols03RecordID,NULL UpdatedValue,
	A.Auditid,'WCPols03Record' TableName,'PolicyChangeEffectiveDate' ProcessName ,'WCPols03RecordID' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WCPols03Record B
	on A.WCTrackHistoryID=B.WCTrackHistoryID and A.TransactionType like 'Endorse%'
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and A.TransactionType like 'Endorse%'
	and B.PolicyChangeEffectiveDate=B.PolicyChangeExpirationDate
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.DELETED_ATRIBUTES}
	
	UNION
	
	Select B.WCPols04RecordID,NULL UpdatedValue,
	A.Auditid,'WCPols04Record' TableName,'PolicyChangeEffectiveDate' ProcessName ,'WCPols04RecordID' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WCPols04Record B
	on A.WCTrackHistoryID=B.WCTrackHistoryID and A.TransactionType like 'Endorse%'
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and A.TransactionType like 'Endorse%'
	and B.PolicyChangeEffectiveDate=B.PolicyChangeExpirationDate
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.DELETED_ATRIBUTES}
	
	UNION
	
	Select B.WCPols05RecordID,NULL UpdatedValue,
	A.Auditid,'WCPols05Record' TableName,'PolicyChangeEffectiveDate' ProcessName ,'WCPols05RecordID' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WCPols05Record B
	on A.WCTrackHistoryID=B.WCTrackHistoryID and A.TransactionType like 'Endorse%'
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and A.TransactionType like 'Endorse%'
	and B.PolicyChangeEffectiveDate=B.PolicyChangeExpirationDate
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.DELETED_ATRIBUTES}
	
	UNION
	
	Select C.WCPols01RecordId,
	'1' UpdatedValue,
	A.Auditid,
	'WCPols01Record' TableName,
	'ExperienceRatingCode' ProcessName,
	'WCPols01RecordId' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WorkWCProcessUpdateTable B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and A.Auditid = B.Auditid
	and B.ProcessName='ExpRatingCode-01'
	inner join WCPols01Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	and A.Auditid = C.AuditId
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and 'NC' in (@{pipeline().parameters.FILENAME})
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.NC_ATTRIBUTES}
	
	UNION
	
	Select C.WCPols01RecordId,
	'2' UpdatedValue,
	A.Auditid,
	'WCPols01Record' TableName,
	'ExperienceRatingCode' ProcessName,
	'WCPols01RecordId' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WorkWCProcessUpdateTable B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.ProcessName='ExpRatingCode-01'
	and A.Auditid = B.Auditid
	inner join WCPols01Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	and A.Auditid = C.AuditId
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and 'NC' in (@{pipeline().parameters.FILENAME})
	and '@{pipeline().parameters.SET_REVERT}'='REVERT'
	@{pipeline().parameters.NC_ATTRIBUTES}
	
	UNION
	
	Select C.WCPols04RecordID,
	'0000' UpdatedValue,
	A.Auditid,
	'WCPols04Record' TableName,
	'ExperienceModificationFactorMeritRatingFactor' ProcessName,
	'WCPols04RecordID' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WorkWCProcessUpdateTable B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and A.Auditid = B.Auditid
	and B.ProcessName='ExpModFactor-04'
	inner join WCPols04Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	and A.Auditid = C.AuditId
	and C.ExperienceModificationFactorMeritRatingFactor = '1000'
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and 'WI' in (@{pipeline().parameters.FILENAME})
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.WI_ATTRIBUTES}
	
	UNION
	
	Select C.WCPols04RecordID,
	'1000' UpdatedValue,
	A.Auditid,
	'WCPols04Record' TableName,
	'ExperienceModificationFactorMeritRatingFactor' ProcessName,
	'WCPols04RecordID' UpdatedRecord
	from WorkWCTrackHistory A
	inner join WorkWCProcessUpdateTable B
	on A.WCTrackHistoryID=B.WCTrackHistoryID
	and B.ProcessName='ExpModFactor-04'
	and A.Auditid = B.Auditid
	inner join WCPols04Record C
	on A.WCTrackHistoryID=C.WCTrackHistoryID
	and A.Auditid = C.AuditId
	and C.ExperienceModificationFactorMeritRatingFactor = '0000'
	where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and 'WI' in (@{pipeline().parameters.FILENAME})
	and '@{pipeline().parameters.SET_REVERT}'='REVERT'
	@{pipeline().parameters.WI_ATTRIBUTES}
),
EXP_Source_DeletedAttributes AS (
	SELECT
	WCPolsRecordID,
	-- *INF*: TO_CHAR(WCPolsRecordID)
	TO_CHAR(WCPolsRecordID) AS o_WCPolsRecordID,
	UpdatedValue,
	Auditid,
	TableName,
	ProcessName,
	UpdatedRecord,
	-- *INF*: IIF(Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID},
	-- UpdatedValue,'')
	IFF(Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}, UpdatedValue, '') AS v_TableUpdate,
	v_TableUpdate AS UpdateValue
	FROM SQ_WorkWCTrackHistoryState_DeletedAttributes
),
SQL_DeletedAttributes AS (-- SQL_DeletedAttributes

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
WCPOLS_UpdateStatements_FlatFile_DeletedError AS (
	INSERT INTO WCPOLS_UpdateStatements_FlatFile
	(FlatFileComments)
	SELECT 
	SQLError AS FLATFILECOMMENTS
	FROM SQL_DeletedAttributes
),
SQ_WCPols02Record_DBAName AS (
	Select distinct A.WCTrackHistoryID, REPLACE(NameOfInsured,CHAR(39),'') NameOfInsured, ProcessName,REPLACE(AttributeValue,CHAR(39),'') AttributeValue,
	A.Auditid,'WCPols02Record' TableName,'NameOfInsured' FieldName ,'WCTrackHistoryID' Condition1,'NameOfInsured' Condition2
	from WorkWCTrackHistory A
	inner join WorkWCProcessUpdateTable B
	on A.WCTrackHistoryID=B.WCTrackHistoryID and B.ProcessName='DBA'
	inner join WCPols02Record C
	on C.WCTrackHistoryID=A.WCTrackHistoryID and REPLACE(NameOfInsured,CHAR(39),'')=REPLACE(AttributeValue,CHAR(39),'')where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and '@{pipeline().parameters.SET_REVERT}'='SET'
	@{pipeline().parameters.DBA_UPDATE}
	
	UNION
	
	Select distinct A.WCTrackHistoryID, REPLACE(NameOfInsured,CHAR(39),'') NameOfInsured, ProcessName,REPLACE(AttributeValue,CHAR(39),'') AttributeValue,
	A.Auditid,'WCPols02Record' TableName,'NameOfInsured' FieldName ,'WCTrackHistoryID' Condition1,'NameOfInsured' Condition2
	from WorkWCTrackHistory A
	inner join WorkWCProcessUpdateTable B
	on A.WCTrackHistoryID=B.WCTrackHistoryID and B.ProcessName='DBA'
	inner join WCPols02Record C
	on C.WCTrackHistoryID=A.WCTrackHistoryID and REPLACE(NameOfInsured,CHAR(39),'')=CONCAT('DBA ',REPLACE(AttributeValue,CHAR(39),'')) where A.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and '@{pipeline().parameters.SET_REVERT}'='REVERT'
	@{pipeline().parameters.DBA_UPDATE}
),
EXP_DBAName_Update AS (
	SELECT
	WCPolsRecordID,
	-- *INF*: TO_CHAR(WCPolsRecordID)
	TO_CHAR(WCPolsRecordID) AS o_WCPolsRecordID,
	NameOfInsured,
	ProcessName,
	AttributeValue,
	Auditid,
	TableName,
	FieldName,
	Condition1,
	Condition2,
	-- *INF*: DECODE(Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID},
	-- @{pipeline().parameters.SET_REVERT}='SET',
	-- CONCAT(CONCAT(ProcessName,' ') ,AttributeValue),
	-- @{pipeline().parameters.SET_REVERT}='REVERT',AttributeValue,
	-- '')
	DECODE(
	    Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID},
	    @{pipeline().parameters.SET_REVERT} = 'SET', CONCAT(CONCAT(ProcessName, ' '), AttributeValue),
	    @{pipeline().parameters.SET_REVERT} = 'REVERT', AttributeValue,
	    ''
	) AS v_TableUpdate,
	v_TableUpdate AS UpdateValue
	FROM SQ_WCPols02Record_DBAName
),
SQL_DBANameUpdate AS (-- SQL_DBANameUpdate

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
WCPOLS_UpdateStatements_FlatFile_DBAName AS (
	INSERT INTO WCPOLS_UpdateStatements_FlatFile
	(FlatFileComments)
	SELECT 
	SQLError AS FLATFILECOMMENTS
	FROM SQL_DBANameUpdate
),