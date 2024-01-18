WITH
SQ_SupWCPOLS_StateCode AS (
	WITH cte_StateCodeUpdates as
	(SELECT A.SourcesystemID, 
	A.WCPOLSCode, 
	A.SourceCode, 
	A.TableName, 
	A.ProcessName 
	FROM
	dbo.SUPWCPOLS A with (nolock)
	inner join (
	select SourcesystemID,TableName,ProcessName,SourceCode from dbo.SupWCPOLS with (nolock)
	where SourceCode not in ('NotusedbyWB','logic based')
	group by SourcesystemID,TableName,ProcessName,SourceCode
	having count(1)>1) B
	on A.SourcesystemID=B.SourcesystemID
	and A.TableName=B.TableName
	and A.ProcessName=B.ProcessName
	and A.SourceCode=B.SourceCode
	where 
	PATINDEX('%StateCode%',A.ProcessName) > 0 AND
	(('@{pipeline().parameters.FILENAME}'='NCCI' and A.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and A.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and A.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and A.MNRequiredFlag=1))
	)
	
	SELECT 
	distinct B.SourcesystemID,
	B.WCPOLSCode,
	B.SourceCode,
	B.TableName,
	B.ProcessName,
	COLUMN_NAME StateCodeField
	FROM 
	INFORMATION_SCHEMA.COLUMNS A with (nolock)
	inner join cte_StateCodeUpdates B
	on A.TABLE_NAME=B.TableName
	WHERE 
	EXISTS (select 1 from WCPols03Record where ForeignAddressIndicator='Y' and AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and AddressState=B.SourceCode) AND
	COLUMN_NAME like '%StateCode%' AND 
	COLUMN_NAME !='StateCodeLink' 
	order by 4,5,6
),
EXP_SrcDataCollect_StateCode AS (
	SELECT
	SourcesystemID,
	WCPOLSCode,
	SourceCode,
	TableName,
	ProcessName,
	StateCodeField,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID
	FROM SQ_SupWCPOLS_StateCode
),
EXP_SqlStateCodeOutput AS (
	SELECT
	@{pipeline().parameters.FILENAME}||'_1'||'.txt' AS FileName,
	SourcesystemID AS SourcesystemID_output,
	WCPOLSCode AS WCPOLSCode_output,
	SourceCode AS SourceCode_output,
	TableName AS TableName_output,
	ProcessName AS ProcessName_output,
	StateCodeField,
	AuditID,
	-- *INF*: 'UPDATE A SET A.'||StateCodeField||'=' || CHR(39) ||WCPOLSCode_output || CHR(39) ||' FROM dbo.' || TableName_output ||'  A INNER JOIN dbo.SupWCPOLS B ON A.'|| StateCodeField ||'=B.WCPOLSCode INNER JOIN WCPols03Record C on A.WCTrackHistoryID=C.WCTrackHistoryID and C.ForeignAddressIndicator= '|| CHR(39) ||'Y' || CHR(39) || ' and C.AddressState=B.SourceCode  WHERE B.SourcesystemID=' ||CHR(39) ||  SourcesystemID_output || CHR(39)  ||' AND B.TableName=' ||CHR(39) ||  TableName_output || CHR(39) ||' AND B.ProcessName='|| CHR(39) || ProcessName_output ||CHR(39) || ' AND B.SourceCode='|| CHR(39) || SourceCode_output || CHR(39) || 
	-- ' AND C.AuditId=' || AuditID
	-- 
	-- -- based on UPDATE A
	-- --SET A.~StateCodeField~=?WCPOLSCode?
	-- --FROM dbo.~TableName~ A
	-- --INNER JOIN dbo.SupWCPOLS B
	-- --ON A.~StateCodeField~=B.WCPOLSCode
	-- --WHERE B.SourcesystemID=?SourcesystemID?
	-- --AND B.TableName=?TableName?
	-- --AND B.ProcessName=?ProcessName?
	-- --AND B.SourceCode=?SourceCode?
	-- -- integrate a foreign address field indicator since US and CA address codes overlap
	-- --inner join WCPols03Record C on A.WCTrackHistoryID=C.WCTrackHistoryID and C.ForeignAddressIndicator='Y'
	'UPDATE A SET A.' || StateCodeField || '=' || CHR(39) || WCPOLSCode_output || CHR(39) || ' FROM dbo.' || TableName_output || '  A INNER JOIN dbo.SupWCPOLS B ON A.' || StateCodeField || '=B.WCPOLSCode INNER JOIN WCPols03Record C on A.WCTrackHistoryID=C.WCTrackHistoryID and C.ForeignAddressIndicator= ' || CHR(39) || 'Y' || CHR(39) || ' and C.AddressState=B.SourceCode  WHERE B.SourcesystemID=' || CHR(39) || SourcesystemID_output || CHR(39) || ' AND B.TableName=' || CHR(39) || TableName_output || CHR(39) || ' AND B.ProcessName=' || CHR(39) || ProcessName_output || CHR(39) || ' AND B.SourceCode=' || CHR(39) || SourceCode_output || CHR(39) || ' AND C.AuditId=' || AuditID AS SqlUpdate
	FROM EXP_SrcDataCollect_StateCode
),
FIL_RemoveIfNoStateCodeFieldsApplicable AS (
	SELECT
	SourcesystemID_output AS SourcesystemID, 
	WCPOLSCode_output AS WCPOLSCode, 
	SourceCode_output AS SourceCode, 
	TableName_output AS TableName, 
	ProcessName_output AS ProcessName, 
	StateCodeField, 
	SqlUpdate
	FROM EXP_SqlStateCodeOutput
	WHERE NOT ISNULL(StateCodeField) AND (ISNULL(SQLError) OR length(rtrim(ltrim(SQLError)))=0)
),
SRT_SortUpdateStateCommands AS (
	SELECT
	TableName, 
	ProcessName, 
	SourceCode, 
	WCPOLSCode, 
	SourcesystemID, 
	StateCodeField, 
	SqlUpdate
	FROM FIL_RemoveIfNoStateCodeFieldsApplicable
	ORDER BY TableName ASC, ProcessName ASC, SourceCode ASC, WCPOLSCode ASC
),
SQL_UpdateStateCodeFields AS (-- SQL_UpdateStateCodeFields

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_checkForError AS (
	SELECT
	SQLError,
	@{pipeline().parameters.FILENAME}||'_2'||'.txt' AS FileName,
	NumRowsAffected,
	SqlUpdate_output,
	-- *INF*: SqlUpdate_output  || ' ----- Rows Affected:    '  ||   TO_CHAR(NumRowsAffected)
	SqlUpdate_output || ' ----- Rows Affected:    ' || TO_CHAR(NumRowsAffected) AS SqlResults,
	@{pipeline().parameters.FILENAME}||'_StateLog'||'.txt' AS FileName_StateLog
	FROM SQL_UpdateStateCodeFields
),
FIL_SQLError2 AS (
	SELECT
	SQLError, 
	FileName
	FROM EXP_checkForError
	WHERE NOT ISNULL(SQLError)
),
SQLError2 AS (
	INSERT INTO SQLError
	(SQLError, FileName)
	SELECT 
	SQLERROR, 
	FILENAME
	FROM FIL_SQLError2
),
SQLError3 AS (
	INSERT INTO SQLError
	(SQLError, FileName)
	SELECT 
	SqlResults AS SQLERROR, 
	FileName_StateLog AS FILENAME
	FROM EXP_checkForError
),
SQ_SupWCPOLS_regular AS (
	SELECT A.SourcesystemID, 
	A.WCPOLSCode, 
	A.SourceCode, 
	A.TableName, 
	A.ProcessName 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.SUPWCPOLS A with (nolock)
	inner join (
	select SourcesystemID,TableName,ProcessName,SourceCode from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLS with (nolock)
	where SourceCode not in ('NotusedbyWB','logic based')
	group by SourcesystemID,TableName,ProcessName,SourceCode
	having count(1)>1) B
	on A.SourcesystemID=B.SourcesystemID
	and A.TableName=B.TableName
	and A.ProcessName=B.ProcessName
	and A.SourceCode=B.SourceCode
	where 
	PATINDEX('%StateCode%',A.ProcessName) < 1 AND
	(('@{pipeline().parameters.FILENAME}'='NCCI' and A.NCCIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='WI' and A.WIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MI' and A.MIRequiredFlag=1) OR
	('@{pipeline().parameters.FILENAME}'='MN' and A.MNRequiredFlag=1))
),
EXP_SrcDataCollect_regular AS (
	SELECT
	SourcesystemID,
	WCPOLSCode,
	SourceCode,
	TableName,
	ProcessName,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID
	FROM SQ_SupWCPOLS_regular
),
SQL AS (-- SQL

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_SQLError AS (
	SELECT
	SQLError,
	@{pipeline().parameters.FILENAME}||'.txt' AS FileName
	FROM SQL
),
FIL_SqlError AS (
	SELECT
	SQLError, 
	FileName
	FROM EXP_SQLError
	WHERE NOT ISNULL(SQLError)
),
SQLError AS (
	INSERT INTO SQLError
	(SQLError, FileName)
	SELECT 
	SQLERROR, 
	FILENAME
	FROM FIL_SqlError
),