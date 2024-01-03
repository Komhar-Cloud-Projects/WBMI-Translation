WITH
SQ_Source_Queries AS (

-- TODO Manual --

),
EXP_Param_Substitution AS (
	SELECT
	Query
	FROM SQ_Source_Queries
),
SQL_Enable_Indexes AS (-- SQL_Enable_Indexes

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_Generate_Log AS (
	SELECT
	SQLError,
	Query_output AS SQL_Query_output,
	-- *INF*: TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS')||' SQL Statement: '||CHR(10)||SQL_Query_output||CHR(10)||DECODE(TRUE,ISNULL(SQLError),'Command(s) completed successfully. ','SQLError: '||SQLError)
	TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS') || ' SQL Statement: ' || CHR(10) || SQL_Query_output || CHR(10) || DECODE(TRUE,
	SQLError IS NULL, 'Command(s) completed successfully. ',
	'SQLError: ' || SQLError) AS Result
	FROM SQL_Enable_Indexes
),
Execution_Results AS (
	INSERT INTO Execution_Results
	(Result)
	SELECT 
	RESULT
	FROM EXP_Generate_Log
),