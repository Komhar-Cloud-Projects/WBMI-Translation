WITH
SQ_WorkControlDeleteQueries AS (
	SELECT
		WorkControlDeleteQueriesId,
		CreatedDate,
		ControlDeleteQueries
	FROM WorkControlDeleteQueries
),
EXP_default AS (
	SELECT
	WorkControlDeleteQueriesId,
	CreatedDate,
	ControlDeleteQueries
	FROM SQ_WorkControlDeleteQueries
),
SQL_deleterules AS (-- SQL_deleterules

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_deletequeries AS (
	SELECT
	ControlDeleteQueries_output AS o_ControlDeleteQueries
	FROM SQL_deleterules
),
Tgt_File_Result AS (
	INSERT INTO Tgt_File_Result
	(Result)
	SELECT 
	o_ControlDeleteQueries AS RESULT
	FROM EXP_deletequeries
),