WITH
SQ_WorkTokenExResponse_Customer AS (
	SELECT 1 as SSNId, 1 as SSNToken
),
EXP_INPUT AS (
	SELECT
	SSNId,
	SSNToken
	FROM SQ_WorkTokenExResponse_Customer
),
SQL_Customer AS (-- SQL_Customer

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
sql_error_Customer AS (
	INSERT INTO sql_error
	(sql_error)
	SELECT 
	SQLError AS SQL_ERROR
	FROM SQL_Customer
),