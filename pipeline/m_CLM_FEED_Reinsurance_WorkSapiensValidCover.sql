WITH
SQ_RIVCATV AS (
	select 
	'ACP' as Cover,
	RTRIM(DEPENDENCY_LVL_1) as Value,  
	NULL as ASL, 
	NULL as SAS 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'ACP' and VAL_STATUS = 'ACT'
	union
	select 
	'LOB' as Cover, 
	RTRIM(DEPENDENCY_LVL_1) as Value, 
	NULL as ASL, 
	NULL as SAS 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'LOB' and VAL_STATUS = 'ACT'
	union
	select 
	'PCN' as Cover, 
	RTRIM(DEPENDENCY_LVL_1) as Value,
	NULL as ASL, 
	NULL as SAS 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'PCN' and VAL_STATUS = 'ACT'
	union
	select 
	'PDT' as Cover, 
	RTRIM(DEPENDENCY_LVL_1) as Value, 
	null as ASL, 
	NULL as SAS 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'PDT' and VAL_STATUS = 'ACT'
	union
	select 
	'RKS' as Cover, 
	RTRIM(DEPENDENCY_LVL_1) as Value, 
	NULL as ASL, 
	NULL as SAS 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'RKS' and VAL_STATUS = 'ACT'
	union
	select 
	'ASL' as Cover, 
	RTRIM(DEPENDENCY_LVL_1) as Value, 
	NULL as ASL, 
	NULL as SAS 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'ASL' and VAL_STATUS = 'ACT'
	union
	select 
	'SAS' as cover,
	A.SASValue as Value,
	B.ASLVal as ASL,
	null as SAS
	from
	(select 
	RTRIM(DEPENDENCY_LVL_2) as SASValue, 
	DEPENDENCY_LVL_1 as ASLValue 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'SAS' and VAL_STATUS = 'ACT') A
	left join
	(select 
	DEPENDENCY_LVL_1 as ASLVal 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'ASL' and VAL_STATUS = 'ACT') B
	on B.ASLVal = A.ASLValue
	union
	select 
	'SNA' as cover,
	A.SNAValue as Value,
	B.ASLVal as ASL,
	B.SASVal as SAS
	from
	(select 
	RTRIM(DEPENDENCY_LVL_3) as SNAValue, 
	DEPENDENCY_LVL_2 as SASValue, 
	DEPENDENCY_LVL_1 as ASLValue 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'SNA' and VAL_STATUS = 'ACT') A
	left join
	(select 
	RTRIM(DEPENDENCY_LVL_2) as SASVal, 
	DEPENDENCY_LVL_1 as ASLVal 
	from RIAPPLDB.RI.RIVCATV with (nolock) 
	where CATEGORY_CODE = 'SAS' and VAL_STATUS = 'ACT') B
	on B.SASVal = A.SASValue
),
EXPTRANS AS (
	SELECT
	SYSDATE AS Default,
	Cover,
	Value,
	ASL,
	-- *INF*: LTRIM(RTRIM(ASL))
	LTRIM(RTRIM(ASL)) AS o_ASL,
	SAS,
	-- *INF*: LTRIM(RTRIM(SAS))
	LTRIM(RTRIM(SAS)) AS o_SAS
	FROM SQ_RIVCATV
),
WorkSapiensValidCover AS (
	TRUNCATE TABLE WorkSapiensValidCover;
	INSERT INTO WorkSapiensValidCover
	(CreatedDate, ModifiedDate, Cover, Value, ASL, SAS)
	SELECT 
	Default AS CREATEDDATE, 
	Default AS MODIFIEDDATE, 
	COVER, 
	VALUE, 
	o_ASL AS ASL, 
	o_SAS AS SAS
	FROM EXPTRANS
),