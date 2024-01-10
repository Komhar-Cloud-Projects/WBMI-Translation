WITH
SQ_policy_dim AS (
	DECLARE @DATE datetime
	SET @DATE = DATEADD(mm,DATEDIFF(m,0,GETDATE()),0)
	
	       SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(PTF.DirectWrittenPremium) as TotalPremiumChange
	       FROM dbo.PremiumTransactionfact PTF
	       INNER JOIN dbo.Policy_Dim POL
	         on POL.pol_dim_id = PTF.PolicyDimId
	  INNER JOIN dbo.calendar_dim CD on CD.clndr_id = PTF.PremiumTransactionBookedDateID
	       WHERE CD.clndr_date >= @DATE  ---- transactions with booked of current month and future months
	  AND POL.pol_sym = '000'
	and POL.Pol_Key in
	('058560517','106505714','110152313','124588312','181972210','185177409','205673209','206112409','208181508','213277609','219065707','219919807','228172807','A36655104','A39078904','A39464304','A39466605','A54924407','A61646603','A70490603','A72251402','A72426302','A73360802','A75868902','A86210602','A87217001','A95310101','A97627400','B00652201','B04610100','B05215700','B05741000','B07296900','B07479700','B08428500','B12535400','B12625000','B12737000','B12761200','B13225500',
	'B13900000','B14765500','B17117400','B17389900','B18154800','B18713900','B18739500','B18826900','B20434200','B20552400','B20762500','B21084500','B21169000','B21377000','B21580400','B21746900','B21863600','B21863700','B21874900','B21875400','B22018900','B22034500')
	  GROUP BY POL.Pol_Num, POL.Pol_Key
),
SQ_policy_dim1 AS (
	DECLARE @DATE datetime
	SET @DATE = DATEADD(mm,DATEDIFF(m,0,GETDATE()),0)
	         SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(PMF.PremiumMasterDirectWrittenPremium) as TotalPremiumChange
	       FROM dbo.PremiumMasterFact PMF
	       INNER JOIN dbo.Policy_Dim POL
		  
	  on POL.pol_dim_id = PMF.PolicyDimId
	   inner join calendar_dim c on c.clndr_id = pmf.PremiumMasterRunDateID
	     WHERE POL.pol_sym = '000'
		 and POL.Pol_Key in
	('058560517','106505714','110152313','124588312','181972210','185177409','205673209','206112409','208181508','213277609','219065707','219919807','228172807','A36655104','A39078904','A39464304','A39466605','A54924407','A61646603','A70490603','A72251402','A72426302','A73360802','A75868902','A86210602','A87217001','A95310101','A97627400','B00652201','B04610100','B05215700','B05741000','B07296900','B07479700','B08428500','B12535400','B12625000','B12737000','B12761200','B13225500',
	'B13900000','B14765500','B17117400','B17389900','B18154800','B18713900','B18739500','B18826900','B20434200','B20552400','B20762500','B21084500','B21169000','B21377000','B21580400','B21746900','B21863600','B21863700','B21874900','B21875400','B22018900','B22034500')
	       GROUP BY
	              POL.Pol_Num,
	              POL.Pol_Key
),
SQ_policy_dim2 AS (
	DECLARE @DATE datetime
	SET @DATE = DATEADD(mm,DATEDIFF(m,0,GETDATE()),0)
	       SELECT POL.Pol_Num as SourceReference,
	              POL.Pol_Key as SourceUID,
	              sum(PTCT.PassThroughChargeTransactionAmount) as TotalPremiumChange
	       FROM dbo.PassThroughChargeTransactionfact PTCT
	       INNER JOIN dbo.Policy_Dim POL
	         on POL.pol_dim_id = PTCT.PolicyDimId
			    inner join calendar_dim c on c.clndr_id = ptct.PassThroughChargeTransactionBookedDateId
	 WHERE POL.pol_sym = '000'
	 and 
	 POL.Pol_Key in
	('058560517','106505714','110152313','124588312','181972210','185177409','205673209','206112409','208181508','213277609','219065707','219919807','228172807','A36655104','A39078904','A39464304','A39466605','A54924407','A61646603','A70490603','A72251402','A72426302','A73360802','A75868902','A86210602','A87217001','A95310101','A97627400','B00652201','B04610100','B05215700','B05741000','B07296900','B07479700','B08428500','B12535400','B12625000','B12737000','B12761200','B13225500',
	'B13900000','B14765500','B17117400','B17389900','B18154800','B18713900','B18739500','B18826900','B20434200','B20552400','B20762500','B21084500','B21169000','B21377000','B21580400','B21746900','B21863600','B21863700','B21874900','B21875400','B22018900','B22034500')
	       GROUP BY     POL.Pol_Num,
	              POL.Pol_Key
),
UN_all AS (
	SELECT SourceReference, SourceUID, TotalPremiumChange
	FROM SQ_policy_dim
	UNION
	SELECT SourceReference, SourceUID, TotalPremiumChange
	FROM SQ_policy_dim1
	UNION
	SELECT SourceReference, SourceUID, TotalPremiumChange
	FROM SQ_policy_dim2
),
AGG_SR_SUID AS (
	SELECT
	SourceReference,
	SourceUID,
	TotalPremiumChange,
	-- *INF*: SUM(TotalPremiumChange)
	SUM(TotalPremiumChange) AS TotalPremiumChangeOut
	FROM UN_all
	GROUP BY SourceReference, SourceUID
),
EXP_Values AS (
	SELECT
	SourceReference,
	SourceUID,
	TotalPremiumChangeOut AS TotalPremiumChange,
	-- *INF*: '{' ||
	--    '"SourceSystemCode":"DWMRT",' ||  
	--    '"ComponentName":"PolicyMart",' ||
	--    '"TypeCode":"EXBAL",' ||
	--    '"SourceReference":"'|| SourceReference || '",' ||
	--    '"SourceTypeCode":"POLKY",'  || 
	--    '"SourceUID":"'|| SourceUID ||'",' ||  
	--    '"TransactionType":"All Transactions",'  || 
	--    '"TransactionTotal":' || TO_CHAR(TotalPremiumChange) ||
	-- '}'
	'{' || '"SourceSystemCode":"DWMRT",' || '"ComponentName":"PolicyMart",' || '"TypeCode":"EXBAL",' || '"SourceReference":"' || SourceReference || '",' || '"SourceTypeCode":"POLKY",' || '"SourceUID":"' || SourceUID || '",' || '"TransactionType":"All Transactions",' || '"TransactionTotal":' || TO_CHAR(TotalPremiumChange) || '}' AS v_JSON_row,
	v_JSON_row AS Json_RowData,
	@{pipeline().parameters.TARGETFILE} AS Filename
	FROM AGG_SR_SUID
),
TransactionalData_JSONFile AS (
	INSERT INTO JSONFile
	(JSONMessage, FileName)
	SELECT 
	Json_RowData AS JSONMESSAGE, 
	Filename AS FILENAME
	FROM EXP_Values
),