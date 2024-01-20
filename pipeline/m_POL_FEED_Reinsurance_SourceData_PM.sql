WITH
LKP_Existing_PolicyCovers AS (
	SELECT
	ASLCoversKey
	FROM (
		select A.ASLCoversKey as ASLCoversKey
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy A
		inner join
		(
		select PolicyKey,max(case when DocumntType='N' then SourceSequenceNumber else 0 end) max_SourceSequenceNumber
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
		group by PolicyKey) B
		on A.PolicyKey=B.PolicyKey
		and A.SourceSequenceNumber>B.max_SourceSequenceNumber
		group by A.ASLCoversKey
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ASLCoversKey ORDER BY ASLCoversKey DESC) = 1
),
LKP_TransCode AS (
	SELECT
	TargetAttributeValue,
	SourceAttributeValue
	FROM (
		select SourceAttributeValue as SourceAttributeValue,
		TargetAttributeValue as TargetAttributeValue from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceLookup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SourceAttributeValue ORDER BY TargetAttributeValue DESC) = 1
),
LKP_SupReinsuranceUmbrellaLayer AS (
	SELECT
	ReinsuranceUmbrellaLayer,
	StrategicProfitCenterAbbreviation,
	SourceUmbrellaLayerStart,
	SourceUmbrellaLayerEnd,
	EffectiveDate,
	ExpirationDate
	FROM (
		SELECT 
			ReinsuranceUmbrellaLayer,
			StrategicProfitCenterAbbreviation,
			SourceUmbrellaLayerStart,
			SourceUmbrellaLayerEnd,
			EffectiveDate,
			ExpirationDate
		FROM SapiensReinsuranceUmbrellaLayer
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StrategicProfitCenterAbbreviation,SourceUmbrellaLayerStart,SourceUmbrellaLayerEnd,EffectiveDate,ExpirationDate ORDER BY ReinsuranceUmbrellaLayer DESC) = 1
),
LKP_Tgt AS (
	SELECT
	OutPut,
	ASLCoversKey
	FROM (
		select A.ASLCoversKey as ASLCoversKey,
		convert(varchar,max(case when A.entryprocess='DAILY' then 0 when  A.SourceSequenceNumber<=B.max_SourceSequenceNumber then 0 else A.EndorsementNumber end))+'|'+convert(varchar,max(A.TransactionNumber))+'|' as OutPut
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy A
		inner join
		(
		select PolicyKey,max(case when DocumntType='N' then SourceSequenceNumber else 0 end) max_SourceSequenceNumber
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
		group by PolicyKey) B
		on A.PolicyKey=B.PolicyKey
		--and A.SourceSequenceNumber>B.max_SourceSequenceNumber
		group by A.ASLCoversKey
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ASLCoversKey ORDER BY OutPut DESC) = 1
),
LKP_Get_Max_Sapiens_SourceSequenceNumber AS (
	SELECT
	Source_Seq_Num,
	ID
	FROM (
		SELECT MAX(A.SourceSequenceNumber) AS Source_Seq_Num,
			1 AS ID
		FROM (
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaimRestate
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaimRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicyRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicyRestate	) A
			--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Source_Seq_Num DESC) = 1
),
SQ_PremiumMasterFact AS (
	Declare @Date1 as datetime,
	                 @Date2 as datetime
	Set @Date1=dateadd(dd,-1,dateadd(MM,-@{pipeline().parameters.NO_OF_MONTHS},dateadd(mm,Datediff(mm,0,getdate()),0)))
	Set @Date2=case when '@{pipeline().parameters.PMSESSIONNAME}' like '%Restate%' or '@{pipeline().parameters.PMSESSIONNAME}' like '%Historical%' then '2001-01-31 00:00:00'  else dateadd(MM,-@{pipeline().parameters.NO_OF_MONTHS}-1,dateadd(mm,Datediff(mm,0,getdate()),0)) end
	
	select Pol_Sym,Pol_key,
	PremiumTransactionCode,PremiumTypeCode,
	Accounting_Date,PersonalUmbrellaLayer2Premium,PersonalUmbrellaLayer1Premium,
	PremiumMasterDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	CASE 
			WHEN asl_code = '220' then '220'
			ELSE CASE WHEN sub_asl_code = 'N/A' THEN NULL ELSE sub_asl_code END
		END AS SubASLCode,
	state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	UmbrellaLayer,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	Name
	from
	(
	select Pol_Sym,Pol_key,
	PremiumTransactionCode,PremiumTypeCode,
	Accounting_Date,PersonalUmbrellaLayer2Premium,PersonalUmbrellaLayer1Premium,
	PremiumMasterDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,sub_asl_code,state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	UmbrellaLayer,
	min(Policy_Issue_Date) over(partition by Pol_key,ProductCode,StrategicProfitCenterAbbreviation,AccountingProductCode,InsuranceReferenceLineOfBusinessCode,asl_code,sub_asl_code,state_of_domicile_code)  Policy_Issue_Date,
	min(Policy_Start_Date) over(partition by Pol_key,ProductCode,StrategicProfitCenterAbbreviation,AccountingProductCode,InsuranceReferenceLineOfBusinessCode,asl_code,sub_asl_code,state_of_domicile_code) Policy_Start_Date,
	max(Policy_End_Date) over(partition by Pol_key,ProductCode,StrategicProfitCenterAbbreviation,AccountingProductCode,InsuranceReferenceLineOfBusinessCode,asl_code,sub_asl_code,state_of_domicile_code) Policy_End_Date,
	RatingPlanAbbreviation,
	Name
	from 
	(select P.Pol_Sym,P.Pol_key,
	PTT.PremiumTransactionCode,PTT.PremiumTypeCode,
	RD.clndr_date Accounting_Date,
	case when P.Pol_Sym<>'000' and IRD.StrategicProfitCenterAbbreviation='WB - PL' and IRD.ProductCode='890' then PM.PremiumMasterCededWrittenPremium else 0.0 end PersonalUmbrellaLayer2Premium,
	case when P.Pol_Sym<>'000' and IRD.StrategicProfitCenterAbbreviation='WB - PL' and IRD.ProductCode='890' then PM.PremiumMasterDirectWrittenPremium-PM.PremiumMasterCededWrittenPremium else 0.0 end PersonalUmbrellaLayer1Premium,
	PM.PremiumMasterDirectWrittenPremium,
	IRD.ProductCode,
	replace(CASE WHEN IRD.StrategicProfitCenterAbbreviation = 'Argent' THEN 'A' ELSE IRD.StrategicProfitCenterAbbreviation END,' ','') StrategicProfitCenterAbbreviation,IRD.AccountingProductCode,
	IRD.InsuranceReferenceLineOfBusinessCode,
	ASL.asl_code,
	CASE 
		WHEN ASL.asl_code = '220' then '220'
		ELSE CASE WHEN ASL.sub_asl_code = 'N/A' THEN NULL ELSE ASL.sub_asl_code END
	END AS sub_asl_code,
	case when IRD.InsuranceReferenceLineOfBusinessCode in ('590','812') then RL.StateProvinceCode else P.state_of_domicile_code end state_of_domicile_code,
	EFD.clndr_date Endorsement_Start_Date,
	ETD.clndr_date Endorsement_Issue_Date,
	PM.EDWPremiumMasterCalculationPKID,
	ISNULL(CUD.UmbrellaLayer,0) UmbrellaLayer,
	ETD.clndr_date Policy_Issue_Date
	,EFD.clndr_date Policy_Start_Date
	,EXD.clndr_date Policy_End_Date,ISNULL(IRD.RatingPlanAbbreviation,'NULL') RatingPlanAbbreviation,ISNULL(CCD.Name,'NULL') Name
	from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterFact PM
	inner JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P
	ON PM.PolicyDimID=P.pol_dim_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransactionTypeDim PTT
	on PM.PremiumTransactionTypeDimID=PTT.PremiumTransactionTypeDimID
	Inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim RD
	on PM.PremiumMasterRunDateID=RD.clndr_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim EFD
	on PM.PremiumMasterCoverageEffectiveDateID=EFD.clndr_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim ETD
	on PM.PremiumTransactionEnteredDateID=ETD.clndr_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD
	on PM.InsuranceReferenceDimID=IRD.InsuranceReferenceDimId
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim ASL
	on PM.AnnualStatementLineDimID=ASL.asl_dim_id
	Left join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailCommercialUmbrellaDim CUD
	on PM.CoverageDetailDimId=CUD.CoverageDetailDimId
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim EXD
	on PM.PremiumMasterCoverageExpirationDateID=EXD.clndr_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer_dim CCD
	on PM.ContractCustomerDimID=CCD.contract_cust_dim_id
	inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocationDim RL
	on PM.RiskLocationDimID=RL.RiskLocationDimID
	--where (PM.PremiumMasterDirectWrittenPremium<>0.0 or PM.PremiumMasterCededWrittenPremium<>0.0)
	@{pipeline().parameters.WHERE}
	) A ) A 
	Where A.Accounting_Date between @Date2 and @Date1
	and case when Pol_Sym<>'000' and StrategicProfitCenterAbbreviation='WB-PL' and ProductCode='890' then 1 when PremiumTypeCode='D' then 1 else 0 end=1
	Order By  Pol_key,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	Accounting_Date,
	Endorsement_Issue_Date,
	Endorsement_Start_Date,
	EDWPremiumMasterCalculationPKID
),
EXP_Src_DataCollect AS (
	SELECT
	Pol_Sym,
	pol_key,
	PremiumTransactionCode,
	PremiumTypeCode,
	Accounting_Date,
	PersonalUmbrellaLayer2Premium,
	PersonalUmbrellaLayer1Premium,
	MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	-- *INF*: DECODE (TRUE,
	-- asl_code = '220',sub_asl_code,
	-- asl_code = '440',sub_asl_code,
	-- asl_code = '500',sub_asl_code,
	-- Policy_Start_Date >= TO_DATE('2020-01-01','YYYY-MM-DD'),sub_asl_code,
	-- pol_key = 'A08302003',sub_asl_code,
	-- pol_key = 'NSP134447200',sub_asl_code,
	-- pol_key = 'NAQ196857501',sub_asl_code,
	-- pol_key = 'A24779000',sub_asl_code,
	-- NULL)
	-- --all policies newer than 1/1/2020, asl codes of 220, 440, 500 and the above select policies
	DECODE(
	    TRUE,
	    asl_code = '220', sub_asl_code,
	    asl_code = '440', sub_asl_code,
	    asl_code = '500', sub_asl_code,
	    Policy_Start_Date >= TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD'), sub_asl_code,
	    pol_key = 'A08302003', sub_asl_code,
	    pol_key = 'NSP134447200', sub_asl_code,
	    pol_key = 'NAQ196857501', sub_asl_code,
	    pol_key = 'A24779000', sub_asl_code,
	    NULL
	) AS o_sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	2 AS PersonalUmbrellaLayer2,
	1 AS PersonalUmbrellaLayer1,
	UmbrellaLayer,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	name,
	-- *INF*: IIF(Pol_Sym<>'000' and StrategicProfitCenterAbbreviation='WB-PL' and ProductCode='890','Y','N')
	IFF(
	    Pol_Sym <> '000' and StrategicProfitCenterAbbreviation = 'WB-PL' and ProductCode = '890',
	    'Y',
	    'N'
	) AS Split_Flag
	FROM SQ_PremiumMasterFact
),
RTR_PLSplit AS (
	SELECT
	pol_key,
	PremiumTransactionCode,
	PremiumTypeCode,
	Accounting_Date,
	PersonalUmbrellaLayer2Premium,
	PersonalUmbrellaLayer1Premium,
	MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	o_sub_asl_code AS sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	PersonalUmbrellaLayer2,
	PersonalUmbrellaLayer1,
	UmbrellaLayer,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	name,
	Split_Flag
	FROM EXP_Src_DataCollect
),
RTR_PLSplit_PersonalLinesUmbrella AS (SELECT * FROM RTR_PLSplit WHERE Split_Flag='Y'),
RTR_PLSplit_CommercialLines AS (SELECT * FROM RTR_PLSplit WHERE Split_Flag='N'),
EXP_CLines_DataCollect AS (
	SELECT
	pol_key,
	PremiumTransactionCode,
	PremiumTypeCode,
	Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	UmbrellaLayer,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	name
	FROM RTR_PLSplit_CommercialLines
),
EXP_PLines_DataCollect AS (
	SELECT
	pol_key,
	PremiumTransactionCode,
	PremiumTypeCode,
	Accounting_Date,
	PersonalUmbrellaLayer1Premium,
	PersonalUmbrellaLayer2Premium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	PersonalUmbrellaLayer AS PersonalUmbrellaLayer1,
	PersonalUmbrellaLayer2,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	name
	FROM RTR_PLSplit_PersonalLinesUmbrella
),
NRM_Covert_Columns_Rows AS (
),
FIL_Drop_Zero_Premiums AS (
	SELECT
	Pol_Key, 
	PremiumTransactionCode, 
	PremiumTypeCode, 
	AccountingDate, 
	MonthlyDirectWrittenPremium, 
	ProductCode, 
	StrategicProfitCenterAbbrevation, 
	AccountingProductCode, 
	InsuranceReferenceLineCode, 
	asl_code, 
	sub_asl_code, 
	state_of_domicile_code, 
	Endorsement_Start_Date, 
	Endorsement_Issue_Date, 
	EDWPremiumMasterCalculationPKID, 
	UmbrellaLayer, 
	Policy_Issue_Date, 
	Policy_Start_Date, 
	Policy_End_Date, 
	RatingPlanAbbrevation, 
	name
	FROM NRM_Covert_Columns_Rows
	WHERE MonthlyDirectWrittenPremium<>0.0
),
UN_PLines_CLines AS (
	SELECT Pol_Key AS pol_key, PremiumTransactionCode, PremiumTypeCode, AccountingDate AS Accounting_Date, MonthlyDirectWrittenPremium AS MonthlyTotalDirectWrittenPremium, ProductCode, StrategicProfitCenterAbbrevation AS StrategicProfitCenterAbbreviation, AccountingProductCode, InsuranceReferenceLineCode AS InsuranceReferenceLineOfBusinessCode, asl_code, sub_asl_code, state_of_domicile_code, Endorsement_Start_Date, Endorsement_Issue_Date, EDWPremiumMasterCalculationPKID, UmbrellaLayer, Policy_Issue_Date, Policy_Start_Date, Policy_End_Date, RatingPlanAbbrevation AS RatingPlanAbbreviation, name
	FROM FIL_Drop_Zero_Premiums
	UNION
	SELECT pol_key, PremiumTransactionCode, PremiumTypeCode, Accounting_Date, MonthlyTotalDirectWrittenPremium, ProductCode, StrategicProfitCenterAbbreviation, AccountingProductCode, InsuranceReferenceLineOfBusinessCode, asl_code, sub_asl_code, state_of_domicile_code, Endorsement_Start_Date, Endorsement_Issue_Date, EDWPremiumMasterCalculationPKID, UmbrellaLayer, Policy_Issue_Date, Policy_Start_Date, Policy_End_Date, RatingPlanAbbreviation, name
	FROM EXP_CLines_DataCollect
),
EXP_Get_Umbrella AS (
	SELECT
	pol_key,
	PremiumTransactionCode,
	PremiumTypeCode,
	Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	-- *INF*: IIF(Endorsement_Start_Date>Policy_End_Date,Policy_End_Date,Endorsement_Start_Date)
	IFF(Endorsement_Start_Date > Policy_End_Date, Policy_End_Date, Endorsement_Start_Date) AS O_Endorsement_Start_Date,
	Endorsement_Issue_Date,
	-- *INF*: IIF(Endorsement_Issue_Date>Policy_End_Date,Policy_End_Date,Endorsement_Issue_Date)
	IFF(Endorsement_Issue_Date > Policy_End_Date, Policy_End_Date, Endorsement_Issue_Date) AS O_Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	UmbrellaLayer,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	name,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SUPREINSURANCEUMBRELLALAYER(StrategicProfitCenterAbbreviation,UmbrellaLayer,Endorsement_Start_Date)),0,:LKP.LKP_SUPREINSURANCEUMBRELLALAYER(StrategicProfitCenterAbbreviation,UmbrellaLayer,Endorsement_Start_Date))
	IFF(
	    LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_Endorsement_Start_Date.ReinsuranceUmbrellaLayer IS NULL,
	    0,
	    LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_Endorsement_Start_Date.ReinsuranceUmbrellaLayer
	) AS ReinsuranceUmbrellaLayer
	FROM UN_PLines_CLines
	LEFT JOIN LKP_SUPREINSURANCEUMBRELLALAYER LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_Endorsement_Start_Date
	ON LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_Endorsement_Start_Date.StrategicProfitCenterAbbreviation = StrategicProfitCenterAbbreviation
	AND LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_Endorsement_Start_Date.SourceUmbrellaLayerStart = UmbrellaLayer
	AND LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_Endorsement_Start_Date.SourceUmbrellaLayerEnd = Endorsement_Start_Date

),
SRT_Get_Order_Prior_Current_RecordComparison AS (
	SELECT
	pol_key, 
	PremiumTransactionCode, 
	PremiumTypeCode, 
	MonthlyTotalDirectWrittenPremium, 
	ProductCode, 
	StrategicProfitCenterAbbreviation, 
	AccountingProductCode, 
	InsuranceReferenceLineOfBusinessCode, 
	asl_code, 
	sub_asl_code, 
	state_of_domicile_code, 
	ReinsuranceUmbrellaLayer, 
	UmbrellaLayer, 
	Policy_Issue_Date, 
	Policy_Start_Date, 
	Policy_End_Date, 
	Accounting_Date, 
	O_Endorsement_Issue_Date AS Endorsement_Issue_Date, 
	O_Endorsement_Start_Date AS Endorsement_Start_Date, 
	EDWPremiumMasterCalculationPKID, 
	RatingPlanAbbreviation, 
	name
	FROM EXP_Get_Umbrella
	ORDER BY pol_key ASC, ProductCode ASC, StrategicProfitCenterAbbreviation ASC, AccountingProductCode ASC, InsuranceReferenceLineOfBusinessCode ASC, asl_code ASC, sub_asl_code ASC, state_of_domicile_code ASC, ReinsuranceUmbrellaLayer ASC, Policy_Issue_Date ASC, Policy_Start_Date ASC, Policy_End_Date ASC, Accounting_Date ASC, Endorsement_Issue_Date ASC, Endorsement_Start_Date ASC, EDWPremiumMasterCalculationPKID ASC
),
EXP_HashKey AS (
	SELECT
	pol_key,
	PremiumTransactionCode,
	-- *INF*: :LKP.LKP_TRANSCODE(PremiumTransactionCode)
	LKP_TRANSCODE_PremiumTransactionCode.TargetAttributeValue AS v_DocumentType,
	Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	ReinsuranceUmbrellaLayer AS ReinsuranceUmbrellalayer,
	-- *INF*: IIF(MonthlyTotalDirectWrittenPremium<0,'0','1')
	IFF(MonthlyTotalDirectWrittenPremium < 0, '0', '1') AS Sign_Flag,
	-- *INF*: MD5(pol_key||'|'||ProductCode||'|'||StrategicProfitCenterAbbreviation||'|'||AccountingProductCode||'|'||InsuranceReferenceLineOfBusinessCode||'|'||asl_code||'|'||sub_asl_code||'|'||state_of_domicile_code||'|'||to_char(ReinsuranceUmbrellalayer))
	MD5(pol_key || '|' || ProductCode || '|' || StrategicProfitCenterAbbreviation || '|' || AccountingProductCode || '|' || InsuranceReferenceLineOfBusinessCode || '|' || asl_code || '|' || sub_asl_code || '|' || state_of_domicile_code || '|' || to_char(ReinsuranceUmbrellalayer)) AS v_CoversKey,
	-- *INF*: md5(to_char(Policy_Start_Date,'YYYYMMDD')||'|'||to_char(Policy_End_Date,'YYYYMMDD'))
	md5(to_char(Policy_Start_Date, 'YYYYMMDD') || '|' || to_char(Policy_End_Date, 'YYYYMMDD')) AS v_CoversDateKey,
	-- *INF*: MD5(TO_CHAR(Endorsement_Start_Date,'YYYYYMMDD')||'|'||TO_CHAR(Endorsement_Issue_Date,'YYYYYMMDD'))
	MD5(TO_CHAR(Endorsement_Start_Date, 'YYYYYMMDD') || '|' || TO_CHAR(Endorsement_Issue_Date, 'YYYYYMMDD')) AS v_DateKey,
	v_CoversKey AS CoversKey,
	v_CoversDateKey AS CoversDateKey,
	v_DateKey AS DateKey,
	-- *INF*: IIF(v_CoversKey=v_PriorCoversKey and v_CoversDateKey=v_PriorCoversDateKey ,IIF(v_DateKey=v_PriorDateKey and Accounting_Date=v_Prior_Accounting_Date,v_Count,v_Count+1),1)
	IFF(
	    v_CoversKey = v_PriorCoversKey and v_CoversDateKey = v_PriorCoversDateKey,
	    IFF(
	        v_DateKey = v_PriorDateKey
	    and Accounting_Date = v_Prior_Accounting_Date, v_Count,
	        v_Count + 1
	    ),
	    1
	) AS v_Count,
	-- *INF*: IIF(isnull(:LKP.LKP_EXISTING_POLICYCOVERS(v_CoversKey)),IIF(v_CoversKey=v_PriorCoversKey AND v_CoversDateKey=v_PriorCoversDateKey,IIF(v_DateKey=v_PriorDateKey and v_Count=1 and Accounting_Date=v_Prior_Accounting_Date,IIF(v_DocumentType=v_Prior_Original_DocType,v_Prior_DocType,v_DocumentType),IIF(in(v_DocumentType,'A','C'),v_DocumentType,'E')),'P'),IIF(in(v_DocumentType,'A','C'),v_DocumentType,'E'))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(in(v_DocumentType,'A','C'),v_DocumentType,'E'),'P'),IIF(in(v_DocumentType,'A','C'),v_DocumentType,'E'))
	-- 
	-- 
	-- --IIF(isnull(LKP_ExistingPolicyIssueDate),IIF(CoversKey=v_PriorCoversKey AND v_CoversDateKey=v_PriorCoversDateKey,'E',v_DocumentType))
	IFF(
	    LKP_EXISTING_POLICYCOVERS_v_CoversKey.ASLCoversKey IS NULL,
	    IFF(
	        v_CoversKey = v_PriorCoversKey
	    and v_CoversDateKey = v_PriorCoversDateKey,
	        IFF(
	            v_DateKey = v_PriorDateKey
	            and v_Count = 1
	            and Accounting_Date = v_Prior_Accounting_Date,
	            IFF(
	                v_DocumentType = v_Prior_Original_DocType, v_Prior_DocType,
	                v_DocumentType
	            ),
	            IFF(
	                v_DocumentType IN ('A','C'), v_DocumentType, 'E'
	            )
	        ),
	        'P'
	    ),
	    IFF(
	        v_DocumentType IN ('A','C'), v_DocumentType, 'E'
	    )
	) AS v_DocType,
	v_DocType AS DocType,
	v_DocumentType AS v_Prior_Original_DocType,
	v_DocType AS v_Prior_DocType,
	-- *INF*: Decode(v_DocType,'P','1','E','3','C','2','A','4')
	Decode(
	    v_DocType,
	    'P', '1',
	    'E', '3',
	    'C', '2',
	    'A', '4'
	) AS DocTypeOrder,
	v_CoversKey AS v_PriorCoversKey,
	v_CoversDateKey AS v_PriorCoversDateKey,
	v_DateKey AS v_PriorDateKey,
	Accounting_Date AS v_Prior_Accounting_Date,
	v_pol_key,
	ReinsuranceUmbrellalayer AS O_ReinsuranceUmbrellalayer,
	RatingPlanAbbreviation,
	name
	FROM SRT_Get_Order_Prior_Current_RecordComparison
	LEFT JOIN LKP_TRANSCODE LKP_TRANSCODE_PremiumTransactionCode
	ON LKP_TRANSCODE_PremiumTransactionCode.SourceAttributeValue = PremiumTransactionCode

	LEFT JOIN LKP_EXISTING_POLICYCOVERS LKP_EXISTING_POLICYCOVERS_v_CoversKey
	ON LKP_EXISTING_POLICYCOVERS_v_CoversKey.ASLCoversKey = v_CoversKey

),
SRT_Original_Transaction_Sequence AS (
	SELECT
	pol_key, 
	MonthlyTotalDirectWrittenPremium, 
	ProductCode, 
	StrategicProfitCenterAbbreviation, 
	AccountingProductCode, 
	InsuranceReferenceLineOfBusinessCode, 
	asl_code, 
	sub_asl_code, 
	state_of_domicile_code, 
	O_ReinsuranceUmbrellalayer AS ReinsuranceUmbrellalayer, 
	Policy_Issue_Date, 
	Policy_Start_Date, 
	Policy_End_Date, 
	Accounting_Date, 
	Endorsement_Issue_Date, 
	Endorsement_Start_Date, 
	DocTypeOrder, 
	Sign_Flag, 
	EDWPremiumMasterCalculationPKID, 
	CoversKey, 
	CoversDateKey, 
	DateKey, 
	DocType, 
	RatingPlanAbbreviation, 
	name
	FROM EXP_HashKey
	ORDER BY pol_key ASC, ProductCode ASC, StrategicProfitCenterAbbreviation ASC, AccountingProductCode ASC, InsuranceReferenceLineOfBusinessCode ASC, asl_code ASC, sub_asl_code ASC, state_of_domicile_code ASC, ReinsuranceUmbrellalayer ASC, Policy_Issue_Date ASC, Policy_Start_Date ASC, Policy_End_Date ASC, Accounting_Date ASC, Endorsement_Issue_Date ASC, Endorsement_Start_Date ASC, DocTypeOrder ASC, Sign_Flag ASC
),
EXP_TGT_Data_Collect AS (
	SELECT
	pol_key,
	DocType,
	-- *INF*: IIF(DocType='A','E',DocType)
	IFF(DocType = 'A', 'E', DocType) AS v_DocType,
	v_DocType AS O_Doc_Type,
	Accounting_Date,
	MonthlyTotalDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	sub_asl_code,
	state_of_domicile_code,
	Policy_Start_Date,
	Policy_End_Date,
	Endorsement_Start_Date,
	Endorsement_Issue_Date,
	Policy_Issue_Date,
	CoversKey,
	CoversDateKey,
	DateKey,
	Sign_Flag,
	-- *INF*: IIF(ISNULL(:LKP.LKP_TGT(CoversKey)),'0|0|',:LKP.LKP_TGT(CoversKey))
	IFF(LKP_TGT_CoversKey.OutPut IS NULL, '0|0|', LKP_TGT_CoversKey.OutPut) AS v_LKP_TgtValue,
	-- *INF*: TO_BIGINT(substr(v_LKP_TgtValue,1,INSTR(v_LKP_TgtValue,'|',1,1)-1))
	CAST(substr(v_LKP_TgtValue, 1, REGEXP_INSTR(v_LKP_TgtValue, '|', 1, 1) - 1) AS BIGINT) AS v_lkp_Endorsement_No,
	-- *INF*: TO_BIGINT(substr(v_LKP_TgtValue,INSTR(v_LKP_TgtValue,'|',1,1)+1,INSTR(v_LKP_TgtValue,'|',1,2)-INSTR(v_LKP_TgtValue,'|',1,1)))
	CAST(substr(v_LKP_TgtValue, REGEXP_INSTR(v_LKP_TgtValue, '|', 1, 1) + 1, REGEXP_INSTR(v_LKP_TgtValue, '|', 1, 2) - REGEXP_INSTR(v_LKP_TgtValue, '|', 1, 1)) AS BIGINT) AS v_lkp_Tran_No,
	-- *INF*: iif(CoversKey=v_Prior_CoversKey and CoversDateKey=v_PriorCoversDateKey and DateKey=v_PriorDateKey and v_DocType=v_PriorDocType and Accounting_Date=v_Priror_Accounting_Date and Sign_Flag=v_PriorSign_Flag,'0','1')
	IFF(
	    CoversKey = v_Prior_CoversKey
	    and CoversDateKey = v_PriorCoversDateKey
	    and DateKey = v_PriorDateKey
	    and v_DocType = v_PriorDocType
	    and Accounting_Date = v_Priror_Accounting_Date
	    and Sign_Flag = v_PriorSign_Flag,
	    '0',
	    '1'
	) AS v_Flag,
	-- *INF*: IIF(ISNULL(:LKP.LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER(1)),0,:LKP.LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER(1))
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, 0,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num
	) AS v_lkp_Source_Seq_Num,
	-- *INF*: IIF(v_Flag='1',v_count+1,v_count)
	IFF(v_Flag = '1', v_count + 1, v_count) AS v_count,
	-- *INF*: IIF(v_Flag='1',v_lkp_Source_Seq_Num+v_count,v_Source_Seq_Num)
	IFF(v_Flag = '1', v_lkp_Source_Seq_Num + v_count, v_Source_Seq_Num) AS v_Source_Seq_Num,
	v_Source_Seq_Num AS Source_Seq_Num,
	-- *INF*: IIF(CoversKey=v_Prior_CoversKey and CoversDateKey=v_PriorCoversDateKey ,IIF(DateKey=v_PriorDateKey and v_DocType=v_PriorDocType and Accounting_Date=v_Priror_Accounting_Date and Sign_Flag=v_PriorSign_Flag,v_Counter,v_Counter+1),1)
	IFF(
	    CoversKey = v_Prior_CoversKey and CoversDateKey = v_PriorCoversDateKey,
	    IFF(
	        DateKey = v_PriorDateKey
	        and v_DocType = v_PriorDocType
	        and Accounting_Date = v_Priror_Accounting_Date
	        and Sign_Flag = v_PriorSign_Flag,
	        v_Counter,
	        v_Counter + 1
	    ),
	    1
	) AS v_Counter,
	-- *INF*: IIF(v_Flag='1',
	-- IIF(v_lkp_Tran_No=0,IIF(CoversKey=v_Prior_CoversKey and CoversDateKey=v_PriorCoversDateKey ,IIF(DateKey=v_PriorDateKey and v_DocType=v_PriorDocType and Accounting_Date=v_Priror_Accounting_Date and Sign_Flag=v_PriorSign_Flag,v_Tran_No,IIF(v_Counter=1,2,v_Tran_No+1)),2),v_lkp_Tran_No+v_Counter),v_Tran_No)
	-- 
	-- 
	-- --IIF(v_Flag='1',IIF(pol_key=v_pol_key,IIF(v_lkp_Tran_No=0,v_Tran_No+1,IIF(pol_key=v_pol_key,v_Tran_No+1,v_lkp_Tran_No+1)),IIF(v_lkp_Tran_No=0,2,IIF(pol_key=v_pol_key,v_Tran_No+1,v_lkp_Tran_No+1))),v_Tran_No)
	-- 
	-- --IIF(pol_key=v_pol_key,IIF(v_Flag='1',))
	-- 
	-- --IIF(v_Flag='1',IIF(pol_key=v_pol_key,IIF(v_lkp_Tran_No=0,v_Tran_No+1,v_lkp_Tran_No+1),IIF(v_lkp_Tran_No=0,2,v_lkp_Tran_No+1)),v_Tran_No)
	-- 
	-- 
	-- --IIF(v_Flag='1',IIF(v_lkp_Tran_No=0,IIF(pol_key=v_pol_key,v_Tran_No+1,2),v_lkp_Tran_No+1),v_Tran_No)
	-- 
	-- 
	IFF(
	    v_Flag = '1',
	    IFF(
	        v_lkp_Tran_No = 0,
	        IFF(
	            CoversKey = v_Prior_CoversKey
	        and CoversDateKey = v_PriorCoversDateKey,
	            IFF(
	                DateKey = v_PriorDateKey
	                and v_DocType = v_PriorDocType
	                and Accounting_Date = v_Priror_Accounting_Date
	                and Sign_Flag = v_PriorSign_Flag,
	                v_Tran_No,
	                IFF(
	                    v_Counter = 1, 2, v_Tran_No + 1
	                )
	            ),
	            2
	        ),
	        v_lkp_Tran_No + v_Counter
	    ),
	    v_Tran_No
	) AS v_Tran_No,
	v_Tran_No AS Tran_No,
	-- *INF*: IIF(v_DocType='E',
	-- IIF(v_Flag='1',IIF(v_lkp_Endorsement_No=0,IIF(CoversKey=v_Prior_CoversKey and CoversDateKey=v_PriorCoversDateKey ,IIF(DateKey=v_PriorDateKey and v_DocType=v_PriorDocType and Accounting_Date=v_Priror_Accounting_Date and Sign_Flag=v_PriorSign_Flag,v_Endorsement_No,v_PreviousEndorsement_No+1),1),v_lkp_Endorsement_No+1),v_Endorsement_No),0)
	-- 
	-- 
	IFF(
	    v_DocType = 'E',
	    IFF(
	        v_Flag = '1',
	        IFF(
	            v_lkp_Endorsement_No = 0,
	            IFF(
	                CoversKey = v_Prior_CoversKey
	            and CoversDateKey = v_PriorCoversDateKey,
	                IFF(
	                    DateKey = v_PriorDateKey
	                    and v_DocType = v_PriorDocType
	                    and Accounting_Date = v_Priror_Accounting_Date
	                    and Sign_Flag = v_PriorSign_Flag,
	                    v_Endorsement_No,
	                    v_PreviousEndorsement_No + 1
	                ),
	                1
	            ),
	            v_lkp_Endorsement_No + 1
	        ),
	        v_Endorsement_No
	    ),
	    0
	) AS v_Endorsement_No,
	-- *INF*: IIF(CoversKey=v_Prior_CoversKey and CoversDateKey=v_PriorCoversDateKey,Greatest(v_Endorsement_No,v_PreviousEndorsement_No),0)
	IFF(
	    CoversKey = v_Prior_CoversKey and CoversDateKey = v_PriorCoversDateKey,
	    Greatest(v_Endorsement_No, v_PreviousEndorsement_No),
	    0
	) AS v_PreviousEndorsement_No,
	v_Endorsement_No AS Endorsement_No,
	-- *INF*: ''
	-- 
	-- --IIF(v_Flag='1',
	-- --IIF(NOT ISNULL(LKP_OutPut) and v_LKP_Document_Type='E',IIF(v_Endorsement_Start_Date<v_LKP_Endorsement_Start_Date,'OSE',''),IIF(CoversKey=v_Prior_CoversKey and CoversDateKey=v_PriorCoversDateKey ,IIF(v_Endorsement_Start_Date<v_Prior_Endorsement_Start_Date,'OSE',''),'')),'')
	-- 
	'' AS OSE_Flag,
	pol_key AS v_pol_key,
	CoversKey AS v_Prior_CoversKey,
	CoversDateKey AS v_PriorCoversDateKey,
	DateKey AS v_PriorDateKey,
	v_DocType AS v_PriorDocType,
	Endorsement_Start_Date AS v_Prior_Endorsement_Start_Date,
	Accounting_Date AS v_Priror_Accounting_Date,
	Sign_Flag AS v_PriorSign_Flag,
	CoversKey AS ASLCoversKey,
	CoversDateKey AS ASLCoversDateKey,
	EDWPremiumMasterCalculationPKID,
	ReinsuranceUmbrellalayer,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	'MONTHLY' AS EntryProcess,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	RatingPlanAbbreviation,
	name
	FROM SRT_Original_Transaction_Sequence
	LEFT JOIN LKP_TGT LKP_TGT_CoversKey
	ON LKP_TGT_CoversKey.ASLCoversKey = CoversKey

	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsurancePolicy AS (
	TRUNCATE TABLE SapiensReinsurancePolicy;
	INSERT INTO SapiensReinsurancePolicy
	(AuditId, CreatedDate, ModifiedDate, PolicyKey, DocumntType, AccountingDate, MonthlyTotalDirectWrittenPremium, ProductCode, StrategicProfitCenterAbbreviation, AccountingProductCode, InsuranceReferenceLineOfBusinessCode, ASLCode, SubASLCode, PrimaryStateCode, CoverageEffectiveDate, CoverageExpirationDate, EndorsementStartDate, EndorsementIssueDate, PolicyIssueDate, SourceSequenceNumber, TransactionNumber, EndorsementNumber, ASLCoversKey, DateKey, PremiumMasterCalculationPKId, ReinsuranceUmbrellaLayer, OSECode, EntryProcess, RatingPlanAbbreviation, FirstNameIsured)
	SELECT 
	Auditid AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	pol_key AS POLICYKEY, 
	O_Doc_Type AS DOCUMNTTYPE, 
	Accounting_Date AS ACCOUNTINGDATE, 
	MONTHLYTOTALDIRECTWRITTENPREMIUM, 
	PRODUCTCODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	ACCOUNTINGPRODUCTCODE, 
	INSURANCEREFERENCELINEOFBUSINESSCODE, 
	asl_code AS ASLCODE, 
	sub_asl_code AS SUBASLCODE, 
	state_of_domicile_code AS PRIMARYSTATECODE, 
	Policy_Start_Date AS COVERAGEEFFECTIVEDATE, 
	Policy_End_Date AS COVERAGEEXPIRATIONDATE, 
	Endorsement_Start_Date AS ENDORSEMENTSTARTDATE, 
	Endorsement_Issue_Date AS ENDORSEMENTISSUEDATE, 
	Policy_Issue_Date AS POLICYISSUEDATE, 
	Source_Seq_Num AS SOURCESEQUENCENUMBER, 
	Tran_No AS TRANSACTIONNUMBER, 
	Endorsement_No AS ENDORSEMENTNUMBER, 
	ASLCOVERSKEY, 
	ASLCoversDateKey AS DATEKEY, 
	EDWPremiumMasterCalculationPKID AS PREMIUMMASTERCALCULATIONPKID, 
	ReinsuranceUmbrellalayer AS REINSURANCEUMBRELLALAYER, 
	OSE_Flag AS OSECODE, 
	ENTRYPROCESS, 
	RATINGPLANABBREVIATION, 
	name AS FIRSTNAMEISURED
	FROM EXP_TGT_Data_Collect
),