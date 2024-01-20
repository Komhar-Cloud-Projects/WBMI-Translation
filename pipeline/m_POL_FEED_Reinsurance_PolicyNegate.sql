WITH
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
LKP_Existing_Policy AS (
	SELECT
	POLICYDATES,
	POLICY_NO,
	AccountingProductCode,
	AnnualStatementLineCode,
	CompanyCode,
	LineOfBusiness,
	StrategicProfitCenter,
	ProductCode,
	RiskState,
	SubASLCode,
	ReinsuranceUmbrellaLayer
	FROM (
		select ltrim(rtrim(A.POLICY_NO)) as POLICY_NO,ltrim(rtrim(AccountingProductCode)) as AccountingProductCode, ltrim(rtrim(AnnualStatementLineCode)) as AnnualStatementLineCode, ltrim(rtrim(CompanyCode)) as CompanyCode, ltrim(rtrim(InsuredName)) as InsuredName, ltrim(rtrim(LineOfBusiness)) as LineOfBusiness, ltrim(rtrim(StrategicProfitCenter)) as StrategicProfitCenter, ltrim(rtrim(ProductCode)) as ProductCode, ltrim(rtrim(RiskState)) as RiskState, ltrim(rtrim(ISNULL(SubASLCode,'0'))) as SubASLCode, RatingPlanAbbrevation as RatingPlanAbbrevation,ltrim(rtrim(ISNULL(ReinsuranceUmbrellaLayer,'0'))) as ReinsuranceUmbrellaLayer ,C.POLICYDATES as POLICYDATES
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract A
		inner join (select SOURCE_SEQ_NUM,ACP AccountingProductCode,ASL AnnualStatementLineCode,COM CompanyCode,INM InsuredName,LOB LineOfBusiness,PCN StrategicProfitCenter,PDT ProductCode,RKS RiskState,SAS SubASLCode,ZRP RatingPlanAbbrevation,SNA ReinsuranceUmbrellaLayer,ZRS EntryProcess from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceAttributesExtract
		pivot
		(
		max(ATTR_VAL)
		for ATTR_CODE in (ACP,ASL,COM,INM,LOB,PCN,PDT,RKS,SAS,ZRP,SNA,ZRS)
		) PV) B
		on A.SOURCE_SEQ_NUM=B.SOURCE_SEQ_NUM
		inner join (select cast(PLS as varchar)+'|'+cast(PLE as varchar)+'|' POLICYDATES,SOURCE_SEQ_NUM 
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceDatesExtract
		pivot
		(
		max(DATE_VALUE)
		for DATE_CODE in (PLE,PLS)
		) DT
		where PLE is not NULL and PLS is not NULL) C
		on A.SOURCE_SEQ_NUM=C.SOURCE_SEQ_NUM
		inner join
		(select POLICY_NO,max(case when DOCUMENT_TYPE='N' then SOURCE_SEQ_NUM else 0 end) max_SOURCE_SEQ_NUM 
		from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract E
		where DATA_SOURCE='SRP'
		group by POLICY_NO) D
		on A.POLICY_NO=D.POLICY_NO
		and A.SOURCE_SEQ_NUM>max_SOURCE_SEQ_NUM
		where DATA_SOURCE='SRP'
		and DOCUMENT_TYPE='P'
		and EntryProcess='POLICY-CLAIMSMADE'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY POLICY_NO,AccountingProductCode,AnnualStatementLineCode,CompanyCode,LineOfBusiness,StrategicProfitCenter,ProductCode,RiskState,SubASLCode,ReinsuranceUmbrellaLayer ORDER BY POLICYDATES DESC) = 1
),
SQ_Shortcut_to_WorkSapiensPolicyRestateClaimsMade AS (
	select 
	ltrim(rtrim(A.POLICY_NO)) as POLICY_NO,
	ltrim(rtrim(AccountingProductCode)) as AccountingProductCode,
	 ltrim(rtrim(AnnualStatementLineCode)) as AnnualStatementLineCode,
	 ltrim(rtrim(CompanyCode)) as CompanyCode,
	 ltrim(rtrim(InsuredName)) as InsuredName,
	 ltrim(rtrim(LineOfBusiness)) as LineOfBusiness,
	 ltrim(rtrim(StrategicProfitCenter)) as StrategicProfitCenter,
	 ltrim(rtrim(ProductCode)) as ProductCode,
	 ltrim(rtrim(RiskState)) as RiskState,
	 ltrim(rtrim(ISNULL(SubASLCode,'0'))) as SubASLCode,
	 RatingPlanAbbrevation as RatingPlanAbbrevation,
	 ltrim(rtrim(ISNULL(ReinsuranceUmbrellaLayer,'0'))) as ReinsuranceUmbrellaLayer ,
	 C.POLICYDATES as POLICYDATES
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract A
	inner join (select SOURCE_SEQ_NUM,ACP AccountingProductCode,ASL AnnualStatementLineCode,COM CompanyCode,INM InsuredName,LOB LineOfBusiness,PCN StrategicProfitCenter,PDT ProductCode,RKS RiskState,SAS SubASLCode,ZRP RatingPlanAbbrevation,SNA ReinsuranceUmbrellaLayer,ZRS EntryProcess from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceAttributesExtract
	pivot
	(
	max(ATTR_VAL)
	for ATTR_CODE in (ACP,ASL,COM,INM,LOB,PCN,PDT,RKS,SAS,ZRP,SNA,ZRS)
	) PV) B
	on A.SOURCE_SEQ_NUM=B.SOURCE_SEQ_NUM
	inner join (select cast(PLS as varchar)+'|'+cast(PLE as varchar)+'|' POLICYDATES,SOURCE_SEQ_NUM 
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceDatesExtract
	pivot
	(
	max(DATE_VALUE)
	for DATE_CODE in (PLE,PLS)
	) DT
	where PLE is not NULL and PLS is not NULL) C
	on A.SOURCE_SEQ_NUM=C.SOURCE_SEQ_NUM
	inner join
	(select POLICY_NO,max(case when DOCUMENT_TYPE='N' then SOURCE_SEQ_NUM else 0 end) max_SOURCE_SEQ_NUM 
	from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract E
	where DATA_SOURCE='SRP'
	group by POLICY_NO) D
	on A.POLICY_NO=D.POLICY_NO
	and A.SOURCE_SEQ_NUM>max_SOURCE_SEQ_NUM
	where DATA_SOURCE='SRP'
	and DOCUMENT_TYPE='P'
	and EntryProcess='POLICY-CLAIMSMADE'
),
exp_Source_Fields AS (
	SELECT
	PolicyKey,
	AccountingProductCode,
	AnnualStatementLineCode,
	CompanyCode,
	InsuredName,
	LineOfBusiness,
	StrategicProfitCenter,
	ProductCode,
	RiskState,
	SubASLCode,
	RatingPlanAbbrevation,
	ReinsuranceUmbrellaLayer,
	PolicyDates,
	CURRENT_TIMESTAMP AS CreatedDate
	FROM SQ_Shortcut_to_WorkSapiensPolicyRestateClaimsMade
),
WorkSapiensPolicyRestateClaimsMade1 AS (
	TRUNCATE TABLE Shortcut_to_WorkSapiensPolicyRestateClaimsMade;
	INSERT INTO Shortcut_to_WorkSapiensPolicyRestateClaimsMade
	(PolicyKey, AccountingProductCode, AnnualStatementLineCode, CompanyCode, InsuredName, LineOfBusiness, StrategicProfitCenter, ProductCode, RiskState, SubASLCode, RatingPlanAbbrevation, ReinsuranceUmbrellaLayer, PolicyDates, CreatedDate)
	SELECT 
	POLICYKEY, 
	ACCOUNTINGPRODUCTCODE, 
	ANNUALSTATEMENTLINECODE, 
	COMPANYCODE, 
	INSUREDNAME, 
	LINEOFBUSINESS, 
	STRATEGICPROFITCENTER, 
	PRODUCTCODE, 
	RISKSTATE, 
	SUBASLCODE, 
	RATINGPLANABBREVATION, 
	REINSURANCEUMBRELLALAYER, 
	POLICYDATES, 
	CREATEDDATE
	FROM exp_Source_Fields
),
SQ_PremiumMasterFact AS (
	Declare @Date1 as datetime,
	                 @Date2 as datetime
	Set @Date1=dateadd(dd,-1,dateadd(MM,-@{pipeline().parameters.NO_OF_MONTHS},dateadd(mm,Datediff(mm,0,getdate()),0)))
	Set @Date2='2001-01-01 00:00:00'
	
	select Pol_key,
	PremiumTransactionCode,PremiumTypeCode,
	Accounting_Date,PersonalUmbrellaLayer2Premium,PersonalUmbrellaLayer1Premium,
	PremiumMasterDirectWrittenPremium,
	ProductCode,
	StrategicProfitCenterAbbreviation,
	AccountingProductCode,
	InsuranceReferenceLineOfBusinessCode,
	asl_code,
	CASE 
		WHEN asl_code in ('440','500') 
			THEN CASE WHEN sub_asl_code = 'N/A' THEN NULL ELSE sub_asl_code END
		WHEN asl_code = '220' then '220'
		ELSE NULL
	END AS sub_asl_code,
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
	Name,
	pol_eff_date
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
		WHEN ASL.asl_code in ('440','500') 
			THEN CASE WHEN ASL.sub_asl_code = 'N/A' THEN NULL ELSE ASL.sub_asl_code END
		WHEN ASL.asl_code = '220' then '220'
		ELSE NULL
	END AS sub_asl_code,
	case when IRD.InsuranceReferenceLineOfBusinessCode in ('590','812') then RL.StateProvinceCode else P.state_of_domicile_code end state_of_domicile_code,
	EFD.clndr_date Endorsement_Start_Date,
	ETD.clndr_date Endorsement_Issue_Date,
	PM.EDWPremiumMasterCalculationPKID,
	ISNULL(CUD.UmbrellaLayer,0) UmbrellaLayer,
	ETD.clndr_date Policy_Issue_Date
	,EFD.clndr_date Policy_Start_Date
	,EXD.clndr_date Policy_End_Date,ISNULL(IRD.RatingPlanAbbreviation,'NULL') RatingPlanAbbreviation,ISNULL(CCD.Name,'NULL') Name,P.pol_eff_date
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
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkSapiensPolicyRestateClaimsMade WT
	on WT.PolicyKey=P.Pol_key
	--where (PM.PremiumMasterDirectWrittenPremium<>0.0 or PM.PremiumMasterCededWrittenPremium<>0.0)
	@{pipeline().parameters.WHERE}
	) A ) A 
	Where A.Accounting_Date between @Date2 and @Date1
	and A.pol_eff_date>=@Date2
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
	-- *INF*: IIF(substr(pol_key,1,3)<>'000' and StrategicProfitCenterAbbreviation='WB-PL' and ProductCode='890','Y','N')
	IFF(
	    substr(pol_key, 1, 3) <> '000'
	    and StrategicProfitCenterAbbreviation = 'WB-PL'
	    and ProductCode = '890',
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
	sub_asl_code,
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
	-- *INF*: IIF(ISNULL(sub_asl_code),'0',sub_asl_code)
	IFF(sub_asl_code IS NULL, '0', sub_asl_code) AS v_sub_asl_code,
	state_of_domicile_code,
	Endorsement_Start_Date,
	-- *INF*: IIF(Endorsement_Start_Date>Policy_End_Date,Policy_End_Date,Endorsement_Start_Date)
	IFF(Endorsement_Start_Date > Policy_End_Date, Policy_End_Date, Endorsement_Start_Date) AS v_Endorsement_Start_Date,
	v_Endorsement_Start_Date AS O_Endorsement_Start_Date,
	Endorsement_Issue_Date,
	-- *INF*: IIF(Endorsement_Issue_Date>Policy_End_Date,Policy_End_Date,Endorsement_Issue_Date)
	IFF(Endorsement_Issue_Date > Policy_End_Date, Policy_End_Date, Endorsement_Issue_Date) AS v_Endorsement_Issue_Date,
	v_Endorsement_Issue_Date AS O_Endorsement_Issue_Date,
	EDWPremiumMasterCalculationPKID,
	UmbrellaLayer,
	Policy_Issue_Date,
	Policy_Start_Date,
	Policy_End_Date,
	RatingPlanAbbreviation,
	name,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SUPREINSURANCEUMBRELLALAYER(StrategicProfitCenterAbbreviation,UmbrellaLayer,v_Endorsement_Start_Date)),0,:LKP.LKP_SUPREINSURANCEUMBRELLALAYER(StrategicProfitCenterAbbreviation,UmbrellaLayer,v_Endorsement_Start_Date))
	IFF(
	    LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_v_Endorsement_Start_Date.ReinsuranceUmbrellaLayer IS NULL,
	    0,
	    LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_v_Endorsement_Start_Date.ReinsuranceUmbrellaLayer
	) AS v_ReinsuranceUmbrellaLayer,
	-- *INF*: :LKP.LKP_EXISTING_POLICY(pol_key,AccountingProductCode,asl_code,'WBMI',InsuranceReferenceLineOfBusinessCode,StrategicProfitCenterAbbreviation,ProductCode,state_of_domicile_code,v_sub_asl_code,to_char(v_ReinsuranceUmbrellaLayer))
	LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.POLICYDATES AS LKP_ASL,
	-- *INF*: to_date(substr(LKP_ASL,1,INSTR(LKP_ASL,'|',1,1)-1),'YYYY/MM/DD')
	TO_TIMESTAMP(substr(LKP_ASL, 1, REGEXP_INSTR(LKP_ASL, '|', 1, 1) - 1), 'YYYY/MM/DD') AS LKP_Policy_Start_Date,
	-- *INF*: to_date(substr(LKP_ASL,INSTR(LKP_ASL,'|',1,1)+1,INSTR(LKP_ASL,'|',1,2)-(INSTR(LKP_ASL,'|',1,1)+1)),'YYYY/MM/DD')
	TO_TIMESTAMP(substr(LKP_ASL, REGEXP_INSTR(LKP_ASL, '|', 1, 1) + 1, REGEXP_INSTR(LKP_ASL, '|', 1, 2) - (REGEXP_INSTR(LKP_ASL, '|', 1, 1) + 1)), 'YYYY/MM/DD') AS LKP_Policy_End_Date,
	-- *INF*: IIF(ISNULL(LKP_ASL),'N',IIF(LKP_Policy_Start_Date<>Policy_Start_Date or LKP_Policy_End_Date<>Policy_End_Date,'Y','N'))
	IFF(
	    LKP_ASL IS NULL, 'N',
	    IFF(
	        LKP_Policy_Start_Date <> Policy_Start_Date
	    or LKP_Policy_End_Date <> Policy_End_Date,
	        'Y',
	        'N'
	    )
	) AS Filter_Flag
	FROM UN_PLines_CLines
	LEFT JOIN LKP_SUPREINSURANCEUMBRELLALAYER LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_v_Endorsement_Start_Date
	ON LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_v_Endorsement_Start_Date.StrategicProfitCenterAbbreviation = StrategicProfitCenterAbbreviation
	AND LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_v_Endorsement_Start_Date.SourceUmbrellaLayerStart = UmbrellaLayer
	AND LKP_SUPREINSURANCEUMBRELLALAYER_StrategicProfitCenterAbbreviation_UmbrellaLayer_v_Endorsement_Start_Date.SourceUmbrellaLayerEnd = v_Endorsement_Start_Date

	LEFT JOIN LKP_EXISTING_POLICY LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer
	ON LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.POLICY_NO = pol_key
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.AccountingProductCode = AccountingProductCode
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.AnnualStatementLineCode = asl_code
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.CompanyCode = 'WBMI'
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.LineOfBusiness = InsuranceReferenceLineOfBusinessCode
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.StrategicProfitCenter = StrategicProfitCenterAbbreviation
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.ProductCode = ProductCode
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.RiskState = state_of_domicile_code
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.SubASLCode = v_sub_asl_code
	AND LKP_EXISTING_POLICY_pol_key_AccountingProductCode_asl_code_WBMI_InsuranceReferenceLineOfBusinessCode_StrategicProfitCenterAbbreviation_ProductCode_state_of_domicile_code_v_sub_asl_code_to_char_v_ReinsuranceUmbrellaLayer.ReinsuranceUmbrellaLayer = to_char(v_ReinsuranceUmbrellaLayer)

),
FIL_CorrectedPolicyInfo AS (
	SELECT
	pol_key, 
	Filter_Flag
	FROM EXP_Get_Umbrella
	WHERE Filter_Flag='Y'
),
EXP_Tgt AS (
	SELECT
	pol_key,
	'Y' AS NegateFlag,
	'ClaimsMade' AS UserName,
	CURRENT_TIMESTAMP AS DateTime
	FROM FIL_CorrectedPolicyInfo
),
AGG_EliminatePolicyDuplicates AS (
	SELECT
	pol_key,
	NegateFlag,
	UserName,
	DateTime
	FROM EXP_Tgt
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY NULL) = 1
),
SapiensPolicyRestateClaimsMade AS (
	INSERT INTO SapiensPolicyRestateClaimsMade
	(Policy, NegateFlag, UserName, DateTime)
	SELECT 
	pol_key AS POLICY, 
	NEGATEFLAG, 
	USERNAME, 
	DATETIME
	FROM AGG_EliminatePolicyDuplicates
),