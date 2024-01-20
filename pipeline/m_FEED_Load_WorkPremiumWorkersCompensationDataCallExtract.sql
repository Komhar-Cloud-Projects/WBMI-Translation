WITH
SQ_EarnedPremiumTransactionMonthlyFact AS (
	SELECT EarnedPremiumTransactionMonthlyFact.EarnedPremiumTransactionMonthlyFactID, 
	EarnedPremiumTransactionMonthlyFact.MonthlyChangeinDirectEarnedPremium, 
	EarnedPremiumTransactionMonthlyFact.EDWEarnedPremiumMonthlyCalculationPKID, 
	calendar_dim_PremiumTransactionRunDate.CalendarEndOfMonthDate, 
	calendar_dim_PremiumTransactionBookedDate.clndr_date,
	CoverageDetailWorkersCompensationDim.CoverageDetailDimId, 
	policy_dim.edw_pol_ak_id, 
	policy_dim.pol_key, 
	policy_dim.pol_eff_date, 
	policy_dim.pol_exp_date, 
	LTRIM(RTRIM(CoverageDetailWorkersCompensationDim.NcciClassCode)) AS NcciClassCode, 
	RiskLocationDim.StateProvinceCode, 
	RiskLocationDim.StateProvinceCodeAbbreviation, 
	InsuranceReferenceDim.InsuranceSegmentDescription, 
	InsuranceReferenceDim.PolicyOfferingDescription, 
	PremiumTransactionTypeDim.PremiumTypeCode, 
	CoverageDetailDim.BaseRate, 
	CoverageDetailWorkersCompensationDim.ConsentToRateFlag, 
	CoverageDetailWorkersCompensationDim.RateOverride, 
	InsuranceReferenceDim.StrategicProfitCenterAbbreviation,
	InsuranceReferenceCoverageDim.DctCoverageTypeCode
	FROM EarnedPremiumTransactionMonthlyFact, 
	CoverageDetailDim, 
	CoverageDetailWorkersCompensationDim, 
	calendar_dim calendar_dim_PremiumTransactionRunDate, 
	calendar_dim calendar_dim_PremiumTransactionBookedDate, 
	policy_dim, 
	RiskLocationDim, 
	InsuranceReferenceDim, 
	PremiumTransactionTypeDim,
	InsuranceReferenceCoverageDim
	WHERE ((@{pipeline().parameters.RUN_YEAR} != 0
	AND calendar_dim_PremiumTransactionRunDate.clndr_yr <= @{pipeline().parameters.RUN_YEAR}
	AND CONVERT(CHAR(4), policy_dim.pol_eff_date, 120) <= @{pipeline().parameters.RUN_YEAR} 
	AND CONVERT(CHAR(4), policy_dim.pol_eff_date, 120) > @{pipeline().parameters.RUN_YEAR} - 5)
	OR
	(@{pipeline().parameters.RUN_YEAR} = 0 
	AND calendar_dim_PremiumTransactionRunDate.clndr_yr <=CONVERT(CHAR(4), GETDATE(), 120) - 1
	AND CONVERT(CHAR(4), policy_dim.pol_eff_date, 120) <= CONVERT(CHAR(4), GETDATE(), 120) - 1
	AND CONVERT(CHAR(4), policy_dim.pol_eff_date, 120) >CONVERT(CHAR(4), GETDATE(), 120) - 6
	))
	AND PremiumTransactionTypeDim.PremiumTypeCode = 'D'
	AND CoverageDetailWorkersCompensationDim.NcciClassCode NOT IN ('9740','9741','9752')
	AND EarnedPremiumTransactionMonthlyFact.CoverageDetailDimId = CoverageDetailDim.CoverageDetailDimId
	AND EarnedPremiumTransactionMonthlyFact.CoverageDetailDimId = CoverageDetailWorkersCompensationDim.CoverageDetailDimId
	AND EarnedPremiumTransactionMonthlyFact.PolicyDimID = policy_dim.pol_dim_id
	AND EarnedPremiumTransactionMonthlyFact.RiskLocationDimID = RiskLocationDim.RiskLocationDimID
	AND EarnedPremiumTransactionMonthlyFact.PremiumTransactionTypeDimID = PremiumTransactionTypeDim.PremiumTransactionTypeDimID
	AND EarnedPremiumTransactionMonthlyFact.InsuranceReferenceDimId = InsuranceReferenceDim.InsuranceReferenceDimId
	AND EarnedPremiumTransactionMonthlyFact.PremiumTransactionBookedDateID = calendar_dim_PremiumTransactionBookedDate.clndr_id
	AND EarnedPremiumTransactionMonthlyFact.PremiumTransactionRunDateID = calendar_dim_PremiumTransactionRunDate.clndr_id
	AND EarnedPremiumTransactionMonthlyFact.InsuranceReferenceCoverageDimId =   InsuranceReferenceCoverageDim.InsuranceReferenceCoverageDimId
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_SupWorkersCompensationPremiumAdjustmentFactor AS (
	SELECT
	EffectiveDate,
	WorkersCompensationPremiumAdjustmentFactor,
	WorkersCompensationPremiumAdjustmentType,
	StateCode,
	ExpirationDate
	FROM (
		SELECT 
			EffectiveDate,
			WorkersCompensationPremiumAdjustmentFactor,
			WorkersCompensationPremiumAdjustmentType,
			StateCode,
			ExpirationDate
		FROM SupWorkersCompensationPremiumAdjustmentFactor
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StateCode,EffectiveDate,ExpirationDate ORDER BY EffectiveDate) = 1
),
EXP_STAGE AS (
	SELECT
	SQ_EarnedPremiumTransactionMonthlyFact.EarnedPremiumTransactionMonthlyFactID,
	SQ_EarnedPremiumTransactionMonthlyFact.MonthlyChangeinDirectEarnedPremium,
	SQ_EarnedPremiumTransactionMonthlyFact.EDWEarnedPremiumMonthlyCalculationPKID,
	SQ_EarnedPremiumTransactionMonthlyFact.CalendarEndOfMonthDate AS EarnedPremiumTransactionRundate_CalendarEndOfMonthDate,
	SQ_EarnedPremiumTransactionMonthlyFact.clndr_date AS EarnedPremiumTransactionBookeddate_clndr_date,
	SQ_EarnedPremiumTransactionMonthlyFact.CoverageDetailDimId,
	SQ_EarnedPremiumTransactionMonthlyFact.edw_pol_ak_id,
	SQ_EarnedPremiumTransactionMonthlyFact.pol_key,
	SQ_EarnedPremiumTransactionMonthlyFact.pol_eff_date,
	SQ_EarnedPremiumTransactionMonthlyFact.pol_exp_date,
	SQ_EarnedPremiumTransactionMonthlyFact.NcciClassCode,
	SQ_EarnedPremiumTransactionMonthlyFact.StateProvinceCode,
	SQ_EarnedPremiumTransactionMonthlyFact.StateProvinceCodeAbbreviation,
	SQ_EarnedPremiumTransactionMonthlyFact.InsuranceSegmentDescription,
	SQ_EarnedPremiumTransactionMonthlyFact.PolicyOfferingDescription,
	SQ_EarnedPremiumTransactionMonthlyFact.PremiumTypeCode,
	SQ_EarnedPremiumTransactionMonthlyFact.BaseRate,
	LKP_SupWorkersCompensationPremiumAdjustmentFactor.EffectiveDate AS i_WorkersCompensationPremiumAdjustmentFactorEffectiveDate,
	-- *INF*: IIF(ISNULL(i_WorkersCompensationPremiumAdjustmentFactorEffectiveDate) AND IN(StateProvinceCodeAbbreviation, 'IA', 'KS', 'MI', 'MN', 'MO','NE'),ERROR('WorkersCompensationPremiumAdjustmentFactor not found for state=' || StateProvinceCodeAbbreviation || ' and policy_eff_date=' || TO_CHAR(pol_eff_date) ), i_WorkersCompensationPremiumAdjustmentFactorEffectiveDate)
	IFF(
	    i_WorkersCompensationPremiumAdjustmentFactorEffectiveDate IS NULL
	    and StateProvinceCodeAbbreviation IN ('IA','KS','MI','MN','MO','NE'),
	    ERROR('WorkersCompensationPremiumAdjustmentFactor not found for state=' || StateProvinceCodeAbbreviation || '
	    and policy_eff_date=' || TO_CHAR(pol_eff_date)),
	    i_WorkersCompensationPremiumAdjustmentFactorEffectiveDate
	) AS o_WorkersCompensationPremiumAdjustmentFactorEffectiveDate,
	LKP_SupWorkersCompensationPremiumAdjustmentFactor.WorkersCompensationPremiumAdjustmentFactor,
	LKP_SupWorkersCompensationPremiumAdjustmentFactor.WorkersCompensationPremiumAdjustmentType,
	-- *INF*: DECODE(TRUE,
	-- IN(StateProvinceCodeAbbreviation,'ND','OH','WA','WY'),'N/A',
	-- IN(StateProvinceCodeAbbreviation, 
	-- 'AL','AK','AR',
	-- 'CA','CO','CT',
	-- 'DC','DE',
	-- 'GA',
	-- 'HI',
	-- 'KS','KY',
	-- 'LA',
	-- 'MD','ME','MI','MN','MO','MS','MT',
	-- 'NC','NE','NH','NM','NV','NY',
	-- 'OK','OR',
	-- 'PA',
	-- 'RI',
	-- 'SC','SD',
	-- 'TN','TX',
	-- 'UT',
	-- 'VA','VT',
	-- 'WV'
	-- ),  'LossCostState',
	-- IN(StateProvinceCodeAbbreviation, 'AZ','FL','IA','ID','IL','IN', 'MA', 'NJ', 'WI'),  'RatingState',
	-- 'N/A')
	-- 
	-- 
	DECODE(
	    TRUE,
	    StateProvinceCodeAbbreviation IN ('ND','OH','WA','WY'), 'N/A',
	    StateProvinceCodeAbbreviation IN ('AL','AK','AR','CA','CO','CT','DC','DE','GA','HI','KS','KY','LA','MD','ME','MI','MN','MO','MS','MT','NC','NE','NH','NM','NV','NY','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WV'), 'LossCostState',
	    StateProvinceCodeAbbreviation IN ('AZ','FL','IA','ID','IL','IN','MA','NJ','WI'), 'RatingState',
	    'N/A'
	) AS v_StateType,
	SQ_EarnedPremiumTransactionMonthlyFact.ConsentToRateFlag AS i_ConsentToRateFlag,
	-- *INF*: DECODE(i_ConsentToRateFlag,'T','1','F','0')
	DECODE(
	    i_ConsentToRateFlag,
	    'T', '1',
	    'F', '0'
	) AS o_ConsentToRateFlag,
	SQ_EarnedPremiumTransactionMonthlyFact.RateOverride,
	SQ_EarnedPremiumTransactionMonthlyFact.StrategicProfitCenterAbbreviation,
	SQ_EarnedPremiumTransactionMonthlyFact.DctCoverageTypeCode,
	-- *INF*: DECODE(TRUE,
	-- StateProvinceCodeAbbreviation='IA' and pol_eff_date>=TO_DATE('20100101','YYYYMMDD'),0.95,
	-- StateProvinceCodeAbbreviation='IA' and pol_eff_date>=TO_DATE('20010501','YYYYMMDD'),0.9,
	-- StateProvinceCodeAbbreviation='IA' and pol_eff_date<TO_DATE('20010501','YYYYMMDD'),0.85,
	-- 1
	-- )
	-- ---INC0020661 - APPLY NEWLY APPLICABLE RATE OF 0.95 EFFECTIVE 1/1/2010 FOR DSR
	DECODE(
	    TRUE,
	    StateProvinceCodeAbbreviation = 'IA' and pol_eff_date >= TO_TIMESTAMP('20100101', 'YYYYMMDD'), 0.95,
	    StateProvinceCodeAbbreviation = 'IA' and pol_eff_date >= TO_TIMESTAMP('20010501', 'YYYYMMDD'), 0.9,
	    StateProvinceCodeAbbreviation = 'IA' and pol_eff_date < TO_TIMESTAMP('20010501', 'YYYYMMDD'), 0.85,
	    1
	) AS v_Deviation,
	v_StateType AS o_StateType,
	-- *INF*: DECODE(TRUE,
	-- v_StateType = 'RatingState' AND IN(NcciClassCode, '9658', '9659'), 0.00,
	-- v_StateType = 'RatingState' AND IN(NcciClassCode,
	-- '9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679',
	-- '9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788',
	-- '9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878',
	-- '9900', '9901','9902','9903', '9904', '9905', '9906','9907','9908','9909',
	-- '9910','9911','9912','9913','9914','9915', '9916', '9917', '9918', '9919', 
	-- '9920','9924','9925','9926','9927','9928','9929',
	-- '9930','9931', '9932','9933','9934','9935','9936','9937','9938','9939',
	-- '9940', '9941', '9942', '9943', '9944', '9945', '9946','9947','9948','9949',
	-- '9950','9951','9952','9953','9954','9955','9956','9957','9958','9959',
	-- '9960','9961','9962','9963','9964','9965','9966','9967','9968','9969',
	-- '9970','9971','9972','9973','9974','9975','9976','9978','9979',
	-- '9980','9982','9983','9986','9987','9988'
	-- ), 0.00,
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0063','0064'),0.00,		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9887','9889','9750','9751'),0.00,		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0931'),0.00,		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9721','9723','9722','9724','9655','9656'),0.00,		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0174'),0.00,   --included for PMS policies	
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9890'),0.00,		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9604'),0.00,		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9757','9776'),0.00,		
	-- v_StateType='RatingState' AND DctCoverageTypeCode='RetrospectiveCalculation',0.00,
	-- v_StateType='LossCostState',0,			
	-- MonthlyChangeinDirectEarnedPremium)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9658','9659'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679','9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788','9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878','9900','9901','9902','9903','9904','9905','9906','9907','9908','9909','9910','9911','9912','9913','9914','9915','9916','9917','9918','9919','9920','9924','9925','9926','9927','9928','9929','9930','9931','9932','9933','9934','9935','9936','9937','9938','9939','9940','9941','9942','9943','9944','9945','9946','9947','9948','9949','9950','9951','9952','9953','9954','9955','9956','9957','9958','9959','9960','9961','9962','9963','9964','9965','9966','9967','9968','9969','9970','9971','9972','9973','9974','9975','9976','9978','9979','9980','9982','9983','9986','9987','9988'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0063','0064'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9887','9889','9750','9751'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0931'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9721','9723','9722','9724','9655','9656'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0174'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9890'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9604'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9757','9776'), 0.00,
	    v_StateType = 'RatingState' AND DctCoverageTypeCode = 'RetrospectiveCalculation', 0.00,
	    v_StateType = 'LossCostState', 0,
	    MonthlyChangeinDirectEarnedPremium
	) AS o_RatingCompanyLevel,
	-- *INF*: DECODE(TRUE,
	-- v_StateType = 'RatingState' AND IN(NcciClassCode, '9658', '9659'), 0.00,
	-- v_StateType = 'RatingState' AND IN(NcciClassCode,
	-- '9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679',
	-- '9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788',
	-- '9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878',
	-- '9900', '9901','9902','9903', '9904', '9905', '9906','9907','9908','9909',
	-- '9910','9911','9912','9913','9914','9915', '9916', '9917', '9918', '9919', 
	-- '9920','9924','9925','9926','9927','9928','9929',
	-- '9930','9931', '9932','9933','9934','9935','9936','9937','9938','9939',
	-- '9940', '9941', '9942', '9943', '9944', '9945', '9946','9947','9948','9949',
	-- '9950','9951','9952','9953','9954','9955','9956','9957','9958','9959',
	-- '9960','9961','9962','9963','9964','9965','9966','9967','9968','9969',
	-- '9970','9971','9972','9973','9974','9975','9976','9978','9979',
	-- '9980','9982','9983','9986','9987','9988'
	-- ), 0.00,
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0063','0064'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9887','9889','9750','9751','0887'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0931'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9721','9723','9722','9724','9655','9656'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0174'),0.00,   --included for PMS policies		
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0021','0022','0023','0032','0043'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9890'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9604'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9757','9776'),0.00,			
	-- v_StateType='RatingState' AND IN(NcciClassCode,'9034','9036','9037','9039'),0.00,
	-- v_StateType='RatingState' AND DctCoverageTypeCode='RetrospectiveCalculation',0.00,			
	-- v_StateType='LossCostState',0,			
	-- MonthlyChangeinDirectEarnedPremium/v_Deviation)
	DECODE(
	    TRUE,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9658','9659'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679','9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788','9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878','9900','9901','9902','9903','9904','9905','9906','9907','9908','9909','9910','9911','9912','9913','9914','9915','9916','9917','9918','9919','9920','9924','9925','9926','9927','9928','9929','9930','9931','9932','9933','9934','9935','9936','9937','9938','9939','9940','9941','9942','9943','9944','9945','9946','9947','9948','9949','9950','9951','9952','9953','9954','9955','9956','9957','9958','9959','9960','9961','9962','9963','9964','9965','9966','9967','9968','9969','9970','9971','9972','9973','9974','9975','9976','9978','9979','9980','9982','9983','9986','9987','9988'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0063','0064'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9887','9889','9750','9751','0887'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0931'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9721','9723','9722','9724','9655','9656'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0174'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0021','0022','0023','0032','0043'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9890'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9604'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9757','9776'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('9034','9036','9037','9039'), 0.00,
	    v_StateType = 'RatingState' AND DctCoverageTypeCode = 'RetrospectiveCalculation', 0.00,
	    v_StateType = 'LossCostState', 0,
	    MonthlyChangeinDirectEarnedPremium / v_Deviation
	) AS o_RatingDSRLevel,
	-- *INF*: DECODE(TRUE,
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9658','9659'),0.00,
	-- v_StateType = 'LossCostState' AND IN(NcciClassCode,
	-- '9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679',
	-- '9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788',
	-- '9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878',
	-- '9900', '9901','9902','9903', '9904', '9905', '9906','9907','9908','9909',
	-- '9910','9911','9912','9913','9914','9915', '9916', '9917', '9918', '9919', 
	-- '9920','9924','9925','9926','9927','9928','9929',
	-- '9930','9931', '9932','9933','9934','9935','9936','9937','9938','9939',
	-- '9940', '9941', '9942', '9943', '9944', '9945', '9946','9947','9948','9949',
	-- '9950','9951','9952','9953','9954','9955','9956','9957','9958','9959',
	-- '9960','9961','9962','9963','9964','9965','9966','9967','9968','9969',
	-- '9970','9971','9972','9973','9974','9975','9976','9978','9979',
	-- '9980','9982','9983','9986','9987','9988'
	-- ), 0.00,
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0063','0064'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9887','9889','9750','9751','0887'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0931'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9721','9723','9722','9724','9655','9656'),0.00,						
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0174'),0.00,   --included for PMS policies		
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0277','0278'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9890','9880'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9604'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9757','9776'),0.00,						
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9611','9612','9613'),0.00,			
	-- v_StateType='LossCostState' AND DctCoverageTypeCode='RetrospectiveCalculation',0.00,			
	-- v_StateType='RatingState',0,
	-- MonthlyChangeinDirectEarnedPremium)
	DECODE(
	    TRUE,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9658','9659'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679','9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788','9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878','9900','9901','9902','9903','9904','9905','9906','9907','9908','9909','9910','9911','9912','9913','9914','9915','9916','9917','9918','9919','9920','9924','9925','9926','9927','9928','9929','9930','9931','9932','9933','9934','9935','9936','9937','9938','9939','9940','9941','9942','9943','9944','9945','9946','9947','9948','9949','9950','9951','9952','9953','9954','9955','9956','9957','9958','9959','9960','9961','9962','9963','9964','9965','9966','9967','9968','9969','9970','9971','9972','9973','9974','9975','9976','9978','9979','9980','9982','9983','9986','9987','9988'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0063','0064'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9887','9889','9750','9751','0887'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0931'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9721','9723','9722','9724','9655','9656'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0174'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0277','0278'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9890','9880'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9604'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9757','9776'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9611','9612','9613'), 0.00,
	    v_StateType = 'LossCostState' AND DctCoverageTypeCode = 'RetrospectiveCalculation', 0.00,
	    v_StateType = 'RatingState', 0,
	    MonthlyChangeinDirectEarnedPremium
	) AS o_LossCostCompanyLevel,
	-- *INF*: DECODE(TRUE,
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0900','0090','9114','9120'),0.00,	
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9658','9659'),0.00,	
	-- v_StateType = 'LossCostState' AND IN(NcciClassCode,
	-- '9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679',
	-- '9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788',
	-- '9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878',
	-- '9900', '9901','9902','9903', '9904', '9905', '9906','9907','9908','9909',
	-- '9910','9911','9912','9913','9914','9915', '9916', '9917', '9918', '9919', 
	-- '9920','9924','9925','9926','9927','9928','9929',
	-- '9930','9931', '9932','9933','9934','9935','9936','9937','9938','9939',
	-- '9940', '9941', '9942', '9943', '9944', '9945', '9946','9947','9948','9949',
	-- '9950','9951','9952','9953','9954','9955','9956','9957','9958','9959',
	-- '9960','9961','9962','9963','9964','9965','9966','9967','9968','9969',
	-- '9970','9971','9972','9973','9974','9975','9976','9978','9979',
	-- '9980','9982','9983','9986','9987','9988'
	-- ), 0.00,
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0063','0064'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9887','9889','9750','9751'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0931'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9721','9723','9722','9724','9655','9656'),0.00,							
	-- v_StateType='RatingState' AND IN(NcciClassCode,'0174'),0.00,   --included for PMS policies			
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0277','0278'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0990','9125','9615','9848','9849'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'0021','0022','0023','0032','0043','9132'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9890','9880'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9604'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9757','9776'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9611','9612','9613'),0.00,							
	-- v_StateType='LossCostState' AND IN(NcciClassCode,'9034','9035','9036','9037','9039'),0.00,		
	-- v_StateType='LossCostState' AND DctCoverageTypeCode='RetrospectiveCalculation',0.00,						
	-- v_StateType='RatingState',0,
	-- MonthlyChangeinDirectEarnedPremium)
	DECODE(
	    TRUE,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0900','0090','9114','9120'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9658','9659'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9657','9663','9664','9667','9668','9669','9670','9671','9672','9673','9674','9675','9676','9677','9678','9679','9758','9759','9760','9761','9762','9763','9764','9770','9771','9772','9773','9780','9781','9784','9785','9786','9787','9788','9801','9853','9854','9855','9856','9857','9858','9859','9860','9861','9862','9863','9864','9865','9866','9867','9868','9869','9878','9900','9901','9902','9903','9904','9905','9906','9907','9908','9909','9910','9911','9912','9913','9914','9915','9916','9917','9918','9919','9920','9924','9925','9926','9927','9928','9929','9930','9931','9932','9933','9934','9935','9936','9937','9938','9939','9940','9941','9942','9943','9944','9945','9946','9947','9948','9949','9950','9951','9952','9953','9954','9955','9956','9957','9958','9959','9960','9961','9962','9963','9964','9965','9966','9967','9968','9969','9970','9971','9972','9973','9974','9975','9976','9978','9979','9980','9982','9983','9986','9987','9988'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0063','0064'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9887','9889','9750','9751'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0931'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9721','9723','9722','9724','9655','9656'), 0.00,
	    v_StateType = 'RatingState' AND NcciClassCode IN ('0174'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0277','0278'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0990','9125','9615','9848','9849'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('0021','0022','0023','0032','0043','9132'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9890','9880'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9604'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9757','9776'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9611','9612','9613'), 0.00,
	    v_StateType = 'LossCostState' AND NcciClassCode IN ('9034','9035','9036','9037','9039'), 0.00,
	    v_StateType = 'LossCostState' AND DctCoverageTypeCode = 'RetrospectiveCalculation', 0.00,
	    v_StateType = 'RatingState', 0,
	    MonthlyChangeinDirectEarnedPremium
	) AS o_LossCostDSRLevel,
	-- *INF*: IIF(@{pipeline().parameters.RUN_YEAR} = 0, 
	--        TO_DATE('12/31/'  || TO_CHAR(GET_DATE_PART(SYSDATE, 'YYYY') -1), 'MM/DD/YYYY'), 
	--        TO_DATE('12/31/'  || TO_CHAR(@{pipeline().parameters.RUN_YEAR}), 'MM/DD/YYYY')
	--       )
	IFF(
	    @{pipeline().parameters.RUN_YEAR} = 0,
	    TO_TIMESTAMP('12/31/' || TO_CHAR(DATE_PART(CURRENT_TIMESTAMP, 'YYYY') - 1), 'MM/DD/YYYY'),
	    TO_TIMESTAMP('12/31/' || TO_CHAR(@{pipeline().parameters.RUN_YEAR}), 'MM/DD/YYYY')
	) AS o_RunDateYear,
	sysdate AS o_CreateDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_EarnedPremiumTransactionMonthlyFact
	LEFT JOIN LKP_SupWorkersCompensationPremiumAdjustmentFactor
	ON LKP_SupWorkersCompensationPremiumAdjustmentFactor.StateCode = SQ_EarnedPremiumTransactionMonthlyFact.StateProvinceCode AND LKP_SupWorkersCompensationPremiumAdjustmentFactor.EffectiveDate <= SQ_EarnedPremiumTransactionMonthlyFact.pol_eff_date AND LKP_SupWorkersCompensationPremiumAdjustmentFactor.ExpirationDate >= SQ_EarnedPremiumTransactionMonthlyFact.pol_eff_date
),
LKP_WorkPremiumWorkersCompensationDataCallExtract AS (
	SELECT
	WorkPremiumWorkersCompensationDataCallExtractId,
	EDWEarnedPremiumMonthlyCalculationPKID,
	RunDate
	FROM (
		SELECT 
			WorkPremiumWorkersCompensationDataCallExtractId,
			EDWEarnedPremiumMonthlyCalculationPKID,
			RunDate
		FROM WorkPremiumWorkersCompensationDataCallExtract
		WHERE RunDate = CASE WHEN @{pipeline().parameters.RUN_YEAR} =0  then convert(datetime,'12/31/' + cast(convert(char(4), getdate(), 120)-1 as char(4)) ,101)
		                                   ELSE convert(datetime,'12/31/' + cast(@{pipeline().parameters.RUN_YEAR} as char(4)),101)
		                        END
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWEarnedPremiumMonthlyCalculationPKID,RunDate ORDER BY WorkPremiumWorkersCompensationDataCallExtractId) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_WorkPremiumWorkersCompensationDataCallExtract.WorkPremiumWorkersCompensationDataCallExtractId,
	EXP_STAGE.EarnedPremiumTransactionMonthlyFactID,
	EXP_STAGE.MonthlyChangeinDirectEarnedPremium,
	EXP_STAGE.EDWEarnedPremiumMonthlyCalculationPKID,
	EXP_STAGE.EarnedPremiumTransactionRundate_CalendarEndOfMonthDate,
	EXP_STAGE.EarnedPremiumTransactionBookeddate_clndr_date,
	EXP_STAGE.CoverageDetailDimId,
	EXP_STAGE.edw_pol_ak_id,
	EXP_STAGE.pol_key,
	EXP_STAGE.pol_eff_date,
	EXP_STAGE.pol_exp_date,
	EXP_STAGE.NcciClassCode,
	EXP_STAGE.StateProvinceCode,
	EXP_STAGE.StateProvinceCodeAbbreviation,
	EXP_STAGE.InsuranceSegmentDescription,
	EXP_STAGE.PolicyOfferingDescription,
	EXP_STAGE.PremiumTypeCode,
	EXP_STAGE.BaseRate,
	EXP_STAGE.o_WorkersCompensationPremiumAdjustmentFactorEffectiveDate AS WorkersCompensationPremiumAdjustmentFactorEffectiveDate,
	EXP_STAGE.WorkersCompensationPremiumAdjustmentFactor,
	EXP_STAGE.WorkersCompensationPremiumAdjustmentType,
	EXP_STAGE.o_ConsentToRateFlag AS ConsentToRateFlag,
	EXP_STAGE.RateOverride,
	EXP_STAGE.StrategicProfitCenterAbbreviation,
	EXP_STAGE.o_StateType AS StateType,
	EXP_STAGE.o_RatingCompanyLevel AS CompanyRatingLevel,
	EXP_STAGE.o_RatingDSRLevel AS RatingDSRLevel,
	EXP_STAGE.o_LossCostCompanyLevel AS LossCostCompanyLevel,
	EXP_STAGE.o_LossCostDSRLevel AS LossCostDSRLevel,
	EXP_STAGE.o_RunDateYear AS RunDateYear,
	EXP_STAGE.o_CreateDate AS CreateDate,
	EXP_STAGE.o_AuditId AS AuditId
	FROM EXP_STAGE
	LEFT JOIN LKP_WorkPremiumWorkersCompensationDataCallExtract
	ON LKP_WorkPremiumWorkersCompensationDataCallExtract.EDWEarnedPremiumMonthlyCalculationPKID = EXP_STAGE.EDWEarnedPremiumMonthlyCalculationPKID AND LKP_WorkPremiumWorkersCompensationDataCallExtract.RunDate = EXP_STAGE.o_RunDateYear
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE ISNULL(WorkPremiumWorkersCompensationDataCallExtractId)),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE NOT ISNULL(WorkPremiumWorkersCompensationDataCallExtractId)),
UPD_UPDATE AS (
	SELECT
	WorkPremiumWorkersCompensationDataCallExtractId AS WorkPremiumWorkersCompensationDataCallExtractId3, 
	EarnedPremiumTransactionMonthlyFactID AS EarnedPremiumTransactionMonthlyFactID3, 
	MonthlyChangeinDirectEarnedPremium AS MonthlyChangeinDirectEarnedPremium3, 
	EDWEarnedPremiumMonthlyCalculationPKID AS EDWEarnedPremiumMonthlyCalculationPKID3, 
	EarnedPremiumTransactionRundate_CalendarEndOfMonthDate AS EarnedPremiumTransactionRundate_CalendarEndOfMonthDate3, 
	EarnedPremiumTransactionBookeddate_clndr_date AS EarnedPremiumTransactionBookeddate_clndr_date3, 
	CoverageDetailDimId AS CoverageDetailDimId3, 
	edw_pol_ak_id AS edw_pol_ak_id3, 
	pol_key AS pol_key3, 
	pol_eff_date AS pol_eff_date3, 
	pol_exp_date AS pol_exp_date3, 
	NcciClassCode AS NcciClassCode3, 
	StateProvinceCode AS StateProvinceCode3, 
	StateProvinceCodeAbbreviation AS StateProvinceCodeAbbreviation3, 
	InsuranceSegmentDescription AS InsuranceSegmentDescription3, 
	PolicyOfferingDescription AS PolicyOfferingDescription3, 
	PremiumTypeCode AS PremiumTypeCode3, 
	BaseRate AS BaseRate3, 
	WorkersCompensationPremiumAdjustmentFactorEffectiveDate AS WorkersCompensationPremiumAdjustmentFactorEffectiveDate3, 
	WorkersCompensationPremiumAdjustmentFactor AS WorkersCompensationPremiumAdjustmentFactor3, 
	WorkersCompensationPremiumAdjustmentType AS WorkersCompensationPremiumAdjustmentType3, 
	ConsentToRateFlag AS ConsentToRateFlag3, 
	RateOverride AS RateOverride3, 
	StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation3, 
	StateType AS StateType3, 
	CompanyRatingLevel AS CompanyRatingLevel3, 
	RatingDSRLevel AS RatingDSRLevel3, 
	LossCostCompanyLevel AS LossCostCompanyLevel3, 
	LossCostDSRLevel AS LossCostDSRLevel3, 
	RunDateYear AS RunDateYear3, 
	CreateDate AS CreateDate3, 
	AuditId AS AuditId3
	FROM RTR_INSERT_UPDATE_UPDATE
),
WorkPremiumWorkersCompensationDataCallExtract_UPDATE AS (
	MERGE INTO WorkPremiumWorkersCompensationDataCallExtract AS T
	USING UPD_UPDATE AS S
	ON T.WorkPremiumWorkersCompensationDataCallExtractId = S.WorkPremiumWorkersCompensationDataCallExtractId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId3, T.CreatedDate = S.CreateDate3, T.EDWEarnedPremiumMonthlyCalculationPKID = S.EDWEarnedPremiumMonthlyCalculationPKID3, T.RunDate = S.RunDateYear3, T.EarnedPremiumRunDate = S.EarnedPremiumTransactionRundate_CalendarEndOfMonthDate3, T.PolicyKey = S.pol_key3, T.PolicyEffectiveDate = S.pol_eff_date3, T.StateCode = S.StateProvinceCode3, T.NCCIClassCode = S.NcciClassCode3, T.StrategicProfitCenterAbbreviation = S.StrategicProfitCenterAbbreviation3, T.InsuranceSegmentDescription = S.InsuranceSegmentDescription3, T.PolicyOfferingDescription = S.PolicyOfferingDescription3, T.WorkersCompensationPremiumAdjustmentFactorEffectiveDate = S.WorkersCompensationPremiumAdjustmentFactorEffectiveDate3, T.WorkersCompensationPremiumAdjustmentFactor = S.WorkersCompensationPremiumAdjustmentFactor3, T.WorkersCompensationPremiumAdjustmentType = S.WorkersCompensationPremiumAdjustmentType3, T.ConsentToRateFlag = S.ConsentToRateFlag3, T.RateOverride = S.RateOverride3, T.BaseRate = S.BaseRate3, T.RatingStateType = S.StateType3, T.DirectEarnedPremium = S.MonthlyChangeinDirectEarnedPremium3, T.RatingCompanyLevelEarnedPremium = S.CompanyRatingLevel3, T.RatingDSRLevelEarnedPremium = S.RatingDSRLevel3, T.LossCostCompanyLevelEarnedPremium = S.LossCostCompanyLevel3, T.LossCostDSRLevelEarnedPremium = S.LossCostDSRLevel3
),
UPD_INSERT AS (
	SELECT
	WorkPremiumWorkersCompensationDataCallExtractId AS WorkPremiumWorkersCompensationDataCallExtractId1, 
	EarnedPremiumTransactionMonthlyFactID AS EarnedPremiumTransactionMonthlyFactID1, 
	MonthlyChangeinDirectEarnedPremium AS MonthlyChangeinDirectEarnedPremium1, 
	EDWEarnedPremiumMonthlyCalculationPKID AS EDWEarnedPremiumMonthlyCalculationPKID1, 
	EarnedPremiumTransactionRundate_CalendarEndOfMonthDate AS EarnedPremiumTransactionRundate_CalendarEndOfMonthDate1, 
	EarnedPremiumTransactionBookeddate_clndr_date AS EarnedPremiumTransactionBookeddate_clndr_date1, 
	CoverageDetailDimId AS CoverageDetailDimId1, 
	edw_pol_ak_id AS edw_pol_ak_id1, 
	pol_key AS pol_key1, 
	pol_eff_date AS pol_eff_date1, 
	pol_exp_date AS pol_exp_date1, 
	NcciClassCode AS NcciClassCode1, 
	StateProvinceCode AS StateProvinceCode1, 
	StateProvinceCodeAbbreviation AS StateProvinceCodeAbbreviation1, 
	InsuranceSegmentDescription AS InsuranceSegmentDescription1, 
	PolicyOfferingDescription AS PolicyOfferingDescription1, 
	PremiumTypeCode AS PremiumTypeCode1, 
	BaseRate AS BaseRate1, 
	WorkersCompensationPremiumAdjustmentFactorEffectiveDate AS WorkersCompensationPremiumAdjustmentFactorEffectiveDate1, 
	WorkersCompensationPremiumAdjustmentFactor AS WorkersCompensationPremiumAdjustmentFactor1, 
	WorkersCompensationPremiumAdjustmentType AS WorkersCompensationPremiumAdjustmentType1, 
	ConsentToRateFlag AS ConsentToRateFlag1, 
	RateOverride AS RateOverride1, 
	StrategicProfitCenterAbbreviation AS StrategicProfitCenterAbbreviation1, 
	StateType AS StateType1, 
	CompanyRatingLevel AS CompanyRatingLevel1, 
	RatingDSRLevel AS RatingDSRLevel1, 
	LossCostCompanyLevel AS LossCostCompanyLevel1, 
	LossCostDSRLevel AS LossCostDSRLevel1, 
	RunDateYear AS RunDateYear1, 
	CreateDate AS CreateDate1, 
	AuditId AS AuditId1
	FROM RTR_INSERT_UPDATE_INSERT
),
WorkPremiumWorkersCompensationDataCallExtract_INSERT AS (
	INSERT INTO WorkPremiumWorkersCompensationDataCallExtract
	(AuditId, CreatedDate, EDWEarnedPremiumMonthlyCalculationPKID, RunDate, EarnedPremiumRunDate, PolicyKey, PolicyEffectiveDate, StateCode, NCCIClassCode, StrategicProfitCenterAbbreviation, InsuranceSegmentDescription, PolicyOfferingDescription, WorkersCompensationPremiumAdjustmentFactorEffectiveDate, WorkersCompensationPremiumAdjustmentFactor, WorkersCompensationPremiumAdjustmentType, ConsentToRateFlag, RateOverride, BaseRate, RatingStateType, DirectEarnedPremium, RatingCompanyLevelEarnedPremium, RatingDSRLevelEarnedPremium, LossCostCompanyLevelEarnedPremium, LossCostDSRLevelEarnedPremium)
	SELECT 
	AuditId1 AS AUDITID, 
	CreateDate1 AS CREATEDDATE, 
	EDWEarnedPremiumMonthlyCalculationPKID1 AS EDWEARNEDPREMIUMMONTHLYCALCULATIONPKID, 
	RunDateYear1 AS RUNDATE, 
	EarnedPremiumTransactionRundate_CalendarEndOfMonthDate1 AS EARNEDPREMIUMRUNDATE, 
	pol_key1 AS POLICYKEY, 
	pol_eff_date1 AS POLICYEFFECTIVEDATE, 
	StateProvinceCode1 AS STATECODE, 
	NcciClassCode1 AS NCCICLASSCODE, 
	StrategicProfitCenterAbbreviation1 AS STRATEGICPROFITCENTERABBREVIATION, 
	InsuranceSegmentDescription1 AS INSURANCESEGMENTDESCRIPTION, 
	PolicyOfferingDescription1 AS POLICYOFFERINGDESCRIPTION, 
	WorkersCompensationPremiumAdjustmentFactorEffectiveDate1 AS WORKERSCOMPENSATIONPREMIUMADJUSTMENTFACTOREFFECTIVEDATE, 
	WorkersCompensationPremiumAdjustmentFactor1 AS WORKERSCOMPENSATIONPREMIUMADJUSTMENTFACTOR, 
	WorkersCompensationPremiumAdjustmentType1 AS WORKERSCOMPENSATIONPREMIUMADJUSTMENTTYPE, 
	ConsentToRateFlag1 AS CONSENTTORATEFLAG, 
	RateOverride1 AS RATEOVERRIDE, 
	BaseRate1 AS BASERATE, 
	StateType1 AS RATINGSTATETYPE, 
	MonthlyChangeinDirectEarnedPremium1 AS DIRECTEARNEDPREMIUM, 
	CompanyRatingLevel1 AS RATINGCOMPANYLEVELEARNEDPREMIUM, 
	RatingDSRLevel1 AS RATINGDSRLEVELEARNEDPREMIUM, 
	LossCostCompanyLevel1 AS LOSSCOSTCOMPANYLEVELEARNEDPREMIUM, 
	LossCostDSRLevel1 AS LOSSCOSTDSRLEVELEARNEDPREMIUM
	FROM UPD_INSERT
),