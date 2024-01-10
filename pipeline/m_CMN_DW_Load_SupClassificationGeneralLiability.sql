WITH
LKP_SupClassificationGeneralLiability_CurrentChangeFlag AS (
	SELECT
	SupClassificationGeneralLiabilityId,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	SublineCode,
	ISOGeneralLiabilityClassGroupCode,
	ISOGeneralLiabilityClassSummary,
	RatingBasis,
	EffectiveDate
	FROM (
		SELECT 
			SupClassificationGeneralLiabilityId,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			SublineCode,
			ISOGeneralLiabilityClassGroupCode,
			ISOGeneralLiabilityClassSummary,
			RatingBasis,
			EffectiveDate
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGeneralLiability
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,ClassDescription,OriginatingOrganizationCode,SublineCode,ISOGeneralLiabilityClassGroupCode,ISOGeneralLiabilityClassSummary,RatingBasis,EffectiveDate ORDER BY SupClassificationGeneralLiabilityId DESC) = 1
),
SQ_SupClassificationGeneralLiability AS (
	SELECT  EffectiveDate
		 , ExpirationDate
		 , LTRIM(RTRIM(LineOfBusinessAbbreviation))  as LineOfBusinessAbbreviation
	     , LTRIM(RTRIM(RatingStateCode))  as RatingStateCode
		 , LTRIM(RTRIM(ClassCode ))  as ClassCode
		 , LTRIM(RTRIM(ClassDescription))  as ClassDescription
		 , LTRIM(RTRIM(OriginatingOrganizationCode))  as OriginatingOrganizationCode
		 , LTRIM(RTRIM(SublineCode))  as SublineCode
	
		 , LTRIM(RTRIM(ISOGeneralLiabilityClassGroupCode))  as ISOGeneralLiabilityClassGroupCode	 
	       , LTRIM(RTRIM(ISOGeneralLiabilityClassSummary))  as ISOGeneralLiabilityClassSummary
		 , LTRIM(RTRIM(RatingBasis))  as RatingBasis
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupClassificationGeneralLiability
	--WHERE ClassCode = '10010'
),
LKP_SupClassificationGeneralLiability AS (
	SELECT
	SupClassificationGeneralLiabilityId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	SublineCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	RatingBasis
	FROM (
		SELECT 
			SupClassificationGeneralLiabilityId,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode,
			SublineCode,
			ISOGeneralLiabilityClassSummary,
			ISOGeneralLiabilityClassGroupCode,
			RatingBasis
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGeneralLiability
		WHERE CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,SublineCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationGeneralLiabilityId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_SupClassificationGeneralLiability.SupClassificationGeneralLiabilityId AS lkp_SupClassificationGeneralLiabilityId,
	LKP_SupClassificationGeneralLiability.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationGeneralLiability.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationGeneralLiability.SourceSystemId AS lkp_SourceSystemId,
	LKP_SupClassificationGeneralLiability.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationGeneralLiability.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationGeneralLiability.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationGeneralLiability.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	LKP_SupClassificationGeneralLiability.SublineCode AS lkp_SublineCode,
	LKP_SupClassificationGeneralLiability.ISOGeneralLiabilityClassSummary AS lkp_ISOGeneralLiabilityClassSummary,
	LKP_SupClassificationGeneralLiability.ISOGeneralLiabilityClassGroupCode AS lkp_ISOGeneralLiabilityClassGroupCode,
	LKP_SupClassificationGeneralLiability.RatingBasis AS lkp_RatingBasis,
	SQ_SupClassificationGeneralLiability.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationGeneralLiability.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationGeneralLiability.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationGeneralLiability.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationGeneralLiability.ClassCode AS i_ClassCode,
	SQ_SupClassificationGeneralLiability.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationGeneralLiability.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	SQ_SupClassificationGeneralLiability.SublineCode AS i_SublineCode,
	SQ_SupClassificationGeneralLiability.ISOGeneralLiabilityClassGroupCode AS i_ISOGeneralLiabilityClassGroupCode,
	SQ_SupClassificationGeneralLiability.ISOGeneralLiabilityClassSummary AS i_ISOGeneralLiabilityClassSummary,
	SQ_SupClassificationGeneralLiability.RatingBasis AS i_RatingBasis,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL( :LKP.LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG (i_RatingStateCode,i_ClassCode,i_ClassDescription,i_OriginatingOrganizationCode,i_SublineCode,i_ISOGeneralLiabilityClassGroupCode,i_ISOGeneralLiabilityClassSummary,i_RatingBasis,i_EffectiveDate)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	-- 
	--  
	DECODE(TRUE,
		NOT LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.SupClassificationGeneralLiabilityId IS NULL, 'NOCHANGE',
		'INSERT') AS v_RecordPopulated,
	-- *INF*:  DECODE(TRUE,
	-- --i_ExpirationDate   <=  lkp_EffectiveDate OR 1=1, 'NOCHANGE',
	--  i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationGeneralLiabilityId) 
	-- OR (  
	-- i_RatingStateCode = lkp_RatingStateCode
	-- 	AND i_ClassCode = lkp_ClassCode
	--            AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode
	--           AND  i_SublineCode= lkp_SublineCode
	--             	AND (	i_ClassDescription <> lkp_ClassDescription
	--             OR i_EffectiveDate <> lkp_EffectiveDate
	--                   OR i_ExpirationDate <> lkp_ExpirationDate
	--                  --  --OR i_SourceSystemId <> lkp_SourceSystemId  
	--               OR  i_ClassDescription <> lkp_ClassDescription  
	--               OR  i_ISOGeneralLiabilityClassSummary <> lkp_ISOGeneralLiabilityClassSummary 
	--                OR  i_ISOGeneralLiabilityClassGroupCode <> lkp_ISOGeneralLiabilityClassGroupCode  
	--                  OR  i_RatingBasis <>lkp_RatingBasis  
	--                    )
	-- )
	-- ,'INSERT',
	-- i_RatingStateCode <>lkp_RatingStateCode OR
	-- i_ClassCode <>lkp_ClassCode OR
	-- i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode OR 
	-- i_SublineCode<> lkp_SublineCode
	--  ,'UPDATE', 
	-- 'NOCHANGE'
	-- )
	-- 
	-- 
	-- 
	-- 
	--  
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationGeneralLiabilityId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode AND i_SublineCode = lkp_SublineCode AND ( i_ClassDescription <> lkp_ClassDescription OR i_EffectiveDate <> lkp_EffectiveDate OR i_ExpirationDate <> lkp_ExpirationDate OR i_ClassDescription <> lkp_ClassDescription OR i_ISOGeneralLiabilityClassSummary <> lkp_ISOGeneralLiabilityClassSummary OR i_ISOGeneralLiabilityClassGroupCode <> lkp_ISOGeneralLiabilityClassGroupCode OR i_RatingBasis <> lkp_RatingBasis ) ), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode OR i_SublineCode <> lkp_SublineCode, 'UPDATE',
		'NOCHANGE') AS v_ChangeFlag,
	'Please correct the EffectiveDate in CSV file for ClassCode = '||i_ClassCode||' and RatingStateCode = '|| i_RatingStateCode ||', because EffectiveDate should reflect the real effective date for any change on this ClassCode.' AS v_ErrorMessage,
	-- *INF*: 'PASS'
	-- 
	-- --DECODE(TRUE, 
	-- -- i_RatingStateCode = lkp_RatingStateCode
	-- --AND i_ClassCode = lkp_ClassCode
	-- --AND i_EffectiveDate  = lkp_EffectiveDate
	-- --AND 
	-- --(i_ClassDescription <>lkp_ClassDescription
	-- --OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode
	-- --OR i_SourceSystemId <> lkp_SourceSystemId
	-- --OR i_SublineCode <> lkp_SublineCode
	-- --OR i_ISOGeneralLiabilityClassSummary <> lkp_ISOGeneralLiabilityClassSummary
	-- --OR i_ISOGeneralLiabilityClassGroupCode <> lkp_ISOGeneralLiabilityClassGroupCode
	-- --OR i_RatingBasis <> lkp_RatingBasis
	-- --), 
	-- --ERROR(v_ErrorMessage)
	-- --,'PASS')
	'PASS' AS v_RaiseError,
	v_ChangeFlag AS o_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Auditid,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	-- *INF*: i_EffectiveDate
	-- 
	-- --IIF(v_ChangeFlag='INSERT',
	-- 	--TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	--TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	i_EffectiveDate AS o_EffectiveDate,
	-- *INF*: i_ExpirationDate
	-- 
	-- 
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	i_ExpirationDate AS o_ExpirationDate,
	-- *INF*: @{pipeline().parameters.SOURCE_SYSTEM_ID}
	-- --'N/A'
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	i_RatingStateCode AS o_RatingStateCode,
	i_SublineCode AS o_SublineCode,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_OriginatingOrganizationCode AS o_OriginatingOrganizationCode,
	i_ISOGeneralLiabilityClassGroupCode AS o_ISOGeneralLiabilityClassGroupCode,
	i_ISOGeneralLiabilityClassSummary AS o_ISOGeneralLiabilityClassSummary,
	i_RatingBasis AS o_RatingBasis
	FROM SQ_SupClassificationGeneralLiability
	LEFT JOIN LKP_SupClassificationGeneralLiability
	ON LKP_SupClassificationGeneralLiability.RatingStateCode = SQ_SupClassificationGeneralLiability.RatingStateCode AND LKP_SupClassificationGeneralLiability.SublineCode = SQ_SupClassificationGeneralLiability.SublineCode AND LKP_SupClassificationGeneralLiability.ClassCode = SQ_SupClassificationGeneralLiability.ClassCode AND LKP_SupClassificationGeneralLiability.OriginatingOrganizationCode = SQ_SupClassificationGeneralLiability.OriginatingOrganizationCode
	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.OriginatingOrganizationCode = i_OriginatingOrganizationCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.ISOGeneralLiabilityClassGroupCode = i_ISOGeneralLiabilityClassGroupCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.ISOGeneralLiabilityClassSummary = i_ISOGeneralLiabilityClassSummary
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.RatingBasis = i_RatingBasis
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode_i_SublineCode_i_ISOGeneralLiabilityClassGroupCode_i_ISOGeneralLiabilityClassSummary_i_RatingBasis_i_EffectiveDate.EffectiveDate = i_EffectiveDate

),
RTR_Insert_Update AS (
	SELECT
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_Auditid AS Auditid,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemId AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_OriginatingOrganizationCode AS OriginatingOrganizationCode,
	o_SublineCode AS SublineCode,
	o_ISOGeneralLiabilityClassSummary AS ISOGeneralLiabilityClassSummary,
	o_ISOGeneralLiabilityClassGroupCode AS ISOGeneralLiabilityClassGroupCode,
	o_RatingBasis AS RatingBasis,
	o_ChangeFlag AS ChangeFlag
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_Insert_Update AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='INSERT' OR ChangeFlag='UPDATE'),
SupClassificationGeneralLiability AS (
	INSERT INTO SupClassificationGeneralLiability
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode, SublineCode, ISOGeneralLiabilityClassSummary, ISOGeneralLiabilityClassGroupCode, RatingBasis)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	Auditid AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ORIGINATINGORGANIZATIONCODE, 
	SUBLINECODE, 
	ISOGENERALLIABILITYCLASSSUMMARY, 
	ISOGENERALLIABILITYCLASSGROUPCODE, 
	RATINGBASIS
	FROM RTR_Insert_Update_Insert_Update
),
SQ_SupClassificationGeneralLiability_IL AS (
	SELECT SupClassificationGeneralLiability.SupClassificationGeneralLiabilityId
	     , SupClassificationGeneralLiability.CurrentSnapshotFlag
	
		 , SupClassificationGeneralLiability.EffectiveDate
		 , SupClassificationGeneralLiability.ExpirationDate
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.SourceSystemId))  as SourceSystemId
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.LineOfBusinessAbbreviation))  as LineOfBusinessAbbreviation
	     , LTRIM(RTRIM(SupClassificationGeneralLiability.RatingStateCode))  as RatingStateCode
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.ClassCode ))  as ClassCode
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.ClassDescription))  as ClassDescription
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.OriginatingOrganizationCode))  as OriginatingOrganizationCode
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.SublineCode))  as SublineCode
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.ISOGeneralLiabilityClassSummary))  as ISOGeneralLiabilityClassSummary
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.ISOGeneralLiabilityClassGroupCode))  as ISOGeneralLiabilityClassGroupCode
		 , LTRIM(RTRIM(SupClassificationGeneralLiability.RatingBasis))  as RatingBasis
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGeneralLiability
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationGeneralLiability.SublineCode,
	SupClassificationGeneralLiability.ClassCode  ,
	SupClassificationGeneralLiability.RatingStateCode, 
	SupClassificationGeneralLiability.EffectiveDate DESC,
	SupClassificationGeneralLiability.CreatedDate DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	SupClassificationGeneralLiabilityId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	SublineCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	RatingBasis,
	CurrentSnapshotFlag,
	-- *INF*: DECODE(TRUE,RatingStateCode = v_PREV_ROW_RatingStateCode
	-- 		AND ClassCode = v_PREV_ROW_ClassCode
	--              AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	--              AND SublineCode = v_PREV_ROW_SublineCode
	-- 		AND (	ClassDescription <> v_PREV_ROW_ClassDescription
	-- 			--OR 	EffectiveDate <>  v_PREV_ROW_EffectiveDate   
	-- 			OR  ADD_TO_DATE(ExpirationDate,'SS',+1) <>v_PREV_ROW_EffectiveDate   
	--                    OR ISOGeneralLiabilityClassSummary <> v_PREV_ROW_ISOGeneralLiabilityClassSummary
	--                    OR ISOGeneralLiabilityClassGroupCode <> v_PREV_ROW_ISOGeneralLiabilityClassGroupCode
	--                    OR RatingBasis <>v_PREV_ROW_RatingBasis
	--                    )
	-- 		,'0','1')
	DECODE(TRUE,
		RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode AND SublineCode = v_PREV_ROW_SublineCode AND ( ClassDescription <> v_PREV_ROW_ClassDescription OR ADD_TO_DATE(ExpirationDate, 'SS', + 1) <> v_PREV_ROW_EffectiveDate OR ISOGeneralLiabilityClassSummary <> v_PREV_ROW_ISOGeneralLiabilityClassSummary OR ISOGeneralLiabilityClassGroupCode <> v_PREV_ROW_ISOGeneralLiabilityClassGroupCode OR RatingBasis <> v_PREV_ROW_RatingBasis ), '0',
		'1') AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(   --v_PREV_ROW_EffectiveDate
	-- 	IIF(v_PREV_ROW_EffectiveDate =  TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ) , sysdate ,v_PREV_ROW_EffectiveDate )
	-- ,'SS',-1)
	-- 
	-- --ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1)
	ADD_TO_DATE(IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), sysdate, v_PREV_ROW_EffectiveDate), 'SS', - 1) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS o_ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	SourceSystemId AS v_PREV_ROW_SourceSystemId,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	SublineCode AS v_PREV_ROW_SublineCode,
	ISOGeneralLiabilityClassSummary AS v_PREV_ROW_ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode AS v_PREV_ROW_ISOGeneralLiabilityClassGroupCode,
	RatingBasis AS v_PREV_ROW_RatingBasis,
	sysdate AS ModifiedDate
	FROM SQ_SupClassificationGeneralLiability_IL
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationGeneralLiabilityId AS SupClassificationId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	ModifiedDate, 
	o_ClassExpirationDate AS ExpirationDate
	FROM EXP_Lag_Eff_dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupClassificationGeneralLiability AS (
	SELECT
	SupClassificationId, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	ExpirationDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationGeneralLiability_CheckExpDate AS (
	MERGE INTO SupClassificationGeneralLiability AS T
	USING UPD_SupClassificationGeneralLiability AS S
	ON T.SupClassificationGeneralLiabilityId = S.SupClassificationId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),