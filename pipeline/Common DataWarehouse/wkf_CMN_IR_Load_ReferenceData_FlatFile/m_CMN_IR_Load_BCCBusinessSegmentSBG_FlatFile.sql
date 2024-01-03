WITH
SQ_BCCBusinessSegmentStategicBusinessGroup AS (

-- TODO Manual --

),
Exp_BCCBusinessSegmentSBG AS (
	SELECT
	'InformS' AS ModifiedUserId,
	CURRENT_TIMESTAMP AS ModifiedDate,
	Effective_Date AS in_EffectiveDate,
	-- *INF*: TO_DATE(in_EffectiveDate,'YYYY-MM-DD HH24:MI:SS.MS')
	TO_DATE(in_EffectiveDate, 'YYYY-MM-DD HH24:MI:SS.MS') AS out_EffectiveDate,
	Expiration_Date AS in_ExpirationDate,
	-- *INF*: TO_DATE(in_ExpirationDate,'YYYY-MM-DD HH24:MI:SS.MS')
	TO_DATE(in_ExpirationDate, 'YYYY-MM-DD HH24:MI:SS.MS') AS out_ExpirationDate,
	Business_Classification_Code AS In_BusinessClassificationCode,
	-- *INF*: ltrim(rtrim(In_BusinessClassificationCode))
	ltrim(rtrim(In_BusinessClassificationCode)) AS Out_BusinessClassificationCode,
	Business_Classification_Description AS In_BusinessClassificationDescription,
	-- *INF*: ltrim(rtrim(In_BusinessClassificationDescription))
	ltrim(rtrim(In_BusinessClassificationDescription)) AS out_BusinessClassificationDescription,
	Business_Segment_Code AS In_BusinessSegmentCode,
	-- *INF*: ltrim(rtrim(In_BusinessSegmentCode))
	ltrim(rtrim(In_BusinessSegmentCode)) AS out_BusinessSegmentCode,
	Business_Segment_Description AS In_BusinessSegmentDescription,
	-- *INF*: ltrim(rtrim(In_BusinessSegmentDescription))
	ltrim(rtrim(In_BusinessSegmentDescription)) AS out_BusinessSegmentDescription,
	Strategic_Business_Group_Code AS In_StrategicBusinessGroupCode,
	-- *INF*: ltrim(rtrim(In_StrategicBusinessGroupCode))
	ltrim(rtrim(In_StrategicBusinessGroupCode)) AS out_StrategicBusinessGroupCode,
	Strategic_Business_Group_Description AS In_StrategicBusinessGroupDescription,
	-- *INF*: ltrim(rtrim(In_StrategicBusinessGroupDescription))
	ltrim(rtrim(In_StrategicBusinessGroupDescription)) AS Out_StrategicBusinessGroupDescription,
	Argent_Business_Segment_Code AS In_Argent_Business_Segment_Code,
	-- *INF*: LTRIM(RTRIM(In_Argent_Business_Segment_Code))
	LTRIM(RTRIM(In_Argent_Business_Segment_Code)) AS Out_Argent_Business_Segment_Code,
	Argent_Business_Segment_Description AS In_Argent_Business_Segment_Description,
	-- *INF*: LTRIM(RTRIM(In_Argent_Business_Segment_Description))
	LTRIM(RTRIM(In_Argent_Business_Segment_Description)) AS Out_Argent_Business_Segment_Description
	FROM SQ_BCCBusinessSegmentStategicBusinessGroup
),
BCCBusinessSegmentSBG AS (
	TRUNCATE TABLE BCCBusinessSegmentSBG;
	INSERT INTO BCCBusinessSegmentSBG
	(ModifiedUserId, ModifiedDate, EffectiveDate, ExpirationDate, BusinessClassificationCode, BusinessClassificationDescription, BusinessSegmentCode, BusinessSegmentDescription, StrategicBusinessGroupCode, StrategicBusinessGroupDescription, ArgentBusinessSegmentCode, ArgentBusinessSegmentDescription)
	SELECT 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	out_EffectiveDate AS EFFECTIVEDATE, 
	out_ExpirationDate AS EXPIRATIONDATE, 
	Out_BusinessClassificationCode AS BUSINESSCLASSIFICATIONCODE, 
	out_BusinessClassificationDescription AS BUSINESSCLASSIFICATIONDESCRIPTION, 
	out_BusinessSegmentCode AS BUSINESSSEGMENTCODE, 
	out_BusinessSegmentDescription AS BUSINESSSEGMENTDESCRIPTION, 
	out_StrategicBusinessGroupCode AS STRATEGICBUSINESSGROUPCODE, 
	Out_StrategicBusinessGroupDescription AS STRATEGICBUSINESSGROUPDESCRIPTION, 
	Out_Argent_Business_Segment_Code AS ARGENTBUSINESSSEGMENTCODE, 
	Out_Argent_Business_Segment_Description AS ARGENTBUSINESSSEGMENTDESCRIPTION
	FROM Exp_BCCBusinessSegmentSBG
),