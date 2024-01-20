WITH
SQ_WB_PolicyUnderwritingAdditionalInterest AS (
	SELECT WB_PolicyUnderwritingAdditionalInterest.PartyId, WB_PolicyUnderwritingAdditionalInterest.WB_PolicyUnderwritingAdditionalInterestId, WB_PolicyUnderwritingAdditionalInterest.SessionId, WB_PolicyUnderwritingAdditionalInterest.Interest, WB_PolicyUnderwritingAdditionalInterest.ReferenceLoanNumber, WB_PolicyUnderwritingAdditionalInterest.Country, WB_PolicyUnderwritingAdditionalInterest.GovernmentEntity, WB_PolicyUnderwritingAdditionalInterest.NoticesReinstatements 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_PolicyUnderwritingAdditionalInterest
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	WB_PolicyUnderwritingAdditionalInterest.SessionId=DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}'
	and 
	DC_Session.CreateDateTime <  '@{pipeline().parameters.SELECTION_END_TS}'
	ORDER BY
	WB_PolicyUnderwritingAdditionalInterest.SessionId
),
Exp_WB_PolicyUnderwritingAdditionalInterest AS (
	SELECT
	PartyId,
	WB_PolicyUnderwritingAdditionalInterestId,
	SessionId,
	Interest,
	ReferenceLoanNumber,
	Country,
	GovernmentEntity,
	NoticesReinstatements,
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_WB_PolicyUnderwritingAdditionalInterest
),
WBPolicyUnderwritingAdditionalInterestStage AS (
	INSERT INTO Shortcut_to_WBPolicyUnderwritingAdditionalInterestStage
	(ExtractDate, SourceSystemid, PartyId, WB_PolicyUnderwritingAdditionalInterestId, SessionId, Interest, ReferenceLoanNumber, Country, GovernmentEntity, NoticesReinstatements)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	PARTYID, 
	WB_POLICYUNDERWRITINGADDITIONALINTERESTID, 
	SESSIONID, 
	INTEREST, 
	REFERENCELOANNUMBER, 
	COUNTRY, 
	GOVERNMENTENTITY, 
	NOTICESREINSTATEMENTS
	FROM Exp_WB_PolicyUnderwritingAdditionalInterest
),