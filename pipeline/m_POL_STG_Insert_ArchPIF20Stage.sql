WITH
SQ_Pif20Stage AS (
	SELECT
		Pif20StageId,
		ExtractDate,
		SourceSystemId,
		AuditId,
		HomeOwnersPifSymbol,
		HomeOwnersPifPolicyNumber,
		HomeOwnersPifModule,
		HomeOwnersPremiumId,
		HomeOwnersPremiumSeq,
		HomeOwnersFiller1,
		HomeOwnersBasePremium,
		HomeOwnersForm1,
		HomeOwnersSeg1,
		HomeOwnersPremium1,
		HomeOwnersForm2,
		HomeOwnersSeg2,
		HomeOwnersPremium2,
		HomeOwnersForm3,
		HomeOwnersSeg3,
		HomeOwnersPremium3,
		HomeOwnersForm4,
		HomeOwnersSeg4,
		HomeOwnersPremium4,
		HomeOwnersForm5,
		HomeOwnersSeg5,
		HomeOwnersPremium5,
		HomeOwnersForm6,
		HomeOwnersSeg6,
		HomeOwnersPremium6,
		HomeOwnersForm7,
		HomeOwnersSeg7,
		HomeOwnersPremium7,
		HomeOwnersForm8,
		HomeOwnersSeg8,
		HomeOwnersPremium8,
		HomeOwnersForm9,
		HomeOwnersSeg9,
		HomeOwnersPremium9,
		HomeOwnersForm10,
		HomeOwnersSeg10,
		HomeOwnersPremium10,
		HomeOwnersForm11,
		HomeOwnersSeg11,
		HomeOwnersPremium11,
		HomeOwnersForm12,
		HomeOwnersSeg12,
		HomeOwnersPremium12,
		HomeOwnersForm13,
		HomeOwnersSeg13,
		HomeOwnersPremium13,
		HomeOwnersForm14,
		HomeOwnersSeg14,
		HomeOwnersPremium14,
		HomeOwnersForm15,
		HomeOwnersSeg15,
		HomeOwnersPremium15,
		HomeOwnersForm16,
		HomeOwnersSeg16,
		HomeOwnersPremium16,
		HomeOwnersForm17,
		HomeOwnersSeg17,
		HomeOwnersPremium17,
		HomeOwnersForm18,
		HomeOwnersSeg18,
		HomeOwnersPremium18,
		HomeOwnersForm19,
		HomeOwnersSeg19,
		HomeOwnersPremium19,
		HomeOwnersForm20,
		HomeOwnersSeg20,
		HomeOwnersPremium20,
		HomeOwnersForm21,
		HomeOwnersSeg21,
		HomeOwnersPremium21,
		HomeOwnersForm22,
		HomeOwnersSeg22,
		HomeOwnersPremium22,
		HomeOwnersForm23,
		HomeOwnersSeg23,
		HomeOwnersPremium23,
		HomeOwnersPMSFutureUse,
		HomeOwnersCUSTFutureUse,
		HomeOwnersSeq1,
		HomeOwnersPrem1,
		HomeOwnersSeq2,
		HomeOwnersPrem2,
		HomeOwnersSeq3,
		HomeOwnersPrem3,
		HomeOwnersMNTax1,
		HomeOwnersMNTax2,
		HomeOwnersMNTax3,
		HomeOwnersYr2000CUSTUSE
	FROM Pif20Stage
),
EXP_Values AS (
	SELECT
	Pif20StageId,
	ExtractDate,
	SourceSystemId,
	AuditId,
	HomeOwnersPifSymbol,
	HomeOwnersPifPolicyNumber,
	HomeOwnersPifModule,
	HomeOwnersPremiumId,
	HomeOwnersPremiumSeq,
	HomeOwnersFiller1,
	HomeOwnersBasePremium,
	HomeOwnersForm1,
	HomeOwnersSeg1,
	HomeOwnersPremium1,
	HomeOwnersForm2,
	HomeOwnersSeg2,
	HomeOwnersPremium2,
	HomeOwnersForm3,
	HomeOwnersSeg3,
	HomeOwnersPremium3,
	HomeOwnersForm4,
	HomeOwnersSeg4,
	HomeOwnersPremium4,
	HomeOwnersForm5,
	HomeOwnersSeg5,
	HomeOwnersPremium5,
	HomeOwnersForm6,
	HomeOwnersSeg6,
	HomeOwnersPremium6,
	HomeOwnersForm7,
	HomeOwnersSeg7,
	HomeOwnersPremium7,
	HomeOwnersForm8,
	HomeOwnersSeg8,
	HomeOwnersPremium8,
	HomeOwnersForm9,
	HomeOwnersSeg9,
	HomeOwnersPremium9,
	HomeOwnersForm10,
	HomeOwnersSeg10,
	HomeOwnersPremium10,
	HomeOwnersForm11,
	HomeOwnersSeg11,
	HomeOwnersPremium11,
	HomeOwnersForm12,
	HomeOwnersSeg12,
	HomeOwnersPremium12,
	HomeOwnersForm13,
	HomeOwnersSeg13,
	HomeOwnersPremium13,
	HomeOwnersForm14,
	HomeOwnersSeg14,
	HomeOwnersPremium14,
	HomeOwnersForm15,
	HomeOwnersSeg15,
	HomeOwnersPremium15,
	HomeOwnersForm16,
	HomeOwnersSeg16,
	HomeOwnersPremium16,
	HomeOwnersForm17,
	HomeOwnersSeg17,
	HomeOwnersPremium17,
	HomeOwnersForm18,
	HomeOwnersSeg18,
	HomeOwnersPremium18,
	HomeOwnersForm19,
	HomeOwnersSeg19,
	HomeOwnersPremium19,
	HomeOwnersForm20,
	HomeOwnersSeg20,
	HomeOwnersPremium20,
	HomeOwnersForm21,
	HomeOwnersSeg21,
	HomeOwnersPremium21,
	HomeOwnersForm22,
	HomeOwnersSeg22,
	HomeOwnersPremium22,
	HomeOwnersForm23,
	HomeOwnersSeg23,
	HomeOwnersPremium23,
	HomeOwnersPMSFutureUse,
	HomeOwnersCUSTFutureUse,
	HomeOwnersSeq1,
	HomeOwnersPrem1,
	HomeOwnersSeq2,
	HomeOwnersPrem2,
	HomeOwnersSeq3,
	HomeOwnersPrem3,
	HomeOwnersMNTax1,
	HomeOwnersMNTax2,
	HomeOwnersMNTax3,
	HomeOwnersYr2000CUSTUSE
	FROM SQ_Pif20Stage
),
ArchPif20Stage AS (
	INSERT INTO ArchPif20Stage
	(Pif20HapStageId, ExtractDate, SourceSystemId, AuditId, HomeOwnersPifSymbol, HomeOwnersPifPolicyNumber, HomeOwnersPifModule, HomeOwnersPremiumId, HomeOwnersPremiumSeq, HomeOwnersFiller1, HomeOwnersBasePremium, HomeOwnersForm1, HomeOwnersSeg1, HomeOwnersPremium1, HomeOwnersForm2, HomeOwnersSeg2, HomeOwnersPremium2, HomeOwnersForm3, HomeOwnersSeg3, HomeOwnersPremium3, HomeOwnersForm4, HomeOwnersSeg4, HomeOwnersPremium4, HomeOwnersForm5, HomeOwnersSeg5, HomeOwnersPremium5, HomeOwnersForm6, HomeOwnersSeg6, HomeOwnersPremium6, HomeOwnersForm7, HomeOwnersSeg7, HomeOwnersPremium7, HomeOwnersForm8, HomeOwnersSeg8, HomeOwnersPremium8, HomeOwnersForm9, HomeOwnersSeg9, HomeOwnersPremium9, HomeOwnersForm10, HomeOwnersSeg10, HomeOwnersPremium10, HomeOwnersForm11, HomeOwnersSeg11, HomeOwnersPremium11, HomeOwnersForm12, HomeOwnersSeg12, HomeOwnersPremium12, HomeOwnersForm13, HomeOwnersSeg13, HomeOwnersPremium13, HomeOwnersForm14, HomeOwnersSeg14, HomeOwnersPremium14, HomeOwnersForm15, HomeOwnersSeg15, HomeOwnersPremium15, HomeOwnersForm16, HomeOwnersSeg16, HomeOwnersPremium16, HomeOwnersForm17, HomeOwnersSeg17, HomeOwnersPremium17, HomeOwnersForm18, HomeOwnersSeg18, HomeOwnersPremium18, HomeOwnersForm19, HomeOwnersSeg19, HomeOwnersPremium19, HomeOwnersForm20, HomeOwnersSeg20, HomeOwnersPremium20, HomeOwnersForm21, HomeOwnersSeg21, HomeOwnersPremium21, HomeOwnersForm22, HomeOwnersSeg22, HomeOwnersPremium22, HomeOwnersForm23, HomeOwnersSeg23, HomeOwnersPremium23, HomeOwnersPMSFutureUse, HomeOwnersCUSTFutureUse, HomeOwnersSeq1, HomeOwnersPrem1, HomeOwnersSeq2, HomeOwnersPrem2, HomeOwnersSeq3, HomeOwnersPrem3, HomeOwnersMNTax1, HomeOwnersMNTax2, HomeOwnersMNTax3, HomeOwnersYr2000CUSTUSE)
	SELECT 
	Pif20StageId AS PIF20HAPSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	HOMEOWNERSPIFSYMBOL, 
	HOMEOWNERSPIFPOLICYNUMBER, 
	HOMEOWNERSPIFMODULE, 
	HOMEOWNERSPREMIUMID, 
	HOMEOWNERSPREMIUMSEQ, 
	HOMEOWNERSFILLER1, 
	HOMEOWNERSBASEPREMIUM, 
	HOMEOWNERSFORM1, 
	HOMEOWNERSSEG1, 
	HOMEOWNERSPREMIUM1, 
	HOMEOWNERSFORM2, 
	HOMEOWNERSSEG2, 
	HOMEOWNERSPREMIUM2, 
	HOMEOWNERSFORM3, 
	HOMEOWNERSSEG3, 
	HOMEOWNERSPREMIUM3, 
	HOMEOWNERSFORM4, 
	HOMEOWNERSSEG4, 
	HOMEOWNERSPREMIUM4, 
	HOMEOWNERSFORM5, 
	HOMEOWNERSSEG5, 
	HOMEOWNERSPREMIUM5, 
	HOMEOWNERSFORM6, 
	HOMEOWNERSSEG6, 
	HOMEOWNERSPREMIUM6, 
	HOMEOWNERSFORM7, 
	HOMEOWNERSSEG7, 
	HOMEOWNERSPREMIUM7, 
	HOMEOWNERSFORM8, 
	HOMEOWNERSSEG8, 
	HOMEOWNERSPREMIUM8, 
	HOMEOWNERSFORM9, 
	HOMEOWNERSSEG9, 
	HOMEOWNERSPREMIUM9, 
	HOMEOWNERSFORM10, 
	HOMEOWNERSSEG10, 
	HOMEOWNERSPREMIUM10, 
	HOMEOWNERSFORM11, 
	HOMEOWNERSSEG11, 
	HOMEOWNERSPREMIUM11, 
	HOMEOWNERSFORM12, 
	HOMEOWNERSSEG12, 
	HOMEOWNERSPREMIUM12, 
	HOMEOWNERSFORM13, 
	HOMEOWNERSSEG13, 
	HOMEOWNERSPREMIUM13, 
	HOMEOWNERSFORM14, 
	HOMEOWNERSSEG14, 
	HOMEOWNERSPREMIUM14, 
	HOMEOWNERSFORM15, 
	HOMEOWNERSSEG15, 
	HOMEOWNERSPREMIUM15, 
	HOMEOWNERSFORM16, 
	HOMEOWNERSSEG16, 
	HOMEOWNERSPREMIUM16, 
	HOMEOWNERSFORM17, 
	HOMEOWNERSSEG17, 
	HOMEOWNERSPREMIUM17, 
	HOMEOWNERSFORM18, 
	HOMEOWNERSSEG18, 
	HOMEOWNERSPREMIUM18, 
	HOMEOWNERSFORM19, 
	HOMEOWNERSSEG19, 
	HOMEOWNERSPREMIUM19, 
	HOMEOWNERSFORM20, 
	HOMEOWNERSSEG20, 
	HOMEOWNERSPREMIUM20, 
	HOMEOWNERSFORM21, 
	HOMEOWNERSSEG21, 
	HOMEOWNERSPREMIUM21, 
	HOMEOWNERSFORM22, 
	HOMEOWNERSSEG22, 
	HOMEOWNERSPREMIUM22, 
	HOMEOWNERSFORM23, 
	HOMEOWNERSSEG23, 
	HOMEOWNERSPREMIUM23, 
	HOMEOWNERSPMSFUTUREUSE, 
	HOMEOWNERSCUSTFUTUREUSE, 
	HOMEOWNERSSEQ1, 
	HOMEOWNERSPREM1, 
	HOMEOWNERSSEQ2, 
	HOMEOWNERSPREM2, 
	HOMEOWNERSSEQ3, 
	HOMEOWNERSPREM3, 
	HOMEOWNERSMNTAX1, 
	HOMEOWNERSMNTAX2, 
	HOMEOWNERSMNTAX3, 
	HOMEOWNERSYR2000CUSTUSE
	FROM EXP_Values
),