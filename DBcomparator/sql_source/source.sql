SELECT HASHBYTES('SHA1', CONCAT('ORG_CASE:', [PSA_CAE_OLTP_ORG_CASE].[id], 'ORG_UM_CASE:', [PSA_CAE_OLTP_ORG_UM_CASE].[case_id],
'ORG_UM_LOS:', [PSA_CAE_OLTP_ORG_UM_LOS].[id], 'ORG_UM_LOS_EXTENSION:', [PSA_CAE_OLTP_ORG_UM_LOS_EXTENSION].[id])) [AUTH_LN_HASH],
[PSA_CAE_OLTP_ORG_UM_CASE].[case_id] [CASE_ID],
[PSA_CAE_OLTP_ORG_CASE].[id], [PSA_CAE_OLTP_ORG_CASE].[TZIX_ID] [AUTH_ID],
[PSA_CAE_OLTP_ORG_UM_LOS].[req_prov_id] [LOS_REQUESTING_PROVIDER],
ISNULL(CONVERT(VARCHAR, PSA_CAE_OLTP_ORG_CASE.CREATE_DATE, 112), 0) [HRD_INPUT_DT]
INTO ##Temp1 FROM [hpXr_Stage].[psa].[PSA_CAE_OLTP_ORG_CASE]
JOIN [hpXr_Stage].[psa].[PSA_CAE_OLTP_ORG_UM_CASE] ON [PSA_CAE_OLTP_ORG_UM_CASE].[case_id] = [PSA_CAE_OLTP_ORG_CASE].[id]	
AND [hpXr_Stage].[psa].[PSA_CAE_OLTP_ORG_UM_CASE].[IS_CURRENT] = 1
JOIN [hpXr_Stage].[psa].[PSA_CAE_OLTP_ORG_UM_LOS] ON [PSA_CAE_OLTP_ORG_CASE].[id] = [PSA_CAE_OLTP_ORG_UM_CASE].[case_id]
AND [hpXr_Stage].[psa].[PSA_CAE_OLTP_ORG_UM_LOS].[IS_CURRENT] = 1
LEFT JOIN [hpXr_Stage].[psa].[PSA_CAE_OLTP_ORG_UM_LOS_EXTENSION] ON [PSA_CAE_OLTP_ORG_UM_LOS_EXTENSION].[LOS_ID] = [PSA_CAE_OLTP_ORG_UM_LOS].[ID]
AND [PSA_CAE_OLTP_ORG_UM_LOS_EXTENSION].[IS_CURRENT] = 1
WHERE [PSA_CAE_OLTP_ORG_CASE].IS_CURRENT = 1