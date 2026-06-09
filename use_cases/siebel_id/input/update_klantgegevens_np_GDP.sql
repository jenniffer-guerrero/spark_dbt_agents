DROP TABLE IF EXISTS #NP_DRC_BNK_HIST
SELECT * 
INTO #NP_DRC_BNK_HIST
FROM [Brondata].[kln].[cdf_ggm_np_drc_bnk_hist_prepared]
WHERE edl_valid_to_dts >= '2025-08-01'


DROP TABLE IF EXISTS #NP_HIST
SELECT * 
INTO #NP_HIST
FROM [Brondata].[kln].[cdf_ggm_np_hist_prepared]
WHERE edl_valid_to_dts >= '2025-08-01'


DROP TABLE IF EXISTS #jaarmaand_tabel
SELECT '6_1' as run_script_version, jaarmaand 
INTO #jaarmaand_tabel
FROM live.tarieven.tarieven_referentie
WHERE jaarmaand < 202604

SELECT COUNT(*),rel_id, edl_valid_to_dts FROM #NP_DRC_BNK_HIST
WHERE edl_valid_to_dts = '9999-12-31 00:00:00.0000000'
GROUP BY rel_id, edl_valid_to_dts
HAVING COUNT(*) > 1

SELECT * FROM #NP_DRC_BNK_HIST WHERE rel_id = '000000009412844'
/********************************************
Creeren tabellen drc
**************************/
DROP TABLE IF EXISTS #NP_drc_bnk
SELECT   jaarmaand
		,LEAD(jaarmaand) OVER (PARTITION BY T0.rel_id, T0.np_sbl_id ORDER BY jaarmaand) as lead_jaarmaand
		,T0.rel_id
		,T0.np_sbl_id
		,T0.drc_bnk_f
INTO #NP_drc_bnk
FROM #NP_DRC_BNK_HIST	AS T0
INNER JOIN (	SELECT   CONCAT(YEAR(edl_valid_from_dts),RIGHT(LEFT(edl_valid_from_dts,7),2)) as jaarmaand
						,rel_id
						,np_sbl_id
						,MAX(edl_valid_from_dts) as edl_valid_from_dts
				FROM  #NP_DRC_BNK_HIST	 a
				GROUP BY CONCAT(YEAR(edl_valid_from_dts),RIGHT(LEFT(edl_valid_from_dts,7),2))
						,rel_id
						,np_sbl_id
			)	AS T1
	ON T0.rel_id				= T1.rel_id
	AND T0.np_sbl_id			= T1.np_sbl_id
	AND T0.edl_valid_from_dts	= T1.edl_valid_from_dts
ORDER BY rel_id, jaarmaand


DROP TABLE IF EXISTS #correctie_drc_bnk
SELECT T0.jaarmaand, T0.lead_jaarmaand, T0.rel_id, T0.np_sbl_id, T1.drc_bnk_f
INTO #correctie_drc_bnk
FROM #NP_drc_bnk			AS T0
LEFT JOIN	(	SELECT DISTINCT rel_id, np_sbl_id, drc_bnk_f 
				FROM #NP_DRC_BNK_HIST 
				WHERE edl_valid_to_dts = '9999-12-31 00:00:00.0000000'
				AND drc_bnk_f = 'Y'
			)				AS T1
	ON T0.rel_id = T1.rel_id
	AND T0.np_sbl_id = T1.np_sbl_id
WHERE lead_jaarmaand IS NULL
AND T0.drc_bnk_f = 'N' AND T1.drc_bnk_f = 'Y'


UPDATE T0

SET drc_bnk_f= 'Y'

FROM #NP_drc_bnk as T0
INNER JOIN #correctie_drc_bnk AS T1
	ON  T0.jaarmaand = T1.jaarmaand
	AND T0.np_sbl_id = T1.np_sbl_id
	AND T0.rel_id = T1.rel_id




DROP TABLE IF EXISTS #NP_drc_jaarmaand
SELECT T0.jaarmaand, T1.rel_id, T1.np_sbl_id
INTO #NP_drc_jaarmaand
FROM #jaarmaand_tabel as T0
LEFT JOIN	(	SELECT	 '6_1' as run_script_version
						,rel_id
						,np_sbl_id
						,MIN(jaarmaand) as eerste_jaarmaand 
				FROM #NP_drc_bnk
				GROUP BY rel_id, np_sbl_id
			)	as T1
	ON T0.run_script_version = T1.run_script_version
WHERE T0.jaarmaand >= eerste_jaarmaand


DROP TABLE IF EXISTS #temp_drc_bnk
SELECT T0.jaarmaand, T0.rel_id, T1.drc_bnk_f, T0.np_sbl_id
INTO #temp_drc_bnk
FROM #NP_drc_jaarmaand	AS T0
LEFT JOIN #NP_drc_bnk		AS T1
	ON T0.rel_id = T1.rel_id
	AND T0.jaarmaand = T1.jaarmaand
ORDER BY T0.rel_id, jaarmaand


DROP TABLE IF EXISTS #temp_lead_drc_bnk
SELECT LEAD(jaarmaand) OVER (PARTITION BY rel_id, np_sbl_id ORDER BY jaarmaand) as lead_jaarmaand
		,* 
INTO #temp_lead_drc_bnk
FROM #temp_drc_bnk
WHERE drc_bnk_f IS NOT NULL
ORDER BY rel_id, jaarmaand


DROP TABLE IF EXISTS #NP_drc_FILL
SELECT	 T0.jaarmaand
		,T0.rel_id
		,T0.np_sbl_id
		,ISNULL(T0.drc_bnk_f, T1.drc_bnk_f) as regie_klant 
INTO  #NP_drc_FILL
FROM #temp_drc_bnk				AS T0
LEFT JOIN #temp_lead_drc_bnk	AS T1
	ON (		T0.rel_id = T1.rel_id
			AND T0.np_sbl_id = T1.np_sbl_id
			AND T0.jaarmaand < T1.lead_jaarmaand
			AND T0.jaarmaand > T1.jaarmaand
		)
	OR	(		T0.rel_id = T1.rel_id
			AND T0.np_sbl_id = T1.np_sbl_id
			AND T0.jaarmaand > T1.jaarmaand
			AND T1.lead_jaarmaand IS NULL
		)
ORDER BY T0.rel_id, T0.jaarmaand

/********************************************
Creeren tabellen NP
**************************/
DROP TABLE IF EXISTS #NP
SELECT jaarmaand, LEAD(jaarmaand) OVER (PARTITION BY T0.rel_id ORDER BY jaarmaand)as lead_jaarmaand, T0.*
INTO #NP
		FROM #NP_HIST	AS T0
		INNER JOIN (	SELECT CONCAT(YEAR(edl_valid_from_dts),RIGHT(LEFT(edl_valid_from_dts,7),2)) as jaarmaand
								,rel_id
								,MAX(edl_valid_from_dts) as edl_valid_from_dts
						FROM  #NP_HIST	 a
						GROUP BY CONCAT(YEAR(edl_valid_from_dts),RIGHT(LEFT(edl_valid_from_dts,7),2))
								,rel_id
					)	AS T1
			ON T0.rel_id = T1.rel_id
			AND T0.edl_valid_from_dts = T1.edl_valid_from_dts		
ORDER BY rel_id, jaarmaand

DROP TABLE IF EXISTS #NP_jaarmaand
SELECT T0.jaarmaand, T1.rel_id
INTO #NP_jaarmaand
FROM #jaarmaand_tabel as T0
LEFT JOIN	(	SELECT	 '6_1' as run_script_version
						,rel_id
						,MIN(jaarmaand) as eerste_jaarmaand 
				FROM #NP
				GROUP BY rel_id
			)	as T1
	ON T0.run_script_version = T1.run_script_version
WHERE T0.jaarmaand >= eerste_jaarmaand


DROP TABLE IF EXISTS #temp
SELECT T0.jaarmaand, T0.rel_id, T1.bnk_code, T1.ikb_no, T1.rel_st_tp_ggm_code
INTO #temp
FROM #NP_jaarmaand	AS T0
LEFT JOIN #NP		AS T1
	ON T0.rel_id = T1.rel_id
	AND T0.jaarmaand = T1.jaarmaand
ORDER BY T0.rel_id, jaarmaand



DROP TABLE IF EXISTS #temp_lead
SELECT LEAD(jaarmaand) OVER (PARTITION BY rel_id ORDER BY jaarmaand) as lead_jaarmaand
		,* 
INTO #temp_lead
FROM #temp
WHERE bnk_code IS NOT NULL
ORDER BY rel_id, jaarmaand


DROP TABLE IF EXISTS #NP_FILL
SELECT	 T0.jaarmaand
		,T0.rel_id
		, ISNULL(T0.bnk_code,T1.bnk_code) as bank_code 
		, ISNULL(T0.ikb_no,T1.ikb_no) as ikb_nummer
		, ISNULL(T0.rel_st_tp_ggm_code, T1.rel_st_tp_ggm_code) as rel_st_tp_ggm_code
INTO #NP_FILL
FROM #temp				AS T0
LEFT JOIN #temp_lead	AS T1
	ON (		T0.rel_id = T1.rel_id
			AND T0.jaarmaand < T1.lead_jaarmaand
			AND T0.jaarmaand > T1.jaarmaand
		)
	OR	(		T0.rel_id = T1.rel_id
			AND T0.jaarmaand > T1.jaarmaand
			AND T1.lead_jaarmaand IS NULL
		)
ORDER BY T0.rel_id, T0.jaarmaand


----Creeren van klantkoppeling tabel

DROP TABLE IF EXISTS #NP_drc_compleet
SELECT T0.jaarmaand, T0.rel_id, T0.ikb_nummer, ISNULL(T1.regie_klant,'N') as regie_klant, T0.rel_st_tp_ggm_code
INTO #NP_drc_compleet
FROM #NP_FILL AS T0
LEFT JOIN #NP_drc_FILL AS T1
	ON T0.rel_id = T1.rel_id
	AND T0.ikb_nummer = T1.np_sbl_id
	AND T0.jaarmaand = T1.jaarmaand
ORDER BY T0.rel_id, T0.jaarmaand



DROP TABLE IF EXISTS #Klant_overzicht
SELECT * 
INTO #Klant_overzicht
FROM	(		SELECT *
					,LAG(regie_klant) OVER (PARTITION BY rel_id, ikb_nummer ORDER BY jaarmaand) as lag_regie_klant 
					,LAG(ikb_nummer) OVER (PARTITION BY rel_id, ikb_nummer ORDER BY jaarmaand) as lag_ikb_nummer
					,LAG(rel_st_tp_ggm_code) OVER (PARTITION BY rel_id, ikb_nummer ORDER BY jaarmaand) as lag_rel_st_tp_ggm_code
			FROM #NP_drc_compleet
		)	AS T0
WHERE (regie_klant <> lag_regie_klant OR lag_regie_klant IS NULL)
OR (ikb_nummer <> lag_ikb_nummer OR lag_ikb_nummer IS NULL)
OR (rel_st_tp_ggm_code <> lag_rel_st_tp_ggm_code OR lag_rel_st_tp_ggm_code IS NULL)
ORDER BY rel_id, jaarmaand

DELETE FROM [Dev].[Brondata].[klantkoppeling].[klantgegevens_np]
WHERE jaarmaand_klant_status_ikb > 202508

INSERT INTO [Dev].[Brondata].[klantkoppeling].[klantgegevens_np]

SELECT   T0.rel_id as klant_nummer
		,ikb_nummer 
		,regie_klant
		,T0.rel_st_tp_ggm_code as klant_status
		,jaarmaand as jaarmaand_klant_status_ikb
		,T1.bnk_code as bank_code
		,T1.bnk_code as bank_code_wwft
		,ISNULL(T2.[cst_wwft_pd_f],'N') as wwft_kenmerk
		,NULL as kenmerk_bijzonder_situatie
		,NULL as beschrijving_kenmerk_bijzonder_situatie
		,NULL as kenmerk_legal_restrictie
		,NULL as beschrijving_kenmerk_legal_restrictie
		,T1.rel_st_tp_ggm_dsc as klant_type
		,T1.dscd_f as ind_overleden
		,NULL AS klant_integriteit_status
		,NULL AS acceptatie_klant_integriteit_status
		,'202603' as jaarmaand_wwft
FROM #Klant_overzicht	AS T0
LEFT JOIN	(	SELECT * FROM [Brondata].[kln].[cdf_ggm_np_hist_prepared]
				WHERE edl_valid_to_dts = '9999-12-31 00:00:00.0000000'
				AND rel_id IN (SELECT DISTINCT rel_id FROM #Klant_overzicht WHERE jaarmaand > 202508)
			)			AS T1
	ON T0.rel_id = T1.rel_id
LEFT JOIN [Brondata].[kln].[wwft_np_ads_prepared] AS T2
	ON T0.rel_id = T2.rel_id
WHERE jaarmaand > 202508
AND ikb_nummer IS NOT NULL
ORDER BY jaarmaand_klant_status_ikb



--T1.dscd_f 