{{ config(
  schema='projeto_dengue_izabela',
  database='meicansoft-prd',
  unique_key='SG_UF',
  tags=['staging', 'projeto_dengue']
) }}

SELECT mun.ID_MUNICIPIO, 
      mun.NM_MUNICIPIO, 
      mun.NM_UF, 
      casos.ID_AGRAVO, 
      casos.CS_SEXO, 
      casos.DT_NOTIFIC, 
      casos.C_EVOLUCAO,
      casos.C_CRITERIO,
      casos.C_CLASSI_FIN
FROM {{ ref( 'staging__casos') }} casos
INNER JOIN {{ ref('staging__municipios') }} mun ON mun.ID_MUNICIPIO = casos.ID_MUNICIP