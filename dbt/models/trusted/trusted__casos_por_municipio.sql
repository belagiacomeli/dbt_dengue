{{ config(
  alias='trusted__casos_por_municipio',
  schema='projeto_dengue_izabela',
  unique_key='SG_UF',
  tags=['staging', 'projeto_dengue']
) }}

SELECT count(*) as total_casos, mun.NM_MUNICIPIO
FROM {{ ref( 'staging__casos') }} casos
INNER JOIN {{ ref('staging__municipios') }} mun ON mun.ID_MUNICIPIO = casos.ID_MUNICIP
GROUP BY mun.NM_MUNICIPIO