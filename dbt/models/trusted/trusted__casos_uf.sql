{{ config(
  schema='projeto_dengue_izabela',
  unique_key='SG_UF',
  tags=['staging', 'projeto_dengue']
) }}


SELECT
    mun.NM_UF,
    COUNT(*) AS total_casos
FROM {{ ref( 'staging__casos') }} casos
INNER JOIN {{ ref( 'staging__municipios') }} mun ON mun.ID_MUNICIPIO = casos.ID_MUNICIP
GROUP BY
    mun.NM_UF
ORDER BY
    total_casos DESC