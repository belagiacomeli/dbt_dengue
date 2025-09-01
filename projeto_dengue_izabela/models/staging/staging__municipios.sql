{{ config(
  materialized='incremental',
  alias='staging__municipios',
  schema='projeto_dengue_izabela',
  unique_key='ID_MUNICIPIO',
  tags=['staging', 'projeto_dengue']
) }}

with municipios as (
  select
    cast(ID_UF as int64) as ID_UF,
    cast(NM_UF as string) as NM_UF,
    cast(ID_REGIAO_INTERMEDIARIA as int64) as ID_REGIAO_INTERMEDIARIA,
    cast(ID_MUNICIPIO as int64) as ID_MUNICIPIO,
    cast(NM_MUNICIPIO as string) as NM_MUNICIPIO,
    current_timestamp() as processed_at,
    row_number() over (
      partition by 
        cast(ID_MUNICIPIO as int64), 
        cast(ID_UF as int64), 
        cast(ID_REGIAO_INTERMEDIARIA as int64)
      order by cast(ID_MUNICIPIO as int64)
    ) as rn
  from {{ source('projeto_dengue_izabela', 'raw__municipios') }}
  where ID_MUNICIPIO is not null
)
select *
from municipios
where rn = 1


