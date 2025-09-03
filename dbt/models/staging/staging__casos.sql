{{ config(
  materialized='incremental',
  alias='staging__casos',
  tags=['staging', 'projeto_dengue']
) }}

select 
  cast(ID_MUNICIP as INTEGER) as ID_MUNICIP,
  cast(SG_UF as INTEGER) as SG_UF,
  cast(ID_AGRAVO as STRING) as ID_AGRAVO,
  cast(DT_NOTIFIC as DATE) as DT_NOTIFIC,
  cast(C_CLASSI_FIN as STRING) as C_CLASSI_FIN,
  cast(CS_SEXO as STRING) as CS_SEXO,
  cast(C_CRITERIO as STRING) as C_CRITERIO,
  cast(C_ESCOLARIDADE as STRING) as C_ESCOLARIDADE,
  cast(C_EVOLUCAO as STRING) as C_EVOLUCAO,
  cast(C_SOROTIPO as STRING) as C_SOROTIPO,
  current_timestamp() as processed_at
from {{ source('projeto_dengue_izabela', 'raw__casos') }}
