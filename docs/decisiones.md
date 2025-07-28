# Decisiones de Rendimiento

- Índices en transaction_ts, customer_sk y merchant_sk.
- Partición por rango para mejorar queries por fecha.
- SCD Tipo 2 en dimensiones críticas para historial de cambios.
- Archiving moviendo particiones viejas al esquema archive.
