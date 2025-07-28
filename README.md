# Data Engineering Tech Test - Ejercicio 4

Solución completa al **Ejercicio 4: Modelado de Datos**.

## Contenido
- Esquema estrella/snowflake con tablas de hecho y dimensiones.
- DDL SQL (Postgres) con partición y SCD2.
- Carga inicial desde CSV a modelo (Python + SQL).
- Estrategia SCD (Tipo 2) para dimensiones relevantes.
- Simulación de partición lógica y archivado.
- Documentación de decisiones de rendimiento.

## Estructura
```
.
├── README.md
├── docs/
│   ├── arquitectura.md
│   ├── decisiones.md
│   └── retos_y_siguientes_pasos.md
├── sql/
│   └── ddl_modelo.sql
└── src/
    └── de_test/
        └── pipelines/
            └── etl_transactions.py
```
