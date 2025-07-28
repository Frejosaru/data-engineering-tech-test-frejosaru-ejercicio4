# Arquitectura del Modelo de Datos

- Modelo estrella con dimensiones: dim_customer, dim_merchant, dim_date, dim_payment_method, dim_currency.
- Tabla de hechos: fact_transactions.
- Particiones mensuales por transaction_ts.
- SCD2 en dim_customer y dim_merchant.
