-- Creación de esquemas para arquitectura Medallion
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Extensión para logs y performance si fuera necesario
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";