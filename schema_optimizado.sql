-- ============================================================================
-- SCHEMA DDL - SISTEMA DE TRACKING OPTIMIZADO
-- Supabase PostgreSQL - Diseño Dimensional
-- ============================================================================

-- DROP todas las tablas existentes (orden correcto por foreign keys)
DROP TABLE IF EXISTS fact_mediciones CASCADE;
DROP TABLE IF EXISTS dim_zonas CASCADE;
DROP TABLE IF EXISTS dim_dispositivo CASCADE;
DROP TABLE IF EXISTS dim_calidad CASCADE;
DROP TABLE IF EXISTS dim_red CASCADE;
DROP TABLE IF EXISTS dim_operador CASCADE;
DROP TABLE IF EXISTS dim_hora CASCADE;
DROP TABLE IF EXISTS dim_tiempo CASCADE;
DROP TABLE IF EXISTS etl_control CASCADE;
DROP VIEW IF EXISTS v_mediciones_completas CASCADE;
DROP FUNCTION IF EXISTS get_calidad_id CASCADE;

-- ============================================================================
-- DIMENSION: TIEMPO
-- ============================================================================
CREATE TABLE dim_tiempo (
    tiempo_id INTEGER PRIMARY KEY,  -- YYYYMMDD como clave natural
    fecha DATE NOT NULL UNIQUE,
    anio SMALLINT NOT NULL,
    trimestre SMALLINT NOT NULL,
    mes SMALLINT NOT NULL,
    semana_anio SMALLINT NOT NULL,
    dia_mes SMALLINT NOT NULL,
    dia_semana SMALLINT NOT NULL,
    nombre_dia VARCHAR(20) NOT NULL,
    nombres_mes VARCHAR(20) NOT NULL
);

CREATE INDEX idx_dim_tiempo_fecha ON dim_tiempo(fecha);
CREATE INDEX idx_dim_tiempo_anio_mes ON dim_tiempo(anio, mes);

-- ============================================================================
-- DIMENSION: HORA
-- ============================================================================
CREATE TABLE dim_hora (
    hora_id SMALLINT PRIMARY KEY,  -- 0-23 como clave natural
    hora SMALLINT NOT NULL UNIQUE,
    franja_horaria VARCHAR(20) NOT NULL
);

-- ============================================================================
-- DIMENSION: OPERADOR
-- ============================================================================
CREATE TABLE dim_operador (
    operador_id SERIAL PRIMARY KEY,
    operador_nombre VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_operador_nombre ON dim_operador(operador_nombre);

-- ============================================================================
-- DIMENSION: RED
-- ============================================================================
CREATE TABLE dim_red (
    red_id SERIAL PRIMARY KEY,
    red_tipo VARCHAR(20) NOT NULL UNIQUE,
    generacion VARCHAR(10),  -- 2G, 3G, 4G, 5G
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_red_tipo ON dim_red(red_tipo);

-- ============================================================================
-- DIMENSION: CALIDAD DE SEÑAL
-- ============================================================================
CREATE TABLE dim_calidad (
    calidad_id SERIAL PRIMARY KEY,
    calidad_categoria VARCHAR(20) NOT NULL UNIQUE,
    signal_min INTEGER,
    signal_max INTEGER,
    descripcion TEXT
);

-- Insertar categorías de calidad predefinidas
INSERT INTO dim_calidad (calidad_categoria, signal_min, signal_max, descripcion) VALUES
    ('EXCELENTE', -70, 0, 'Señal excelente, cobertura óptima'),
    ('BUENA', -85, -71, 'Señal buena, sin problemas de conectividad'),
    ('REGULAR', -95, -86, 'Señal aceptable, posibles interrupciones'),
    ('MALA', -105, -96, 'Señal débil, problemas frecuentes'),
    ('CRITICA', -999, -106, 'Señal crítica, conectividad muy limitada');

-- ============================================================================
-- DIMENSION: DISPOSITIVO
-- Usa device_id (UUID) como identificador principal, NO device_name
-- ============================================================================
CREATE TABLE dim_dispositivo (
    dispositivo_id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL UNIQUE,  -- UUID del dispositivo
    device_name VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_dispositivo_device_id ON dim_dispositivo(device_id);
CREATE INDEX idx_dim_dispositivo_name ON dim_dispositivo(device_name);

-- ============================================================================
-- DIMENSION: ZONAS (Grid geográfico)
-- ============================================================================
CREATE TABLE dim_zonas (
    zona_id SERIAL PRIMARY KEY,
    zona_nombre VARCHAR(100),
    grid_lat INTEGER NOT NULL,
    grid_lon INTEGER NOT NULL,
    centro_lat DECIMAL(10,6),
    centro_lon DECIMAL(10,6),
    total_mediciones BIGINT DEFAULT 0,
    altitud_promedio DECIMAL(10,2),
    grid_latitud_inicio DECIMAL(10,6),
    grid_latitud_fin DECIMAL(10,6),
    grid_longitud_inicio DECIMAL(10,6),
    grid_longitud_fin DECIMAL(10,6),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(grid_lat, grid_lon)
);

CREATE INDEX idx_dim_zonas_grid ON dim_zonas(grid_lat, grid_lon);

-- ============================================================================
-- TABLA DE HECHOS: MEDICIONES (PARTICIONADA POR FECHA)
-- ============================================================================
CREATE TABLE fact_mediciones (
    medicion_id BIGSERIAL,
    
    -- Timestamp original de la medición
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Foreign Keys a dimensiones
    tiempo_id INTEGER NOT NULL REFERENCES dim_tiempo(tiempo_id),
    hora_id SMALLINT NOT NULL REFERENCES dim_hora(hora_id),
    dispositivo_id INTEGER NOT NULL REFERENCES dim_dispositivo(dispositivo_id),
    operador_id INTEGER REFERENCES dim_operador(operador_id),
    red_id INTEGER REFERENCES dim_red(red_id),
    calidad_id INTEGER REFERENCES dim_calidad(calidad_id),
    zona_id INTEGER REFERENCES dim_zonas(zona_id),
    
    -- Atributos descriptivos
    zona_altitud VARCHAR(20),  -- BAJA, MEDIA, ALTA
    
    -- Métricas
    medida_senal DECIMAL(10,2),           -- dBm
    medida_velocidad DECIMAL(10,2),       -- kilómetros por hora (km/h)
    medida_altitud DECIMAL(10,2),         -- metros
    medida_bateria DECIMAL(5,2),          -- porcentaje (0-100)
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    
    -- Metadatos ETL
    etl_loaded_at TIMESTAMPTZ DEFAULT NOW(),
    source_id VARCHAR(100),  -- ID original de la fuente para deduplicación
    
    PRIMARY KEY (medicion_id, timestamp),
    UNIQUE (source_id, timestamp)  -- Deduplicación debe incluir columna de partición
) PARTITION BY RANGE (timestamp);

-- Particiones SEMANALES para máxima velocidad (solo período activo: 14 nov - 6 dic 2025)
-- Con 376k registros / 3 semanas = ~125k por semana = consultas ultra-rápidas

-- Semana 1: 14-20 noviembre
CREATE TABLE fact_mediciones_2025_11_w1 PARTITION OF fact_mediciones
    FOR VALUES FROM ('2025-11-14') TO ('2025-11-21');

-- Semana 2: 21-27 noviembre
CREATE TABLE fact_mediciones_2025_11_w2 PARTITION OF fact_mediciones
    FOR VALUES FROM ('2025-11-21') TO ('2025-11-28');

-- Semana 3: 28 nov - 4 dic
CREATE TABLE fact_mediciones_2025_11_w3 PARTITION OF fact_mediciones
    FOR VALUES FROM ('2025-11-28') TO ('2025-12-05');

-- Semana 4: 5-6 diciembre (última)
CREATE TABLE fact_mediciones_2025_12_w1 PARTITION OF fact_mediciones
    FOR VALUES FROM ('2025-12-05') TO ('2025-12-07');

-- Partición default para datos fuera de rango (por seguridad)
CREATE TABLE fact_mediciones_default PARTITION OF fact_mediciones DEFAULT;

-- ============================================================================
-- ÍNDICES ULTRA-OPTIMIZADOS PARA CONSULTAS RÁPIDAS
-- ============================================================================

-- BRIN index para timestamp (muy compacto, perfecto para datos ordenados temporalmente)
-- 1000x más pequeño que B-tree, ideal para rangos de fecha
CREATE INDEX idx_fact_timestamp_brin ON fact_mediciones USING BRIN (timestamp) WITH (pages_per_range = 128);

-- Índices compuestos para filtros combinados (las queries más comunes)
-- Filtro por fecha + hora (principal uso del dashboard)
CREATE INDEX idx_fact_tiempo_hora ON fact_mediciones(tiempo_id, hora_id) INCLUDE (medida_senal, latitude, longitude);

-- Filtro por fecha + operador (análisis por carrier)
CREATE INDEX idx_fact_tiempo_operador ON fact_mediciones(tiempo_id, operador_id) INCLUDE (medida_senal, calidad_id);

-- Filtro por fecha + red (análisis 4G vs 3G)
CREATE INDEX idx_fact_tiempo_red ON fact_mediciones(tiempo_id, red_id) INCLUDE (medida_senal);

-- Filtro por fecha + dispositivo (seguimiento individual)
CREATE INDEX idx_fact_tiempo_dispositivo ON fact_mediciones(tiempo_id, dispositivo_id);

-- Índice espacial para mapas (coordenadas geográficas)
CREATE INDEX idx_fact_coords ON fact_mediciones(latitude, longitude) INCLUDE (medida_senal, operador_id);

-- Índice para zona (agregaciones geográficas)
CREATE INDEX idx_fact_zona ON fact_mediciones(zona_id) INCLUDE (medida_senal, timestamp);

-- ============================================================================
-- TABLA DE CONTROL ETL (Watermark para carga incremental)
-- ============================================================================
CREATE TABLE etl_control (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    last_extracted_at TIMESTAMPTZ,
    last_loaded_at TIMESTAMPTZ,
    records_processed BIGINT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'IDLE',
    error_message TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insertar registro de control inicial
INSERT INTO etl_control (table_name, last_extracted_at, status) 
VALUES ('fact_mediciones', '1970-01-01 00:00:00+00', 'IDLE');

-- ============================================================================
-- FUNCIÓN: Obtener categoría de calidad de señal
-- ============================================================================
CREATE OR REPLACE FUNCTION get_calidad_id(signal_dbm INTEGER)
RETURNS INTEGER AS $$
BEGIN
    IF signal_dbm IS NULL THEN RETURN NULL; END IF;
    IF signal_dbm >= -70 THEN RETURN 1; END IF;   -- EXCELENTE
    IF signal_dbm >= -85 THEN RETURN 2; END IF;   -- BUENA
    IF signal_dbm >= -95 THEN RETURN 3; END IF;   -- REGULAR
    IF signal_dbm >= -105 THEN RETURN 4; END IF;  -- MALA
    RETURN 5;  -- CRITICA
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- VISTA: Mediciones con todas las dimensiones expandidas
-- ============================================================================
CREATE OR REPLACE VIEW v_mediciones_completas AS
SELECT 
    f.medicion_id,
    f.timestamp,
    f.latitude,
    f.longitude,
    f.medida_senal,
    f.medida_velocidad,
    f.medida_altitud,
    f.medida_bateria,
    f.zona_altitud,
    
    -- Dimensión Tiempo
    t.fecha,
    t.anio,
    t.mes,
    t.nombre_dia,
    
    -- Dimensión Hora
    h.hora,
    h.franja_horaria,
    
    -- Dimensión Dispositivo
    d.device_id,
    d.device_name,
    
    -- Dimensión Operador
    o.operador_nombre,
    
    -- Dimensión Red
    r.red_tipo,
    r.generacion,
    
    -- Dimensión Calidad
    c.calidad_categoria,
    
    -- Dimensión Zona
    z.zona_nombre
    
FROM fact_mediciones f
LEFT JOIN dim_tiempo t ON f.tiempo_id = t.tiempo_id
LEFT JOIN dim_hora h ON f.hora_id = h.hora_id
LEFT JOIN dim_dispositivo d ON f.dispositivo_id = d.dispositivo_id
LEFT JOIN dim_operador o ON f.operador_id = o.operador_id
LEFT JOIN dim_red r ON f.red_id = r.red_id
LEFT JOIN dim_calidad c ON f.calidad_id = c.calidad_id
LEFT JOIN dim_zonas z ON f.zona_id = z.zona_id;

-- ============================================================================
-- VISTAS MATERIALIZADAS PARA AGREGACIONES ULTRA-RÁPIDAS
-- ============================================================================

-- Vista materializada: Estadísticas diarias por operador (actualizar cada hora)
CREATE MATERIALIZED VIEW mv_stats_diarias_operador AS
SELECT 
    t.fecha,
    t.anio,
    t.mes,
    t.nombre_dia,
    o.operador_nombre,
    COUNT(*) as total_mediciones,
    ROUND(AVG(f.medida_senal)::numeric, 2) as senal_promedio,
    ROUND(MIN(f.medida_senal)::numeric, 2) as senal_minima,
    ROUND(MAX(f.medida_senal)::numeric, 2) as senal_maxima,
    COUNT(CASE WHEN c.calidad_categoria = 'EXCELENTE' THEN 1 END) as mediciones_excelente,
    COUNT(CASE WHEN c.calidad_categoria = 'BUENA' THEN 1 END) as mediciones_buena,
    COUNT(CASE WHEN c.calidad_categoria = 'REGULAR' THEN 1 END) as mediciones_regular,
    COUNT(CASE WHEN c.calidad_categoria = 'MALA' THEN 1 END) as mediciones_mala,
    COUNT(CASE WHEN c.calidad_categoria = 'CRITICA' THEN 1 END) as mediciones_critica,
    COUNT(DISTINCT f.dispositivo_id) as dispositivos_unicos
FROM fact_mediciones f
INNER JOIN dim_tiempo t ON f.tiempo_id = t.tiempo_id
INNER JOIN dim_operador o ON f.operador_id = o.operador_id
INNER JOIN dim_calidad c ON f.calidad_id = c.calidad_id
GROUP BY t.fecha, t.anio, t.mes, t.nombre_dia, o.operador_nombre;

CREATE UNIQUE INDEX idx_mv_stats_diarias_op ON mv_stats_diarias_operador(fecha, operador_nombre);

-- Vista materializada: Estadísticas por zona geográfica (para mapas de calor)
CREATE MATERIALIZED VIEW mv_stats_zonas AS
SELECT 
    z.zona_id,
    z.zona_nombre,
    z.grid_lat,
    z.grid_lon,
    z.centro_lat,
    z.centro_lon,
    COUNT(*) as total_mediciones,
    ROUND(AVG(f.medida_senal)::numeric, 2) as senal_promedio,
    ROUND(STDDEV(f.medida_senal)::numeric, 2) as senal_desviacion,
    COUNT(DISTINCT f.operador_id) as operadores_presentes,
    COUNT(DISTINCT f.dispositivo_id) as dispositivos_unicos,
    MAX(f.timestamp) as ultima_medicion
FROM fact_mediciones f
INNER JOIN dim_zonas z ON f.zona_id = z.zona_id
GROUP BY z.zona_id, z.zona_nombre, z.grid_lat, z.grid_lon, z.centro_lat, z.centro_lon;

CREATE UNIQUE INDEX idx_mv_stats_zonas ON mv_stats_zonas(zona_id);
CREATE INDEX idx_mv_stats_zonas_coords ON mv_stats_zonas(centro_lat, centro_lon);

-- Vista materializada: Top dispositivos con más datos
CREATE MATERIALIZED VIEW mv_top_dispositivos AS
SELECT 
    d.dispositivo_id,
    d.device_id,
    d.device_name,
    COUNT(*) as total_mediciones,
    ROUND(AVG(f.medida_senal)::numeric, 2) as senal_promedio,
    COUNT(DISTINCT f.zona_id) as zonas_visitadas,
    MIN(f.timestamp) as primera_medicion,
    MAX(f.timestamp) as ultima_medicion
FROM fact_mediciones f
INNER JOIN dim_dispositivo d ON f.dispositivo_id = d.dispositivo_id
GROUP BY d.dispositivo_id, d.device_id, d.device_name
ORDER BY total_mediciones DESC;

CREATE UNIQUE INDEX idx_mv_top_disp ON mv_top_dispositivos(dispositivo_id);

-- ============================================================================
-- COMENTARIOS DE DOCUMENTACIÓN
-- ============================================================================
COMMENT ON COLUMN fact_mediciones.medida_velocidad IS 'Velocidad en kilómetros por hora (km/h)';
COMMENT ON COLUMN fact_mediciones.medida_bateria IS 'Nivel de batería del dispositivo en porcentaje (0-100)';
COMMENT ON TABLE etl_control IS 'Tabla de control para ETL incremental con estrategia watermark';
COMMENT ON MATERIALIZED VIEW mv_stats_diarias_operador IS 'Estadísticas pre-agregadas por día y operador. Refrescar cada hora con: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_stats_diarias_operador';
COMMENT ON MATERIALIZED VIEW mv_stats_zonas IS 'Estadísticas pre-agregadas por zona geográfica para mapas de calor. Refrescar diariamente';
COMMENT ON MATERIALIZED VIEW mv_top_dispositivos IS 'Top dispositivos ordenados por cantidad de mediciones. Refrescar diariamente';
