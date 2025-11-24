CREATE TABLE DIM_TIEMPO (
    tiempo_id INT PRIMARY KEY,
    fecha DATE NOT NULL,
    anio INT NOT NULL,
    trimestre INT NOT NULL,
    mes INT NOT NULL,
    semana_anio INT NOT NULL,
    dia_mes INT NOT NULL,
    dia_semana INT NOT NULL,
    nombre_dia VARCHAR(10) NOT NULL,
    es_fin_semana VARCHAR(2) NOT NULL
);

CREATE TABLE DIM_HORA (
    hora_id INT PRIMARY KEY,
    hora INT NOT NULL,
    franja_horaria VARCHAR(20) NOT NULL
);

CREATE TABLE DIM_OPERADOR (
    operador_id INT PRIMARY KEY,
    operador_normalizado VARCHAR(50) NOT NULL
);

CREATE TABLE DIM_RED (
    red_id INT PRIMARY KEY,
    red_normalizada VARCHAR(20) NOT NULL
);

CREATE TABLE DIM_CALIDAD (
    calidad_id INT PRIMARY KEY,
    calidad_senal VARCHAR(20) NOT NULL
);

CREATE TABLE DIM_UBICACION (
    ubicacion_id INT PRIMARY KEY,
    sector_id VARCHAR(50) NOT NULL,
    zona_altitud VARCHAR(10) NOT NULL,
    centro_lat DECIMAL(10,6) NOT NULL,
    centro_lon DECIMAL(10,6) NOT NULL,
    altitud_promedio DECIMAL(10,2) NOT NULL,
    altitud_minima DECIMAL(10,2) NOT NULL,
    altitud_maxima DECIMAL(10,2) NOT NULL
);

CREATE TABLE DIM_DISPOSITIVO (
    dispositivo_id INT PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL
);

CREATE TABLE DIM_ZONAS (
    zona_id INT PRIMARY KEY,
    zona_nombre VARCHAR(50) NOT NULL,
    centro_lat DECIMAL(10,6) NOT NULL,
    centro_lon DECIMAL(10,6) NOT NULL,
    total_mediciones INT NOT NULL,
    altitud_promedio DECIMAL(10,2) NOT NULL,
    grid_lat INT NOT NULL,
    grid_lon INT NOT NULL,
    grid_latitud_inicio DECIMAL(10,6) NOT NULL,
    grid_latitud_fin DECIMAL(10,6) NOT NULL,
    grid_longitud_inicio DECIMAL(10,6) NOT NULL,
    grid_longitud_fin DECIMAL(10,6) NOT NULL
);

CREATE TABLE FACT_MEDICIONES (
    medicion_id BIGINT PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    tiempo_id INT NOT NULL,
    hora_id INT NOT NULL,
    ubicacion_id INT NOT NULL,
    operador_id INT NOT NULL,
    red_id INT NOT NULL,
    calidad_id INT NOT NULL,
    dispositivo_id INT NOT NULL,
    zona_id INT NOT NULL,
    medida_senal DECIMAL(10,2) NOT NULL,
    medida_velocidad DECIMAL(10,2) NOT NULL,
    medida_altitud DECIMAL(10,2) NOT NULL,
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    FOREIGN KEY (tiempo_id) REFERENCES DIM_TIEMPO(tiempo_id),
    FOREIGN KEY (hora_id) REFERENCES DIM_HORA(hora_id),
    FOREIGN KEY (ubicacion_id) REFERENCES DIM_UBICACION(ubicacion_id),
    FOREIGN KEY (operador_id) REFERENCES DIM_OPERADOR(operador_id),
    FOREIGN KEY (red_id) REFERENCES DIM_RED(red_id),
    FOREIGN KEY (calidad_id) REFERENCES DIM_CALIDAD(calidad_id),
    FOREIGN KEY (dispositivo_id) REFERENCES DIM_DISPOSITIVO(dispositivo_id),
    FOREIGN KEY (zona_id) REFERENCES DIM_ZONAS(zona_id)
);
