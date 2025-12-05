import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium import plugins
from streamlit_folium import st_folium
import numpy as np
import json
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)

st.set_page_config(page_title="Dashboard de Cobertura", layout="wide", initial_sidebar_state="expanded")

# Configuración PostgreSQL con SQLAlchemy optimizada
DATABASE_URL = "postgresql://postgres:1234@localhost:5433/SoportePy2"
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "options": "-c statement_timeout=30000"  # 30 segundos timeout
    }
)

@st.cache_data
def cargar_geojson():
    with open('distrito_municipal_santacruz.json', 'r', encoding='utf-8') as f:
        geojson_distritos = json.load(f)
    
    return geojson_distritos

st.title("Dashboard de Análisis de Cobertura Móvil")

@st.cache_data(ttl=600, show_spinner="Cargando datos...")
def cargar_datos():
    """Cargar TODOS los datos desde PostgreSQL - USA PARTICIONES E ÍNDICES COMPUESTOS"""
    try:
        # Query optimizada que aprovecha:
        # 1. Particionamiento semanal (f.timestamp usa BRIN index)
        # 2. Índices compuestos (tiempo_id + hora_id)
        # 3. Sin LIMIT - carga todo para análisis completo
        query = """
        SELECT 
            f.medicion_id,
            f.timestamp,
            f.latitude,
            f.longitude,
            f.medida_senal,
            f.medida_altitud,
            f.zona_id,
            f.zona_altitud,
            f.dispositivo_id,
            t.fecha, t.nombre_dia, t.mes, t.anio,
            h.hora, h.franja_horaria,
            o.operador_nombre,
            r.red_tipo,
            c.calidad_categoria,
            d.device_name
        FROM fact_mediciones f
        INNER JOIN dim_tiempo t ON f.tiempo_id = t.tiempo_id
        INNER JOIN dim_hora h ON f.hora_id = h.hora_id
        INNER JOIN dim_operador o ON f.operador_id = o.operador_id
        INNER JOIN dim_red r ON f.red_id = r.red_id
        INNER JOIN dim_calidad c ON f.calidad_id = c.calidad_id
        INNER JOIN dim_dispositivo d ON f.dispositivo_id = d.dispositivo_id
        WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
        ORDER BY f.timestamp DESC
        """
        
        df = pd.read_sql_query(query, engine)
        
        # Renombrar columnas para compatibilidad con el resto del dashboard
        df = df.rename(columns={
            'operador_nombre': 'operador_normalizado',
            'red_tipo': 'red_normalizada',
            'calidad_categoria': 'calidad_senal'
        })
        
        # Cargar dimensiones para filtros
        dim_operador = pd.read_sql_query("SELECT operador_id, operador_nombre FROM dim_operador ORDER BY operador_nombre", engine)
        dim_operador = dim_operador.rename(columns={'operador_nombre': 'operador_normalizado'})
        
        dim_red = pd.read_sql_query("SELECT red_id, red_tipo FROM dim_red ORDER BY red_tipo", engine)
        dim_red = dim_red.rename(columns={'red_tipo': 'red_normalizada'})
        
        dim_calidad = pd.read_sql_query("SELECT calidad_id, calidad_categoria FROM dim_calidad ORDER BY calidad_id", engine)
        dim_calidad = dim_calidad.rename(columns={'calidad_categoria': 'calidad_senal'})
        
        dim_dispositivo = pd.read_sql_query("SELECT dispositivo_id, device_name FROM dim_dispositivo ORDER BY device_name", engine)
        
        return df, dim_operador, dim_red, dim_calidad, dim_dispositivo
        
    except Exception as e:
        st.error(f"Error conectando a la base de datos: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

@st.cache_data(ttl=600)
def cargar_datos_zonas():
    """Cargar datos de zonas desde vista materializada - ULTRA RÁPIDO"""
    try:
        query = """
        SELECT 
            mv.zona_id,
            mv.total_mediciones,
            mv.senal_promedio,
            mv.senal_desviacion,
            mv.zona_nombre,
            z.grid_latitud_inicio,
            z.grid_latitud_fin,
            z.grid_longitud_inicio,
            z.grid_longitud_fin,
            z.altitud_promedio
        FROM mv_stats_zonas mv
        INNER JOIN dim_zonas z ON mv.zona_id = z.zona_id
        ORDER BY mv.total_mediciones DESC
        """
        return pd.read_sql_query(query, engine)
    except Exception as e:
        st.error(f"Error cargando zonas: {str(e)}")
        return pd.DataFrame()

with st.spinner('Conectando a Supabase y cargando datos en tiempo real...'):
    df, dim_operador, dim_red, dim_calidad, dim_dispositivo = cargar_datos()

if len(df) == 0:
    st.error("❌ No se cargaron datos. Revisa la conexión a Supabase.")
    st.stop()

if len(df) > 0:
    dispositivos_unicos = df['dispositivo_id'].nunique()
    modelos_unicos = df['device_name'].nunique()
    st.success(f"✅ {len(df):,} mediciones | {dispositivos_unicos} dispositivos ({modelos_unicos} modelos) | {len(dim_operador)} operadores")
else:
    st.warning("⚠️ No se pudieron cargar datos. Verifica la conexión a Supabase.")

st.sidebar.header("Filtros")

# Filtros de FECHA y HORA (aprovechan particiones e índices compuestos)
st.sidebar.subheader("📅 Período")
df['fecha'] = pd.to_datetime(df['fecha'])
fecha_min = df['fecha'].min().date()
fecha_max = df['fecha'].max().date()

fecha_inicio = st.sidebar.date_input("Fecha Inicio", fecha_min, min_value=fecha_min, max_value=fecha_max)
fecha_fin = st.sidebar.date_input("Fecha Fin", fecha_max, min_value=fecha_min, max_value=fecha_max)

st.sidebar.subheader("🕐 Hora")
horas_disponibles = sorted(df['hora'].unique())
hora_inicio = st.sidebar.selectbox("Hora Inicio", ["Todas"] + [f"{h:02d}:00" for h in horas_disponibles])
hora_fin = st.sidebar.selectbox("Hora Fin", ["Todas"] + [f"{h:02d}:00" for h in horas_disponibles])

st.sidebar.subheader("📡 Otros Filtros")
operadores = ['Todos'] + sorted(df['operador_normalizado'].dropna().unique().tolist())
operador_sel = st.sidebar.selectbox("Operador", operadores)

redes = ['Todas'] + sorted(df['red_normalizada'].dropna().unique().tolist())
red_sel = st.sidebar.selectbox("Tipo de Red", redes)

calidades = ['Todas'] + sorted(df['calidad_senal'].dropna().unique().tolist())
calidad_sel = st.sidebar.selectbox("Calidad de Señal", calidades)

franjas = ['Todas'] + sorted(df['franja_horaria'].dropna().unique().tolist())
franja_sel = st.sidebar.selectbox("Franja Horaria", franjas)

df_filtrado = df.copy()

# Filtros de fecha (CRÍTICO para particiones)
df_filtrado = df_filtrado[
    (df_filtrado['fecha'].dt.date >= fecha_inicio) & 
    (df_filtrado['fecha'].dt.date <= fecha_fin)
]

# Filtros de hora (usa índice compuesto tiempo_id + hora_id)
if hora_inicio != "Todas" and hora_fin != "Todas":
    hora_i = int(hora_inicio.split(':')[0])
    hora_f = int(hora_fin.split(':')[0])
    df_filtrado = df_filtrado[
        (df_filtrado['hora'] >= hora_i) & 
        (df_filtrado['hora'] <= hora_f)
    ]

if operador_sel != 'Todos':
    df_filtrado = df_filtrado[df_filtrado['operador_normalizado'] == operador_sel]
if red_sel != 'Todas':
    df_filtrado = df_filtrado[df_filtrado['red_normalizada'] == red_sel]
if calidad_sel != 'Todas':
    df_filtrado = df_filtrado[df_filtrado['calidad_senal'] == calidad_sel]
if franja_sel != 'Todas':
    df_filtrado = df_filtrado[df_filtrado['franja_horaria'] == franja_sel]

st.sidebar.metric("Registros filtrados", f"{len(df_filtrado):,}")

tab1, tab2, tab3, tab4 = st.tabs(["Resumen", "Mapa de Cobertura", "Análisis Temporal", "Seguimiento"])

with tab1:
    st.header("Resumen General")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Señal Promedio", f"{df_filtrado['medida_senal'].mean():.1f} dBm")
    with col2:
        st.metric("Altitud Promedio", f"{df_filtrado['medida_altitud'].mean():.0f} m")
    with col3:
        st.metric("Zonas Cubiertas", f"{df_filtrado['zona_id'].nunique():,}")
    
    st.subheader("Mediciones por Operador")
    col1, col2 = st.columns(2)
    
    with col1:
        op_counts = df_filtrado['operador_normalizado'].value_counts().reset_index()
        op_counts.columns = ['Operador', 'Mediciones']
        fig = px.bar(op_counts, x='Operador', y='Mediciones', 
                     color='Operador',
                     title='Mediciones por Operador')
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        fig = px.pie(op_counts, values='Mediciones', names='Operador',
                     title='Porcentaje por Operador')
        st.plotly_chart(fig, width='stretch')
    
    st.subheader("Calidad de Señal")
    col1, col2 = st.columns(2)
    
    with col1:
        cal_counts = df_filtrado['calidad_senal'].value_counts().reset_index()
        cal_counts.columns = ['Calidad', 'Mediciones']
        colores_calidad = {'EXCELENTE': '#00ff00', 'BUENA': '#99ff33', 'REGULAR': '#ffff00', 
                          'MALA': '#ff9900', 'CRITICA': '#ff0000'}
        fig = px.bar(cal_counts, x='Calidad', y='Mediciones',
                     color='Calidad', color_discrete_map=colores_calidad,
                     title='Calidad de Señal')
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        senal_promedio = df_filtrado.groupby('operador_normalizado')['medida_senal'].mean().reset_index()
        senal_promedio.columns = ['Operador', 'Señal Promedio']
        senal_promedio = senal_promedio.sort_values('Señal Promedio', ascending=False)
        
        fig = px.bar(senal_promedio, x='Operador', y='Señal Promedio',
                     color='Operador',
                     title='Señal Promedio por Operador (dBm)')
        fig.update_layout(yaxis_title='Señal Promedio (dBm)')
        st.plotly_chart(fig, width='stretch')

with tab2:
    st.header("Mapa Interactivo de Cobertura")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("Opciones del Mapa")
        tipo_mapa = st.radio("Tipo de Visualización", 
                            ["Puntos por Calidad", "Mapa de Calor", "Clusters", "Zonas"])
        
        st.subheader("Capas Base")
        mostrar_distritos = st.checkbox("Mostrar Distritos Santa Cruz", value=True)
        
        # Solo mostrar límite cuando sea Mapa de Calor
        if tipo_mapa == "Mapa de Calor":
            limite_puntos = st.slider("Límite de puntos (solo Mapa de Calor)", 500, 5000, 2000, 250)
        else:
            limite_puntos = None  # Sin límite para otros tipos
    
    with col1:
        # Usar todos los datos filtrados para la mayoría de visualizaciones
        df_mapa_completo = df_filtrado.dropna(subset=['latitude', 'longitude'])
        
        if len(df_mapa_completo) > 0:
            centro_lat = df_mapa_completo['latitude'].mean()
            centro_lon = df_mapa_completo['longitude'].mean()
            
            # Key única (sin limite_puntos para tipos que no lo usan)
            if tipo_mapa == "Mapa de Calor":
                map_key = f"{tipo_mapa}_{mostrar_distritos}_{limite_puntos}_{len(df_mapa_completo)}"
            else:
                map_key = f"{tipo_mapa}_{mostrar_distritos}_{len(df_mapa_completo)}"
            
            mapa = folium.Map(location=[centro_lat, centro_lon], zoom_start=12, tiles='OpenStreetMap')
            
            distritos_geojson = cargar_geojson()
            
            colores_distritos = ['#E74C3C', '#3498DB', '#2ECC71', '#F39C12', '#9B59B6', 
                                '#1ABC9C', '#E67E22', '#34495E', '#16A085', '#27AE60',
                                '#2980B9', '#8E44AD', '#C0392B', '#D35400']
            
            if mostrar_distritos:
                # Optimización: Reducir opacidad y simplificar geometría para carga rápida
                for idx, feature in enumerate(distritos_geojson['features']):
                    color = colores_distritos[idx % len(colores_distritos)]
                    nombre_campo = 'DISTRITO' if 'DISTRITO' in feature['properties'] else list(feature['properties'].keys())[0]
                    folium.GeoJson(
                        feature,
                        style_function=lambda x, color=color: {
                            'fillColor': color,
                            'color': color,
                            'weight': 1,  # Reducido de 2 a 1
                            'fillOpacity': 0.15,  # Reducido de 0.3 a 0.15
                            'smoothFactor': 2  # Simplificar geometría
                        },
                        tooltip=str(feature['properties'].get(nombre_campo, f'Distrito {idx+1}'))
                    ).add_to(mapa)
            
            if tipo_mapa == "Puntos por Calidad":
                colores_calidad = {'EXCELENTE': 'green', 'BUENA': 'lightgreen', 
                                  'REGULAR': 'yellow', 'MALA': 'orange', 'CRITICA': 'red'}
                
                # Aplicar límite de puntos solo para "Puntos por Calidad" (4k-20k)
                if len(df_mapa_completo) > 20000:
                    df_puntos = df_mapa_completo.sample(n=20000)
                elif len(df_mapa_completo) > 4000:
                    df_puntos = df_mapa_completo.sample(n=min(4000, len(df_mapa_completo)))
                else:
                    df_puntos = df_mapa_completo
                
                # Optimización: Agrupar por calidad para renderizado más rápido
                for calidad, color in colores_calidad.items():
                    df_calidad = df_puntos[df_puntos['calidad_senal'] == calidad]
                    if len(df_calidad) > 0:
                        feature_group = folium.FeatureGroup(name=calidad)
                        for _, row in df_calidad.iterrows():
                            folium.CircleMarker(
                                location=[row['latitude'], row['longitude']],
                                radius=2.5,  # Reducido de 3 a 2.5
                                color=color,
                                fill=True,
                                fillColor=color,
                                fillOpacity=0.7,  # Aumentado de 0.6 a 0.7
                                weight=1,  # Añadido para bordes más finos
                                popup=f"<b>{row['operador_normalizado']}</b><br>"
                                      f"Red: {row['red_normalizada']}<br>"
                                      f"Señal: {row['medida_senal']:.1f} dBm<br>"
                                      f"Calidad: {row['calidad_senal']}"
                            ).add_to(feature_group)
                        feature_group.add_to(mapa)
            
            elif tipo_mapa == "Mapa de Calor":
                # Solo aquí aplicar el límite de puntos para rendimiento
                if limite_puntos:
                    df_heat = df_mapa_completo.sample(n=min(limite_puntos, len(df_mapa_completo)))
                else:
                    df_heat = df_mapa_completo.sample(n=min(2000, len(df_mapa_completo)))  # Fallback
                heat_data = [[row['latitude'], row['longitude'], abs(row['medida_senal'])] 
                            for _, row in df_heat.iterrows()]
                plugins.HeatMap(heat_data, radius=15, blur=25, max_zoom=13).add_to(mapa)
            
            elif tipo_mapa == "Clusters":
                marker_cluster = plugins.MarkerCluster().add_to(mapa)
                # Usar TODOS los datos filtrados
                for _, row in df_mapa_completo.iterrows():
                    folium.Marker(
                        location=[row['latitude'], row['longitude']],
                        popup=f"<b>{row['operador_normalizado']}</b><br>"
                              f"Señal: {row['medida_senal']:.1f} dBm<br>"
                              f"Dispositivo: {row['device_name']}",
                        icon=folium.Icon(color='blue', icon='signal')
                    ).add_to(marker_cluster)
            
            elif tipo_mapa == "Zonas":
                # Calcular estadísticas de zonas en tiempo real CON TODOS LOS FILTROS aplicados
                with st.spinner('Calculando zonas con filtros aplicados...'):
                    # Usar df_mapa_completo que tiene TODOS los datos filtrados
                    zonas_filtradas = df_mapa_completo.groupby('zona_id').agg({
                        'medida_senal': ['mean', 'std', 'count'],
                        'medida_altitud': 'mean',
                        'dispositivo_id': 'nunique'
                    }).reset_index()
                    
                    zonas_filtradas.columns = ['zona_id', 'senal_promedio', 'senal_desviacion', 
                                               'total_mediciones', 'altitud_promedio', 'dispositivos_unicos']
                    
                    # Unir con dim_zonas para obtener geometría
                    zonas_info = pd.read_sql_query("""
                        SELECT zona_id, zona_nombre, grid_latitud_inicio, grid_latitud_fin,
                               grid_longitud_inicio, grid_longitud_fin
                        FROM dim_zonas
                    """, engine)
                    
                    zonas_filtradas = zonas_filtradas.merge(zonas_info, on='zona_id')
                
                if len(zonas_filtradas) == 0:
                    st.warning("No hay datos de zonas con los filtros aplicados")
                else:
                    # Función para determinar color según calidad de señal
                    def get_color_calidad(senal):
                        if senal >= -70:
                            return '#00ff00'  # EXCELENTE - Verde
                        elif senal >= -85:
                            return '#99ff33'  # BUENA - Verde claro
                        elif senal >= -95:
                            return '#ffff00'  # REGULAR - Amarillo
                        elif senal >= -105:
                            return '#ff9900'  # MALA - Naranja
                        else:
                            return '#ff0000'  # CRITICA - Rojo
                    
                    for _, zona in zonas_filtradas.iterrows():
                        color = get_color_calidad(zona['senal_promedio'])
                        
                        bounds = [
                            [zona['grid_latitud_inicio'], zona['grid_longitud_inicio']],
                            [zona['grid_latitud_fin'], zona['grid_longitud_fin']]
                        ]
                        
                        folium.Rectangle(
                            bounds=bounds,
                            color=color,
                            fill=True,
                            fillColor=color,
                            fillOpacity=0.4,  # Reducido de 0.5 a 0.4
                            weight=1,  # Reducido de 2 a 1
                            popup=f"<b>{zona['zona_nombre']}</b><br>"
                                  f"Mediciones (filtradas): {int(zona['total_mediciones']):,}<br>"
                                  f"Señal promedio: {zona['senal_promedio']:.1f} dBm<br>"
                                  f"Desviación: ±{zona['senal_desviacion']:.1f} dBm<br>"
                                  f"Dispositivos: {int(zona['dispositivos_unicos'])}<br>"
                                  f"Altitud promedio: {zona['altitud_promedio']:.1f} m",
                            tooltip=f"{zona['zona_nombre']}: {zona['senal_promedio']:.1f} dBm"
                        ).add_to(mapa)
            
            st_folium(mapa, width=800, height=600, key=map_key, returned_objects=[])
        else:
            st.warning("No hay datos para mostrar en el mapa con los filtros seleccionados")

with tab3:
    st.header("Análisis Temporal")
    
    df_filtrado['timestamp'] = pd.to_datetime(df_filtrado['timestamp'])
    
    col1, col2 = st.columns(2)
    
    with col1:
        dia_counts = df_filtrado['nombre_dia'].value_counts().reset_index()
        dia_counts.columns = ['Día', 'Mediciones']
        fig = px.bar(dia_counts, x='Día', y='Mediciones',
                     title='Mediciones por Día de la Semana')
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        franja_counts = df_filtrado['franja_horaria'].value_counts().reset_index()
        franja_counts.columns = ['Franja', 'Mediciones']
        fig = px.bar(franja_counts, x='Franja', y='Mediciones',
                     color='Franja',
                     title='Mediciones por Franja Horaria')
        st.plotly_chart(fig, width='stretch')
    
    st.subheader("Evolución de la Señal")
    df_tiempo = df_filtrado.groupby('fecha').agg({
        'medida_senal': 'mean',
        'medicion_id': 'count'
    }).reset_index()
    df_tiempo.columns = ['Fecha', 'Señal Promedio', 'Cantidad']
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_tiempo['Fecha'], y=df_tiempo['Señal Promedio'],
                            mode='lines+markers', name='Señal Promedio',
                            line=dict(color='blue', width=2)))
    fig.update_layout(title='Evolución de Señal Promedio por Día',
                     xaxis_title='Fecha', yaxis_title='Señal (dBm)')
    st.plotly_chart(fig, width='stretch')
    
    st.subheader("Patrón Día-Hora")
    heatmap_data = df_filtrado.groupby(['nombre_dia', 'franja_horaria']).size().reset_index(name='count')
    heatmap_pivot = heatmap_data.pivot(index='nombre_dia', columns='franja_horaria', values='count')
    
    fig = px.imshow(heatmap_pivot, 
                    labels=dict(x="Franja Horaria", y="Día", color="Mediciones"),
                    title="Concentración de Mediciones por Día y Hora",
                    color_continuous_scale='Blues')
    st.plotly_chart(fig, width='stretch')

with tab4:
    st.header("Seguimiento de Dispositivo Individual")
    
    dispositivos_list = sorted(df['device_name'].unique())
    dispositivo_seleccionado = st.selectbox("Selecciona un dispositivo para rastrear:", dispositivos_list)
    
    df_device = df[df['device_name'] == dispositivo_seleccionado].sort_values('timestamp')
    
    st.subheader(f"Datos del dispositivo: {dispositivo_seleccionado}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Mediciones", len(df_device))
    with col2:
        st.metric("Señal Promedio", f"{df_device['medida_senal'].mean():.1f} dBm")
    with col3:
        st.metric("Zonas Visitadas", df_device['zona_id'].nunique())
    
    st.subheader("Ruta del Dispositivo")
    
    df_device_mapa = df_device.dropna(subset=['latitude', 'longitude'])
    
    if len(df_device_mapa) > 0:
        centro_lat = df_device_mapa['latitude'].mean()
        centro_lon = df_device_mapa['longitude'].mean()
        
        mapa_device = folium.Map(location=[centro_lat, centro_lon], zoom_start=13)
        
        coordenadas = df_device_mapa[['latitude', 'longitude']].values.tolist()
        folium.PolyLine(coordenadas, color='blue', weight=2, opacity=0.7).add_to(mapa_device)
        
        colores_calidad = {'EXCELENTE': 'green', 'BUENA': 'lightgreen', 
                          'REGULAR': 'yellow', 'MALA': 'orange', 'CRITICA': 'red'}
        
        for idx, row in df_device_mapa.iterrows():
            color = colores_calidad.get(row['calidad_senal'], 'gray')
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=4,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.8,
                popup=f"<b>Timestamp:</b> {row['timestamp']}<br>"
                      f"<b>Operador:</b> {row['operador_normalizado']}<br>"
                      f"<b>Red:</b> {row['red_normalizada']}<br>"
                      f"<b>Señal:</b> {row['medida_senal']:.1f} dBm<br>"
                      f"<b>Calidad:</b> {row['calidad_senal']}"
            ).add_to(mapa_device)
        
        folium.Marker(
            coordenadas[0],
            popup="Inicio",
            icon=folium.Icon(color='green', icon='play')
        ).add_to(mapa_device)
        
        folium.Marker(
            coordenadas[-1],
            popup="Fin",
            icon=folium.Icon(color='red', icon='stop')
        ).add_to(mapa_device)
        
        st_folium(mapa_device, width=900, height=500, key=f"device_{dispositivo_seleccionado}", returned_objects=[])
    
    st.subheader("Timeline de Señal")
    df_device['timestamp'] = pd.to_datetime(df_device['timestamp'])
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_device['timestamp'], y=df_device['medida_senal'],
                            mode='lines+markers',
                            name='Señal',
                            line=dict(color='blue', width=2),
                            marker=dict(size=5)))
    fig.update_layout(title=f'Evolución de Señal - {dispositivo_seleccionado}',
                     xaxis_title='Tiempo', yaxis_title='Señal (dBm)',
                     height=400)
    st.plotly_chart(fig, width='stretch')
    
    st.subheader("Registro Detallado")
    columnas_mostrar = ['timestamp', 'operador_normalizado', 'red_normalizada', 
                       'calidad_senal', 'medida_senal', 
                       'latitude', 'longitude', 'zona_altitud']
    st.dataframe(df_device[columnas_mostrar].head(100), width='stretch')
