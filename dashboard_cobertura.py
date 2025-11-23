import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium import plugins
from streamlit_folium import folium_static
import numpy as np

st.set_page_config(page_title="Dashboard de Cobertura", layout="wide", initial_sidebar_state="expanded")

st.title("Dashboard de Análisis de Cobertura Móvil")

@st.cache_data
def cargar_datos():
    fact = pd.read_csv('output/datawarehouse/FACT_MEDICIONES.csv')
    dim_tiempo = pd.read_csv('output/datawarehouse/DIM_TIEMPO.csv')
    dim_hora = pd.read_csv('output/datawarehouse/DIM_HORA.csv')
    dim_operador = pd.read_csv('output/datawarehouse/DIM_OPERADOR.csv')
    dim_red = pd.read_csv('output/datawarehouse/DIM_RED.csv')
    dim_calidad = pd.read_csv('output/datawarehouse/DIM_CALIDAD.csv')
    dim_velocidad = pd.read_csv('output/datawarehouse/DIM_VELOCIDAD.csv')
    dim_ubicacion = pd.read_csv('output/datawarehouse/DIM_UBICACION.csv')
    dim_dispositivo = pd.read_csv('output/datawarehouse/DIM_DISPOSITIVO.csv')
    
    df = fact.merge(dim_tiempo, on='tiempo_id', how='left')
    df = df.merge(dim_hora, on='hora_id', how='left')
    df = df.merge(dim_operador, on='operador_id', how='left')
    df = df.merge(dim_red, on='red_id', how='left')
    df = df.merge(dim_calidad, on='calidad_id', how='left')
    df = df.merge(dim_velocidad, on='velocidad_id', how='left')
    df = df.merge(dim_ubicacion, on='ubicacion_id', how='left')
    df = df.merge(dim_dispositivo, on='dispositivo_id', how='left')
    
    return df, dim_operador, dim_red, dim_calidad, dim_dispositivo

with st.spinner('Cargando datos del data warehouse...'):
    df, dim_operador, dim_red, dim_calidad, dim_dispositivo = cargar_datos()

st.success(f"{len(df):,} mediciones cargadas | {df['device_name'].nunique()} dispositivos | {len(dim_operador)} operadores")

st.sidebar.header("Filtros")

operadores = ['Todos'] + sorted(df['operador_normalizado'].dropna().unique().tolist())
operador_sel = st.sidebar.selectbox("Operador", operadores)

redes = ['Todas'] + sorted(df['red_normalizada'].dropna().unique().tolist())
red_sel = st.sidebar.selectbox("Tipo de Red", redes)

calidades = ['Todas'] + sorted(df['calidad_senal'].dropna().unique().tolist())
calidad_sel = st.sidebar.selectbox("Calidad de Señal", calidades)

franjas = ['Todas'] + sorted(df['franja_horaria'].dropna().unique().tolist())
franja_sel = st.sidebar.selectbox("Franja Horaria", franjas)

df_filtrado = df.copy()
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
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Señal Promedio", f"{df_filtrado['medida_senal'].mean():.1f} dBm")
    with col2:
        st.metric("Velocidad Promedio", f"{df_filtrado['medida_velocidad'].mean():.2f} km/h")
    with col3:
        st.metric("Altitud Promedio", f"{df_filtrado['medida_altitud'].mean():.0f} m")
    with col4:
        st.metric("Zonas Cubiertas", f"{df_filtrado['ubicacion_id'].nunique():,}")
    
    st.subheader("Mediciones por Operador")
    col1, col2 = st.columns(2)
    
    with col1:
        op_counts = df_filtrado['operador_normalizado'].value_counts().reset_index()
        op_counts.columns = ['Operador', 'Mediciones']
        fig = px.bar(op_counts, x='Operador', y='Mediciones', 
                     color='Operador',
                     title='Mediciones por Operador')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(op_counts, values='Mediciones', names='Operador',
                     title='Porcentaje por Operador')
        st.plotly_chart(fig, use_container_width=True)
    
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
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        senal_promedio = df_filtrado.groupby('operador_normalizado')['medida_senal'].mean().reset_index()
        senal_promedio.columns = ['Operador', 'Señal Promedio']
        senal_promedio = senal_promedio.sort_values('Señal Promedio', ascending=False)
        
        fig = px.bar(senal_promedio, x='Operador', y='Señal Promedio',
                     color='Operador',
                     title='Señal Promedio por Operador (dBm)')
        fig.update_layout(yaxis_title='Señal Promedio (dBm)')
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("Mapa Interactivo de Cobertura")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("Opciones del Mapa")
        tipo_mapa = st.radio("Tipo de Visualización", 
                            ["Puntos por Calidad", "Mapa de Calor", "Clusters"])
        
        limite_puntos = st.slider("Límite de puntos", 100, 10000, 5000, 100)
    
    with col1:
        df_mapa = df_filtrado.dropna(subset=['latitude', 'longitude']).head(limite_puntos)
        
        if len(df_mapa) > 0:
            centro_lat = df_mapa['latitude'].mean()
            centro_lon = df_mapa['longitude'].mean()
            
            mapa = folium.Map(location=[centro_lat, centro_lon], zoom_start=12, tiles='OpenStreetMap')
            
            if tipo_mapa == "Puntos por Calidad":
                colores_calidad = {'EXCELENTE': 'green', 'BUENA': 'lightgreen', 
                                  'REGULAR': 'yellow', 'MALA': 'orange', 'CRITICA': 'red'}
                
                for _, row in df_mapa.iterrows():
                    color = colores_calidad.get(row['calidad_senal'], 'gray')
                    folium.CircleMarker(
                        location=[row['latitude'], row['longitude']],
                        radius=3,
                        color=color,
                        fill=True,
                        fillColor=color,
                        fillOpacity=0.6,
                        popup=f"<b>{row['operador_normalizado']}</b><br>"
                              f"Red: {row['red_normalizada']}<br>"
                              f"Señal: {row['medida_senal']:.1f} dBm<br>"
                              f"Calidad: {row['calidad_senal']}"
                    ).add_to(mapa)
            
            elif tipo_mapa == "Mapa de Calor":
                heat_data = [[row['latitude'], row['longitude'], abs(row['medida_senal'])] 
                            for _, row in df_mapa.iterrows()]
                plugins.HeatMap(heat_data, radius=15, blur=25).add_to(mapa)
            
            elif tipo_mapa == "Clusters":
                marker_cluster = plugins.MarkerCluster().add_to(mapa)
                for _, row in df_mapa.iterrows():
                    folium.Marker(
                        location=[row['latitude'], row['longitude']],
                        popup=f"<b>{row['operador_normalizado']}</b><br>"
                              f"Señal: {row['medida_senal']:.1f} dBm<br>"
                              f"Dispositivo: {row['device_name']}",
                        icon=folium.Icon(color='blue', icon='signal')
                    ).add_to(marker_cluster)
            
            folium_static(mapa, width=800, height=600)
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
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        franja_counts = df_filtrado['franja_horaria'].value_counts().reset_index()
        franja_counts.columns = ['Franja', 'Mediciones']
        fig = px.bar(franja_counts, x='Franja', y='Mediciones',
                     color='Franja',
                     title='Mediciones por Franja Horaria')
        st.plotly_chart(fig, use_container_width=True)
    
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
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Patrón Día-Hora")
    heatmap_data = df_filtrado.groupby(['nombre_dia', 'franja_horaria']).size().reset_index(name='count')
    heatmap_pivot = heatmap_data.pivot(index='nombre_dia', columns='franja_horaria', values='count')
    
    fig = px.imshow(heatmap_pivot, 
                    labels=dict(x="Franja Horaria", y="Día", color="Mediciones"),
                    title="Concentración de Mediciones por Día y Hora",
                    color_continuous_scale='Blues')
    st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("Seguimiento de Dispositivo Individual")
    
    dispositivos_list = sorted(df['device_name'].unique())
    dispositivo_seleccionado = st.selectbox("Selecciona un dispositivo para rastrear:", dispositivos_list)
    
    df_device = df[df['device_name'] == dispositivo_seleccionado].sort_values('timestamp')
    
    st.subheader(f"Datos del dispositivo: {dispositivo_seleccionado}")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Mediciones", len(df_device))
    with col2:
        st.metric("Señal Promedio", f"{df_device['medida_senal'].mean():.1f} dBm")
    with col3:
        st.metric("Velocidad Promedio", f"{df_device['medida_velocidad'].mean():.2f} km/h")
    with col4:
        st.metric("Ubicaciones Únicas", df_device['ubicacion_id'].nunique())
    
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
                      f"<b>Calidad:</b> {row['calidad_senal']}<br>"
                      f"<b>Velocidad:</b> {row['medida_velocidad']:.2f} km/h"
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
        
        folium_static(mapa_device, width=900, height=500)
    
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
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Registro Detallado")
    columnas_mostrar = ['timestamp', 'operador_normalizado', 'red_normalizada', 
                       'calidad_senal', 'medida_senal', 'medida_velocidad', 
                       'latitude', 'longitude', 'zona_altitud']
    st.dataframe(df_device[columnas_mostrar].head(100), use_container_width=True)
