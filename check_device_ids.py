import pandas as pd

df = pd.read_csv('locations_rows.csv')

df_devices = df[['device_id', 'device_name']].drop_duplicates().dropna(subset=['device_id'])

result = df_devices.groupby('device_name')['device_id'].apply(list).reset_index()
result['num_ids'] = result['device_id'].apply(len)

duplicados = result[result['num_ids'] > 1]

print("Dispositivos con múltiples device_id:")
if len(duplicados) > 0:
    for _, row in duplicados.iterrows():
        print(f"\n{row['device_name']}: {row['device_id']}")
else:
    print("No hay dispositivos con múltiples device_id")

print("\n\n" + "=" * 60)
print(f"Total dispositivos únicos (device_name): {df['device_name'].nunique()}")
print(f"Total device_id únicos (no NaN): {df['device_id'].dropna().nunique()}")
print(f"Total registros con device_id no NaN: {df['device_id'].notna().sum():,}")
print(f"Total registros: {len(df):,}")

print("\n\nTodos los device_id y sus dispositivos:")
print("=" * 60)
print(df_devices.sort_values('device_id').to_string(index=False))
