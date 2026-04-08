import pandas as pd
import folium 
from folium.plugins import HeatMap

file_path = r"C:\Users\User\Desktop\data_analytics\airflow-project\data\heatmap_final.csv"
cleaned_file_path = r"C:\Users\User\Desktop\data_analytics\airflow-project\data\cleaned_heatmap_final.csv"
heatmap_output = r"C:\Users\User\Desktop\data_analytics\airflow-project\data\heatmap.html"

df  = pd.read_csv(file_path);

#rint(df.head())
#print (df.columns)

df = df.dropna(subset=["t_area_id","member_count","Lat","Lon"])

df["Lon"] = (
    df["Lon"]
    .astype(str)
    .str.replace("'", "", regex=False)   # remove '
    .str.strip()
)

df['Lat']= (
    df["Lat"]
    .astype(str)
    .str.replace("'","",regex=False)
    .str.strip()
)

df['Lat'] = pd.to_numeric(df['Lat'],errors="coerce")

df['Lon'] = pd.to_numeric(df['Lon'],errors="coerce")

df['member_count'] = pd.to_numeric(df['member_count'],errors="coerce")

heat_datas = df[["Lat","Lon","member_count"]].values.tolist()

m = folium.Map(location=[4.5,109.5],zoom_start=6)

HeatMap(heat_datas).add_to(m)

for _, row in df.iterrows():
    
    folium.CircleMarker(
        location=[row["Lat"],row['Lon']],
        radius=5,
        popup=f"{row['t_area_id']}<br> Members: {row['member_count']}",
        color="blue",
        fill=True
).add_to(m)

output_map = heatmap_output
m.save(output_map)

print(f"Heatmap saved to :{heatmap_output}")


#df.to_csv(cleaned_file_path)

#print (f" Saved to {cleaned_file_path} successfully")