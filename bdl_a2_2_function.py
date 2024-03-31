import apache_beam as beam
import numpy as np
import os
import apache_beam as beam
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from bdl_a2_1 import YYYY

zip_file_name = f"ncei_data_{YYYY}.zip"
data_dir = "/home/parallels/Downloads/BDl/dags"
required_fields = ['HourlyAltimeterSetting', 'HourlyDewPointTemperature', 'HourlyDryBulbTemperature', 'HourlyPrecipitation', 'HourlyPresentWeatherType', 'HourlyPressureChange', 'HourlyPressureTendency', 'HourlyRelativeHumidity', 'HourlySkyConditions', 'HourlySeaLevelPressure', 'HourlyStationPressure', 'HourlyVisibility', 'HourlyWetBulbTemperature', 'HourlyWindDirection', 'HourlyWindGustSpeed', 'HourlyWindSpeed']

# Define Beam functions (replace placeholder functions)
class ReadAndCleanCSVs(beam.DoFn):
    def process(self, file_path):
        """
        Extracts required fields from the CSV and filters the dataframe.
        Returns a tuple of the form <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>.
        """
        print(file_path)
        data = pd.read_csv(file_path,low_memory=False)
        # Filter the dataframe based on required fields
        required_fields = [col for col in data.columns if 'Hourly' in col]
        filtered_df = data[["DATE","LATITUDE", "LONGITUDE"] + required_fields]

        # Getting month and year from data column
        filtered_df['Month'] = filtered_df['DATE'].apply( lambda x: int(x.split('-')[1]) )  
        filtered_df['Year'] = filtered_df['DATE'].apply( lambda x: int(x.split('-')[0]) )
        filtered_df.drop('DATE',axis=1,inplace=True)
        filtered_df = filtered_df[['Month', 'Year']+list(filtered_df.columns)[:-2]]
        
        tuples_list =  [(tuple(row[0:4]), tuple(row[4:])) for row in filtered_df.itertuples(index=False, name=None)]
        return tuples_list

class CalculateMean(beam.DoFn):
    def process(self, element, **kwargs):
        # Unpack key and values from the element
        group_key, group_values = element
        
        # Convert values to numeric, using np.nan for any conversion failures
        numeric_values = []
        for value_tuple in group_values:
            # Convert each item in the value tuple to float or np.nan
            numeric_row = []
            for item in value_tuple:
                try:
                    numeric_row.append(float(item))
                except ValueError:
                    numeric_row.append(np.nan)  # Use np.nan for non-numeric values
            numeric_values.append(numeric_row)
        
        # Calculate mean while ignoring np.nan values
        mean_values = np.nanmean(numeric_values, axis=0)
        # Ensure mean_values is a list or tuple before concatenation
        mean_list = list(mean_values)
        # Return the calculated mean values as a tuple with the original key
        return [(group_key, tuple(mean_list))]

def format_csv(element):
    key, mean_values = element
    return ','.join(map(str, key + tuple(mean_values)))

def beam_pipeline(data_cols, output_path):
    files = os.listdir(data_dir+'/csv_files')
    files = [data_dir+'/csv_files/'+file for file in files if file.endswith('.csv')]
    with beam.Pipeline() as pipeline:
        records = (
            pipeline
            | 'Create File Paths' >> beam.Create(files)
            | 'Read and Clean CSVs' >> beam.ParDo(ReadAndCleanCSVs())
        )

        grouped_records = (
            records
            | 'Group by Key' >> beam.GroupByKey()
            | 'Compute Mean by Key' >> beam.ParDo(CalculateMean())
        )

        header = ','.join(['Month', 'Year','LATITUDE','LONGITUDE']+data_cols)
        
        output = (
            grouped_records
            | 'Format CSV' >> beam.Map(format_csv)
            | 'Write to CSV' >> beam.io.WriteToText(output_path, file_name_suffix='.csv', shard_name_template='',
                                                    header=header)
        )

def make_plots_by_field(gdf, field):
    grouped_data = gdf.groupby(['LATITUDE','LONGITUDE','Month','Year'])[field].mean().reset_index()

    # Create a folder for saving visualizations for each field
    output_folder_field = os.path.join('vizs', field)
    os.makedirs(output_folder_field, exist_ok=True)

    for index, group in grouped_data.groupby(['Month', 'Year']):
        month, year = index
        month_year = f"{month:02d}_{year}"

        _, ax = plt.subplots(1, 1, figsize=(12, 10))
        group.plot(ax=ax, kind='scatter', x='LONGITUDE', y='LATITUDE', c=field, cmap='YlOrRd', legend=True,
                     s=50, edgecolor='black', linewidth=0.5)
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        world.plot(ax=ax, color='lightgray', edgecolor='black')
        ax.set_title(f'Heatmap: {field} - {month_year}')
        ax.set_axis_off()

        output_folder_year = os.path.join(output_folder_field, str(year))
        os.makedirs(output_folder_year, exist_ok=True)

        # Save the plot in the specified folder structure
        output_file_path = os.path.join(output_folder_year, f"Month-{month:02d}.png")

        plt.savefig(output_file_path, bbox_inches='tight')
        plt.close()

def geopandas_visualisation(heatmap_fields):
    df = pd.read_csv('output/processed.csv')

    # Make sure 'LATITUDE' and 'LONGITUDE' columns are in numeric format
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')

    # Create a GeoDataFrame from the DataFrame
    geometry = [Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Group by MONTH and YEAR and create plots
    for field in heatmap_fields:
        make_plots_by_field(gdf, field)