from matplotlib import pyplot as plt
import geopandas as gpd
import pandas as pd

gdf = gpd.read_file("../resources/boundaries/conflict_regions/karabach/karabach.gpkg")
str1 = str(gdf.geometry.union_all())

str1 = str1.replace(")), ((", '|')
str1 = str1.replace(" ", ",")
str1 = str1.replace(",,", ",")
str1 = str1.replace("MULTIPOLYGON,((", "")
str1 = str1.replace("))", "")
print(str1)
with open("output.txt", 'w') as file:
    for line in str1.split("|"):  # Assuming '|' is your separator
        file.write(line.strip() + "\n\n")  # Double newline for separation

