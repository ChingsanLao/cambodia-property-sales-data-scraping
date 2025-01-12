import pandas as pd
import datetime

# Read the CSV file
csv_file = "Process/final_cleaned_price_data.csv"
df = pd.read_csv(csv_file)

# Helper function to generate unique IDs
def generate_id(prefix, start_id=1):
    id_counter = start_id
    while True:
        yield f"{prefix}_{id_counter}"
        id_counter += 1

# Initialize ID generators
property_id_gen = generate_id("property")
location_id_gen = generate_id("location")
amenity_id_gen = generate_id("amenity")
time_id_gen = generate_id("date")

# Cache for unique mappings
location_map = {}
time_map = {}
amenity_map = {}

# Prepare data for Dim_Property Table
dim_property_data = []
for _, row in df.iterrows():
    property_id = next(property_id_gen)
    dim_property_data.append({
        "property_id": property_id,
        "property_name": row['property_name'],
        "property_types": row['property_types'],
        "floor_level": row['floor_level'],
        "completion_year": row['completion_year'],
        "facing": row['facing']
    })
dim_property_df = pd.DataFrame(dim_property_data)

# Prepare data for Dim_Location Table
dim_location_data = []
for _, row in df.iterrows():
    location_key = f"{row['address']}_{row['commune']}_{row['district']}_{row['city']}"
    if location_key not in location_map:
        location_id = next(location_id_gen)
        location_map[location_key] = location_id
        dim_location_data.append({
            "location_id": location_id,
            "address": row['address'],
            "commune": row['commune'],
            "district": row['district'],
            "city": row['city']
        })
dim_location_df = pd.DataFrame(dim_location_data)

# Prepare data for Dim_Time Table
dim_time_data = []
for date_column in ['listed_date', 'updated_date']:
    df[date_column] = pd.to_datetime(df[date_column], format="%d/%m/%Y", errors='coerce')

for date_obj in pd.concat([df['listed_date'], df['updated_date']]).dropna().unique():
    date_key = date_obj.date()
    if date_key not in time_map:
        date_id = next(time_id_gen)
        time_map[date_key] = date_id
        dim_time_data.append({
            "date_id": date_id,
            "date": date_key,
            "day_of_week": date_obj.strftime('%A'),
            "month": date_obj.strftime('%B'),
            "quarter": f"Q{(date_obj.month-1)//3 + 1}",
            "year": date_obj.year
        })
dim_time_df = pd.DataFrame(dim_time_data)

# Prepare data for Dim_Amenities Table
dim_amenities_data = []
for i, row in df.iterrows():
    property_id = dim_property_data[i]["property_id"]  # Use the corresponding property_id
    if pd.notna(row['amenities']):
        amenities_list = row['amenities'].split(", ")
        for amenity in amenities_list:
            if amenity not in amenity_map:
                amenity_id = next(amenity_id_gen)
                amenity_map[amenity] = amenity_id
            dim_amenities_data.append({
                "amenity_id": amenity_map[amenity],
                "property_id": property_id,
                "amenity_name": amenity
            })
    else:
        dim_amenities_data.append({
            "amenity_id": "No_amenities",
            "property_id": property_id,
            "amenity_name": "No amenities"
        })
dim_amenities_df = pd.DataFrame(dim_amenities_data)

# Prepare data for Fact_PropertyListings Table
fact_property_listings_data = []
for i, row in df.iterrows():
    property_id = dim_property_data[i]["property_id"]  # Use the corresponding property_id
    price = row['price']
    listed_date_id = time_map.get(row['listed_date'].date(), None) if pd.notna(row['listed_date']) else None
    updated_date_id = time_map.get(row['updated_date'].date(), None) if pd.notna(row['updated_date']) else None
    location_id = location_map.get(f"{row['address']}_{row['commune']}_{row['district']}_{row['city']}", None)

    fact_property_listings_data.append({
        "property_id": property_id,
        "price": price,
        "listed_date": listed_date_id,
        "updated_date": updated_date_id,
        "land_area": row['land_area'],
        "floor_area": row['floor_area'],
        "bedrooms": row['bedrooms'],
        "bathrooms": row['bathrooms'],
        "location_id": location_id
    })
fact_property_listings_df = pd.DataFrame(fact_property_listings_data)

# Save DataFrames to CSV
dim_property_df.to_csv("Dim_Property.csv", index=False)
dim_location_df.to_csv("Dim_Location.csv", index=False)
dim_time_df.to_csv("Dim_Time.csv", index=False)
dim_amenities_df.to_csv("Dim_Amenities.csv", index=False)
fact_property_listings_df.to_csv("Fact_PropertyListings.csv", index=False)

print("Processing completed successfully.")
