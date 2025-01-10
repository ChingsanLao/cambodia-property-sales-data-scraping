import requests
from bs4 import BeautifulSoup
import re  # For date validation
import pandas as pd  # For creating the DataFrame


# URL of the webpage to scrape
base_url = "https://www.realestate.com.kh/buy/"

# Sending a GET request to the webpage
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36"
}

# Function to validate the date format
def validate_date_format(date_text):
    return bool(re.match(r"^\d{2}/\d{2}/\d{4}$", date_text))

# List to store all the scraped data
data = []

# Start with the first page
page_number = 1

while True:
    # Construct the URL for the current page
    if page_number == 1:
        url = base_url  # Use base URL for the first page
    else:
        url = f"{base_url}?page={page_number}"  # Add pagination parameter for other pages

    print(f"Scraping page: {page_number} - URL: {url}")

    # Send a GET request
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code != 200:
        print("Failed to fetch the webpage.")
        break

    # Parse the webpage content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all items inside the 'items-container' class
    items_containers = soup.find_all("div", class_="items-container")
    
    # Stop the loop if no items are found on the current page
    if not items_containers:
        print("No more listings found. Ending scrape.")
        break

    for container in items_containers:
        # Find all individual items within the container
        items = container.find_all("div", class_="item")
        
        for item in items:
            # Extract the heading directly
            heading = item.find("div", class_="heading")
            if heading:
                # Remove nested elements like price, address, etc.
                for nested in heading.find_all(recursive=False):
                    nested.extract()
                
                # Get the cleaned heading text
                head = heading.text.strip()
                
                if head in ("Project", "Borey"):
                    pass
                else:
                    date = item.find_all("span", class_="normal-span")
                    listed_date = date[0].text.strip()
                    updated_date = date[1].text.strip()
                    listed_date_text = listed_date.replace("Listed : ", "")
                    updated_date_text = updated_date.replace("Updated: ", "")
                    # Get the link to the detail page
                    link_element = item.find("a", href=True)
                    if link_element:
                        detail_page_url = link_element["href"]                    
                        detail_page_url = "https://www.realestate.com.kh" + detail_page_url
                        # Send a request to the detail page
                    detail_response = requests.get(detail_page_url, headers=headers)
                    if detail_response.status_code == 200:
                        # Parse the detail page content
                        detail_soup = BeautifulSoup(detail_response.content, "html.parser")
                        
                        # Extract the property name
                        h1_headline = detail_soup.find("h1", class_="headline alone")
                        property_name = h1_headline.text.strip() if h1_headline else "N/A"



                        # location
                        location_div = detail_soup.find("div", class_="sections")
                        # Default value for location
                        location_text = "N/A"
                        if location_div:
                            h2_element = location_div.find("h2")
                            if h2_element:
                                # Check if the <h2> contains an <a> tag
                                link_element = h2_element.find("a", href=True)
                                if link_element:
                                    # go to the link
                                    location_link = link_element["href"]
                                    location_link = "https://www.realestate.com.kh" + location_link
                                    location_response = requests.get(location_link, headers=headers)
                                    if location_response.status_code == 200:
                                        location_soup = BeautifulSoup(location_response.content, "html.parser")
                                        location_div_detail = location_soup.find("div", class_="sections")
                                        if location_div_detail:  # Check if location_div_detail exists
                                            h2_element_detail = location_div_detail.find("h2")
                                            location_text = h2_element_detail.text.strip().replace("\n", "").replace("  ", "") if h2_element_detail else "N/A"
                                        else:
                                            location_text = "N/A"
                                        # close the connection
                                        location_response.close()
                                else:
                                    # Otherwise, extract the plain text
                                    location_text = h2_element.text.strip()

                        
                        # Extract icon containers
                        icon_containers = detail_soup.find_all("div", class_="icon-container")

                        # Extract data
                        unit_types = icon_containers[0].find_next_sibling("div")
                        unit_type_text = unit_types.find("span", class_="value").text.strip() if unit_types else "N/A"

                        # Extract values and labels (Bedrooms, Bathrooms, etc.)
                        bedrooms = bathrooms = land_area = floor_area = floor_level = facing = "N/A"
                        value_label_container = detail_soup.find("div", class_="css-r7o7s2 elr7wbp0")
                        if value_label_container:
                            features = value_label_container.find_all("div")
                            for feature in features:
                                value = feature.find("span", class_="value").text.strip() if feature.find("span", class_="value") else "N/A"
                                label = feature.find("span", class_="text").text.strip() if feature.find("span", class_="text") else "N/A"
                                if "Bedroom" in label:
                                    bedrooms = value
                                elif "Bathroom" in label:
                                    bathrooms = value
                                elif "Land Area" in label:
                                    land_area = value
                                elif "Floor Area" in label:
                                    floor_area = value
                                elif "Floor Level" in label:
                                    floor_level = value
                                elif "Facing" in label:
                                    facing = value
                                elif "Completion Year" in label:
                                    completion_year = value

                        # price 
                        price = detail_soup.find("div", class_="actual-price")
                        price_text = price.text.strip() if price else "N/A"
                        

                        if price_text == "N/A":
                            price = detail_soup.find("span", class_="price-value")
                            price_text = price.text.strip() if price else "N/A"

                        # Process the location
                        if location_text != "N/A":
                            # Split the location into parts
                            location_parts = location_text.split(", ")
                            # If there are 4 parts, remove the first one
                            if len(location_parts) == 4:
                                location_parts.pop(0)
                            # Map parts to commune, district, city
                            commune = location_parts[0] if len(location_parts) > 0 else "N/A"
                            district = location_parts[1] if len(location_parts) > 1 else "N/A"
                            city = location_parts[2] if len(location_parts) > 2 else "N/A"
                        else:
                            commune, district, city = "N/A", "N/A", "N/A"

                        # Extract Property ID and its value
                        property_id = "N/A"  # Default value
                        property_id_element = detail_soup.find("span", string=lambda text: text and "Property ID" in text)
                        if property_id_element:
                            property_id_value_element = property_id_element.find_next_sibling("span", class_="value")
                            property_id = property_id_value_element.text.strip() if property_id_value_element else "N/A"


                        # Initialize a list to store the amenities
                        amenities = []

                        # Find all features-block divs
                        features_blocks = detail_soup.find_all("div", class_="features-block")

                        for block in features_blocks:
                            # Check if there's an <h2> with the text "Amenities"
                            h2_element = block.find("h2", string="Amenities")
                            if h2_element:
                                # Find all divs with the class "highlighted" inside this block
                                highlighted_blocks = block.find_all("div", class_="highlighted")
                                for highlighted in highlighted_blocks:
                                    # Find all span elements inside the highlighted block
                                    spans = highlighted.find_all("span", recursive=False)
                                    for span in spans:
                                        # Extract and strip the text
                                        amenity = span.text.strip()
                                        amenities.append(amenity)

                        # Initialize a list to store the cleaned amenities
                        cleaned_amenities = []

                        # Regex to match only readable text (ignoring Unicode characters or icons)
                        clean_regex = re.compile(r"[^\u0020-\u007E]+")

                        for amenity in amenities:
                            # Remove non-ASCII characters and trim whitespace
                            cleaned_amenity = clean_regex.sub("", amenity).strip()
                            if cleaned_amenity:  # Only add the cleaned value if it's not empty
                                cleaned_amenities.append(cleaned_amenity)

                        cleaned_amenities_str = ", ".join(cleaned_amenities)
                    
                        # Append the data to the list
                        data.append({
                            "property_id": property_id,
                            "property_name": property_name,
                            "property_types": unit_type_text,
                            "listed_date": listed_date_text,
                            "updated_date": updated_date_text,
                            "bedrooms": bedrooms,
                            "bathrooms": bathrooms,
                            "land_area": land_area,
                            "floor_area": floor_area,
                            "floor_level": floor_level,
                            "completion_year": completion_year,
                            "facing": facing,
                            "price": price_text,
                            "address": location_text,
                            "commune": commune,
                            "district": district,
                            "city": city,
                            "amenities": cleaned_amenities_str,
                        })
                    # close the connection
                    detail_response.close()
                    print("processing")
                    # After processing the data, save to CSV
                    df = pd.DataFrame(data)
                    df.to_csv("realestate.csv", index=False)  # Overwrite or append to the CSV
                    print("Data saved to CSV")
    # Increment the page number
    page_number += 1