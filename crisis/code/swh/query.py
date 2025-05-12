import json
import requests
from tqdm import tqdm
import time

# Utility to pretty-print json
def jprint(obj):
    print(json.dumps(obj, sort_keys=True, indent=4))

base_url = "https://archive.softwareheritage.org/api/1/"
university_keywords = ["unibo", "alma mater", "alma mater sudiorum", "university of bologna", "almamaterstudiorum", "università di bologna", "universita di bologna"]  # Keywords to search for origins

found_origins = []

print("Searching for origins related to the University of Bologna...")

for keyword in tqdm(university_keywords, desc="Searching for keywords"):
    search_url = f"{base_url}origin/search/{keyword}/"
    try:
        resp = requests.get(search_url)
        resp.raise_for_status()  # Raise an exception for bad status codes
        origins = resp.json()
        print(f"\nFound {len(origins)} entries for keyword: '{keyword}'")
        for origin in origins:
            if origin not in found_origins:
                found_origins.append(origin)
                print(f"- URL: {origin['url']}")
        time.sleep(2)  # Be polite to the API
    except requests.exceptions.RequestException as e:
        print(f"Error during search for '{keyword}': {e}")

print("\n" + "=" * 100)
print(f"{' ' * 30}TOTAL UNIQUE ORIGINS FOUND: --{len(found_origins)}--")
print("=" * 100 + "\n")


#Fetch visit information for the first few found origins
# if found_origins:
#     print("\nFetching visit information for the first few found origins:")
#     for i, origin in enumerate(found_origins[:3]):  # Limit to first 3 for brevity
#         origin_url = origin['url']
#         visits_url = f"{base_url}origin/{requests.utils.quote(origin_url)}/visits/"
#         print(f"\n--- Visits for: {origin_url} ---")
#         try:
#             resp_visits = requests.get(visits_url)
#             resp_visits.raise_for_status()
#             visits = resp_visits.json()
#             print(f"Number of visits: {len(visits)}")
#             if visits:
#                 print("Example of the latest visit:")
#                 jprint(visits[0])
#             else:
#                 print("No visits recorded.")
#         except requests.exceptions.RequestException as e:
#             print(f"Error fetching visits for {origin_url}: {e}")
# else:
#     print("\nNo origins found matching the keywords.")