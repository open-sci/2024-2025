import json
import requests
from tqdm import tqdm
import time
# This script searches for origins related to the University of Bologna in the Software Heritage archive.
# Problem: 429 error (Too Many Requests) 

def jprint(obj):
    print(json.dumps(obj, sort_keys=True, indent=4))

base_url = "https://archive.softwareheritage.org/api/1/"
university_keywords = ["unibo", "alma mater", "alma mater sudiorum", "university of bologna", "almamaterstudiorum", "università di bologna", "universita di bologna"]
potential_unibo_content = []

print("Searching for origins related to the University of Bologna...")

for keyword in tqdm(university_keywords, desc="Searching keywords"):
    search_url = f"{base_url}origin/search/{requests.utils.quote(keyword)}/"
    try:
        print(f"Searching with keyword: '{keyword}'")
        resp = requests.get(search_url)
        resp.raise_for_status()
        origins = resp.json()
        print(f"Found {len(origins)} entries for keyword: '{keyword}'")
        for origin in origins:
            if not any(item['origin_url'] == origin['url'] for item in potential_unibo_content):
                potential_unibo_content.append({'origin_url': origin['url'], 'revisions_with_unibo_email': []})
                print(f"- URL: {origin['url']}")
        time.sleep(2) # Add a n-second delay after each search
    except requests.exceptions.RequestException as e:
        print(f"Error during origin search for '{keyword}': {e}")

print(f"\nPotentially relevant origins found: {len(potential_unibo_content)}")

for item in potential_unibo_content:
    origin_url = item['origin_url']
    print(f"\n--- Processing origin: {origin_url} to check revisions ---")
    latest_visit_url = f"{base_url}origin/{requests.utils.quote(origin_url)}/visit/latest/"
    try:
        resp_latest_visit = requests.get(latest_visit_url)
        resp_latest_visit.raise_for_status()
        latest_visit = resp_latest_visit.json()
        snapshot_id = latest_visit.get('snapshot')
        if snapshot_id:
            snapshot_url = f"{base_url}snapshot/{snapshot_id}/"
            resp_snapshot = requests.get(snapshot_url)
            resp_snapshot.raise_for_status()
            snapshot_details = resp_snapshot.json()
            branches = snapshot_details.get('branches', {})
            for branch_name, branch_info in branches.items():
                if branch_info.get('target_type') == 'revision':
                    revision_url = branch_info['target_url']
                    try:
                        revision_resp = requests.get(revision_url)
                        revision_resp.raise_for_status()
                        revision_details = revision_resp.json()
                        committer = revision_details.get('committer')
                        committer_email = committer.get('email', '') if committer else ''
                        author = revision_details.get('author')
                        author_email = author.get('email', '') if author else ''

                        if 'unibo.it' in committer_email or 'unibo.it' in author_email:
                            print(f"  Found revision on branch '{branch_name}' with unibo email:")
                            jprint({'revision_id': revision_details.get('id'),
                                    'committer': committer,
                                    'author': author})
                            item['revisions_with_unibo_email'].append(revision_details.get('id'))
                        time.sleep(2) # Delay for revision requests

                    except requests.exceptions.RequestException as e:
                        print(f"    Error fetching revision details: {e}")
                        time.sleep(2)
        else:
            print("  No snapshot found for the latest visit.")
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching latest visit for {origin_url}: {e}")
    time.sleep(2) # Delay after processing each origin

print("\n--- Summary of potentially related content ---")
for item in tqdm(potential_unibo_content, desc="Processing origins"):
    print(f"Origin: {item['origin_url']}")
    if item['revisions_with_unibo_email']:
        print(f"  Found {len(item['revisions_with_unibo_email'])} revisions with '@unibo.it' email addresses.")
    else:
        print("  No revisions found with '@unibo.it' email addresses (in the checked branches).")