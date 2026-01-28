"""Task for collecting metadata about patent tables."""
import pytask
import json
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Dict, List
import os

from ..config import DATASETS, BLD

# Get dataset from env var or default to 'granted'
DATASET = os.getenv("CONFIG_TYPE", "granted")
metadata_dir = BLD / "metadata" / DATASET
base_url = DATASETS[DATASET]["base_url"]
dict_url = DATASETS[DATASET]["dict_url"]

def fetch_table_descriptions(url: str) -> Dict[str, str]:
    """Fetch table descriptions from PatentsView website."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    descriptions = {}
    
    for table in soup.find_all("tr"):
        cells = table.find_all("td")
        if len(cells) >= 2:
            table_name = cells[0].text.strip()
            description = cells[1].text.strip()
            descriptions[table_name] = description
            
    return descriptions

def fetch_variable_descriptions(url: str) -> Dict[str, List[Dict]]:
    """Fetch variable descriptions from PatentsView website."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    variable_descriptions = {}
    
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        dataset_name = None
        variables = []
        
        for row in rows:
            cells = row.find_all("td")
            
            if 'table-head' in row.get('class', []):
                if dataset_name and variables:
                    variable_descriptions[dataset_name] = variables
                    variables = []
                dataset_name = cells[0].get_text(strip=True)
            elif len(cells) >= 2 and dataset_name:
                variable_name = cells[0].get_text(strip=True)
                description = cells[1].get_text(strip=True)
                
                if variable_name.lower() == "data element name" and description.lower() == "definition":
                    continue
                    
                variables.append({
                    "variable_name": variable_name,
                    "description": description
                })
                
        if dataset_name and variables:
            variable_descriptions[dataset_name] = variables
            
    return variable_descriptions

@pytask.mark.persist()
def task_collect_metadata(
    produces={
        "tables": metadata_dir / "table_descriptions.json",
        "variables": metadata_dir / "variable_descriptions.json",
    }
):
    """Collect table and variable descriptions."""
    metadata_dir.mkdir(parents=True, exist_ok=True)

    # Fetch descriptions
    table_descriptions = fetch_table_descriptions(base_url)
    variable_descriptions = fetch_variable_descriptions(dict_url)
    
    # Save to files
    with open(produces["tables"], 'w') as f:
        json.dump(table_descriptions, f, indent=2)
    
    with open(produces["variables"], 'w') as f:
        json.dump(variable_descriptions, f, indent=2)
