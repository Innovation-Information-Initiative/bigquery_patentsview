"""Task for downloading data from PatentsView using task generators."""
from pathlib import Path
from typing import Annotated
from urllib.parse import urljoin
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from pytask import Product, task

from ..config import BLD, DATASETS


def get_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_zip_files_from_url(base_url: str) -> list[str]:
    """Get list of zip file names from PatentsView download page."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    # Add a small delay to be respectful
    time.sleep(2)

    session = get_session()
    response = session.get(base_url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    zip_files = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".zip"):
            # Extract just the filename from the URL/path
            filename = Path(href).name
            zip_files.append((filename, href))  # Store both filename and full URL

    return zip_files


@task(is_generator=True)
def task_download_datasets() -> None:
    """Generate download tasks for each dataset."""
    for dataset_name, dataset_config in DATASETS.items():
        raw_dir = BLD / "raw" / dataset_name
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Discover zip files for this dataset
        zip_files = get_zip_files_from_url(dataset_config["base_url"])

        # Generate a task for each zip file
        for filename, href in zip_files:
            # Use urljoin to handle both relative and absolute URLs
            file_url = urljoin(dataset_config["base_url"], href)
            file_path = raw_dir / filename

            # Capture variables in default arguments to avoid closure issues
            @task
            def download_file(
                url: str = file_url,
                path: Annotated[Path, Product] = file_path,
            ) -> None:
                """Download a single zip file."""
                # Skip if already downloaded
                if path.exists():
                    print(f"Skipping {path} (already exists)")
                    return

                print(f"Downloading {url} to {path}")
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }

                # Add delay between downloads
                time.sleep(1)

                session = get_session()
                with session.get(url, headers=headers, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                print(f"Downloaded {path}")
