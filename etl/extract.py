import json
import logging
import os

import requests
from dotenv import load_dotenv

from utils.config import config

logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")


def get_challenger_league() -> bool:
    """
    Fetch Challenger League data from Riot API and save as Raw Data.

    Process:
        1. Request data from Riot API (League-V4).
        2. Handle HTTP errors (429 Rate Limit, 403 Unauthorized).
        3. Save the response payload to the local file system (Raw Layer).

    Returns:
        bool: True if extraction and saving are successful, False otherwise.
    """
    base_url = config["api"]["challenger_url"]
    # Construct URL with API Key
    request_url = f"{base_url}?api_key={API_KEY}"
    save_path = config["path"]["raw_data"]

    try:
        logger.info("[Extract] Requesting data from Riot API...")
        response = requests.get(request_url)

        # Success Case (200 OK)
        if response.status_code == 200:
            data = response.json()

            # Ensure the directory exists before saving
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            logger.info(f"[Extract] Raw data saved successfully: {save_path}")
            return True

        # Error Handling
        else:
            logger.error(f"[Extract] API Request Failed: Status {response.status_code}")

            if response.status_code == 429:
                logger.warning(
                    "[Extract] Rate Limit Exceeded. Retry suggested after wait time."
                )
            elif response.status_code == 403:
                logger.critical(
                    "[Extract] Unauthorized. Check RIOT_API_KEY expiration."
                )

            return False

    except Exception as e:
        logger.exception(f"[Extract] Unexpected System Error: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    get_challenger_league()
