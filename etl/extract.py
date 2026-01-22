import os
import requests
import json
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
HEADERS = {"X-Riot-Token": API_KEY}

def _save_raw_backup(data):
    os.makedirs("data/raw", exist_ok=True)
    with open("data/raw/challenger_raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


"""
Module: extract.py
Handles Riot API communication and raw data caching.
"""

def extract_data():
    """
    Fetch Challenger league entries via Riot API.
    Implemented with backup logic to ensure data traceability.
    """
    if not API_KEY:
        raise ValueError("Missing RIOT_API_KEY environment variable.")

    # Endpoint: Ranked Solo 5x5 Challenger League
    target_url = "https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
    
    try:
        # HTTP GET with 10s timeout to prevent thread hanging
        response = requests.get(target_url, headers=HEADERS, timeout=10)
        
        # Handle HTTP status codes based on Riot API policy
        if response.status_code == 200:
            payload = response.json()
            _save_raw_backup(payload)  # Archive raw response for backfill
            return payload.get("entries", [])
            
        elif response.status_code == 429:
            logger.warning("Rate limit exceeded. Check developer portal.")
            raise Exception("API_RATE_LIMIT_EXCEEDED")
            
        else:
            response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during extraction: {e}")
        raise