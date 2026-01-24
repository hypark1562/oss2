"""
Module: extract.py
Handles Riot API communication and raw data caching with retry logic.
"""
import json
import logging
import os
import time

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

API_KEY = os.getenv("RIOT_API_KEY")
HEADERS = {"X-Riot-Token": API_KEY}


def _save_raw_backup(data):
    os.makedirs("data/raw", exist_ok=True)
    with open("data/raw/challenger_raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def extract_data(retries=3, backoff_factor=2):
    """
    Fetch Challenger league entries via Riot API.
    Implements Exponential Backoff for 429 Rate Limit handling.
    """
    if not API_KEY:
        raise ValueError("Missing RIOT_API_KEY environment variable.")

    target_url = "https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"

    for attempt in range(retries):
        try:
            response = requests.get(target_url, headers=HEADERS, timeout=10)

            if response.status_code == 200:
                payload = response.json()
                _save_raw_backup(payload)
                return payload.get("entries", [])

            elif response.status_code == 429:
                wait_time = backoff_factor**attempt
                logger.warning(f"Rate limit exceeded. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue  # Retry

            else:
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error (Attempt {attempt+1}/{retries}): {e}")
            if attempt == retries - 1:
                raise

    raise Exception("API_RATE_LIMIT_EXCEEDED after max retries")
