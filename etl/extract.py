"""
Module: etl.extract
Description:
    Handles data ingestion from the Riot Games API.
    Implements reliability patterns including Exponential Backoff for rate limiting.
"""
import json
import logging
import os
import time
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

API_KEY = os.getenv("RIOT_API_KEY")
HEADERS = {"X-Riot-Token": API_KEY}


def _save_raw_backup(data: Dict[str, Any]) -> None:
    """
    Persists raw API response to local storage for debugging and backfill purposes.

    Args:
        data (dict): The raw JSON payload received from the API.
    """
    os.makedirs("data/raw", exist_ok=True)
    with open("data/raw/challenger_raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def extract_data(retries: int = 3, backoff_factor: int = 2) -> List[Dict[str, Any]]:
    """
    Fetches Challenger League data from Riot API with fault tolerance.

    Implements an exponential backoff strategy to handle HTTP 429 (Rate Limit)
    and transient network errors.

    Args:
        retries (int): Maximum number of retry attempts. Defaults to 3.
        backoff_factor (int): Multiplier for sleep time between retries. Defaults to 2.

    Returns:
        List[Dict[str, Any]]: A list of player entries (dictionaries).

    Raises:
        ValueError: If RIOT_API_KEY is missing.
        Exception: If API rate limit is exceeded after max retries.
    """
    if not API_KEY:
        logger.critical("[Config] Missing RIOT_API_KEY environment variable.")
        raise ValueError("API_KEY_MISSING")

    target_url = "https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"

    for attempt in range(retries):
        try:
            # [Network] HTTP GET request with explicit timeout to prevent hanging
            response = requests.get(target_url, headers=HEADERS, timeout=10)

            if response.status_code == 200:
                payload = response.json()
                _save_raw_backup(payload)
                logger.info(
                    f"[Extract] Successfully fetched {len(payload.get('entries', []))} records."
                )
                return payload.get("entries", [])

            elif response.status_code == 429:
                # [Rate Limit] Apply Exponential Backoff (e.g., 2s -> 4s -> 8s)
                wait_time = backoff_factor**attempt
                logger.warning(
                    f"[Extract] Rate limit hit. Retrying in {wait_time}s (Attempt {attempt+1}/{retries})."
                )
                time.sleep(wait_time)
                continue

            else:
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(
                f"[Extract] Connection error (Attempt {attempt+1}/{retries}): {e}"
            )
            if attempt == retries - 1:
                raise

    logger.error("[Extract] Max retries exceeded.")
    raise Exception("API_RATE_LIMIT_EXCEEDED")
