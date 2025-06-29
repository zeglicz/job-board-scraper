import sqlite3
import json
import logging
from pathlib import Path
from typing import List, Dict, Union
from datetime import datetime

logger = logging.getLogger(__name__)


class JobOfferDatabase:
    def __init__(self, db_path: Union[str, Path], table_name: str = "offers"):
        self.db_path = Path(db_path)
        self.table_name = table_name
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._create_table()

    @staticmethod
    def to_bool_or_none(val):
        if val is None:
            return None
        return bool(val)

    @staticmethod
    def clean_str(val):
        if isinstance(val, str):
            return val.strip()
        return val

    def _create_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS """
                + self.table_name
                + """ (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT,
                title TEXT,
                requiredSkills TEXT,
                niceToHaveSkills TEXT,
                workplaceType TEXT,
                workingTime TEXT,
                experienceLevel TEXT,
                employmentTypes TEXT,
                categoryId INTEGER,
                multilocation TEXT,
                city TEXT,
                street TEXT,
                latitude TEXT,
                longitude TEXT,
                remoteInterview BOOLEAN,
                companyName TEXT,
                companyLogoThumbUrl TEXT,
                publishedAt TEXT,
                openToHireUkrainians BOOLEAN,
                languages TEXT,
                date_fetched TEXT
            )
            """
            )

    def insert_offer(self, offer: Dict, date_fetched: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
            INSERT INTO """
                + self.table_name
                + """ (
                slug, title, requiredSkills, niceToHaveSkills, workplaceType,
                workingTime, experienceLevel, employmentTypes, categoryId, multilocation,
                city, street, latitude, longitude, remoteInterview,
                companyName, companyLogoThumbUrl, publishedAt, openToHireUkrainians,
                languages, date_fetched
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.clean_str(offer.get("slug")),
                    self.clean_str(offer.get("title")),
                    json.dumps(offer.get("requiredSkills")),
                    json.dumps(offer.get("niceToHaveSkills")),
                    self.clean_str(offer.get("workplaceType")),
                    self.clean_str(offer.get("workingTime")),
                    self.clean_str(offer.get("experienceLevel")),
                    json.dumps(offer.get("employmentTypes")),
                    offer.get("categoryId"),
                    json.dumps(offer.get("multilocation")),
                    self.clean_str(offer.get("city")),
                    self.clean_str(offer.get("street")),
                    str(offer.get("latitude")),
                    str(offer.get("longitude")),
                    self.to_bool_or_none(offer.get("remoteInterview")),
                    self.clean_str(offer.get("companyName")),
                    self.clean_str(offer.get("companyLogoThumbUrl")),
                    self.clean_str(offer.get("publishedAt")),
                    self.to_bool_or_none(offer.get("openToHireUkrainians")),
                    json.dumps(offer.get("languages")),
                    date_fetched,
                ),
            )

    def insert_offers(self, offers: List[Dict]):
        date_fetched = datetime.now().strftime("%Y-%m-%d")
        for offer in offers:
            self.insert_offer(offer, date_fetched)
