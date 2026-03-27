import datetime
import os
import random
from dataclasses import dataclass
from typing import Optional

import musicbrainz.db_musicbrainz as db_musicbrainz
from shared.db import context_manager, increment_play_stats, set_last_cleaned
from shared.utils import humanize_date_delta, parse_and_humanize_date


@dataclass
class CollectionFilters:
    artist: str = ""
    title: str = ""
    storage_format: Optional[str] = None
    year_from: str = ""
    year_to: str = ""


@dataclass
class AnniversaryFilters:
    month: int
    day: int


class GrooveKraftService:
    def __init__(self, cfg):
        self.cfg = cfg

    @staticmethod
    def storage_format_case_sql(alias="d"):
        normalized = f"LOWER(REPLACE(REPLACE({alias}.format, '”', '\"'), '“', '\"'))"
        return (
            "CASE "
            f"WHEN {normalized} LIKE '%cd%' THEN 'Compact Disc' "
            f"WHEN {normalized} LIKE '%box set%' AND {normalized} LIKE '%vinyl%' THEN '12\" vinyl' "
            f"WHEN {normalized} LIKE '%box set%' THEN 'Compact Disc' "
            f"WHEN {normalized} LIKE '%vinyl%' AND ({normalized} LIKE '%7\"%' OR {normalized} LIKE '%7 inch%') THEN '7\" vinyl' "
            f"WHEN {normalized} LIKE '%vinyl%' THEN '12\" vinyl' "
            "ELSE NULL END"
        )

    def get_release_detail(self, discogs_id: int):
        with context_manager(self.cfg.db_path) as cur:
            cur.execute(
                """
                SELECT artist, title, format, country, release_date, release_date_locked, discogs_id,
                       catnos, barcodes, play_count, last_played, clean_count, last_cleaned
                FROM discogs_releases
                WHERE discogs_id = ?
                """,
                (discogs_id,),
            )
            release = cur.fetchone()

        if not release:
            return None

        mb_row = db_musicbrainz.fetch_row(self.cfg.db_path, discogs_id=discogs_id)
        matched = bool(mb_row and mb_row.mbid)
        score = mb_row.score if mb_row and mb_row.score is not None else 0
        locked = bool(getattr(release, "release_date_locked", 0))
        image_path = os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")

        return {
            "artist": release.artist,
            "title": release.title,
            "format": release.format,
            "country": release.country,
            "release_date": release.release_date,
            "release_date_human": parse_and_humanize_date(release.release_date),
            "release_date_delta": humanize_date_delta(release.release_date),
            "release_date_locked": locked,
            "discogs_id": release.discogs_id,
            "catnos": release.catnos,
            "barcodes": release.barcodes,
            "matched": matched,
            "match_score": score,
            "match_stars": score_stars(score),
            "clean_count": getattr(release, "clean_count", 0) or 0,
            "last_cleaned": getattr(release, "last_cleaned", None),
            "play_count": getattr(release, "play_count", 0) or 0,
            "last_played": getattr(release, "last_played", None),
            "image_path": image_path,
            "image_exists": os.path.exists(image_path),
        }

    def list_collection(self, filters: Optional[CollectionFilters] = None):
        filters = filters or CollectionFilters()
        storage_format_case = self.storage_format_case_sql("d")
        query = [
            "SELECT d.sort_name, d.artist, d.title, d.format, d.country, d.release_date,",
            "d.release_date_locked, d.discogs_id, m.mbid",
            "FROM discogs_releases d",
            "LEFT JOIN mb_matches m USING(discogs_id)",
        ]
        where_clauses = []
        params = []

        if filters.artist:
            where_clauses.append("d.sort_name LIKE ?")
            params.append(f"%{filters.artist}%")
        if filters.title:
            where_clauses.append("d.title LIKE ?")
            params.append(f"%{filters.title}%")
        if filters.storage_format:
            where_clauses.append(f"{storage_format_case} = ?")
            params.append(filters.storage_format)
        if filters.year_from:
            where_clauses.append("substr(d.release_date, 1, 4) >= ?")
            params.append(filters.year_from)
        if filters.year_to:
            where_clauses.append("substr(d.release_date, 1, 4) <= ?")
            params.append(filters.year_to)

        if where_clauses:
            query.append("WHERE " + " AND ".join(where_clauses))

        query.append("ORDER BY d.sort_name, d.release_date, d.title, d.discogs_id")

        with context_manager(self.cfg.db_path) as cur:
            cur.execute(" ".join(query), params)
            rows = cur.fetchall()

        output = []
        for row in rows:
            sort_name, artist, title, format_name, country, release_date, release_date_locked, discogs_id, mbid = row
            score = 0
            if mbid:
                mb_row = db_musicbrainz.fetch_row(self.cfg.db_path, discogs_id=discogs_id)
                score = mb_row.score if mb_row and mb_row.score is not None else 0
            output.append(
                {
                    "sort_name": sort_name,
                    "artist": artist,
                    "title": title,
                    "format": format_name,
                    "country": country,
                    "release_date": release_date,
                    "release_date_human": parse_and_humanize_date(release_date),
                    "release_date_delta": humanize_date_delta(release_date),
                    "release_date_locked": bool(release_date_locked) if release_date_locked is not None else False,
                    "discogs_id": discogs_id,
                    "matched": bool(mbid),
                    "match_score": score,
                    "match_stars": score_stars(score),
                    "image_exists": os.path.exists(os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")),
                }
            )
        return output

    def list_on_this_day(self, month: Optional[int] = None, day: Optional[int] = None):
        today = datetime.date.today()
        month = month or today.month
        day = day or today.day

        with context_manager(self.cfg.db_path, namedtuple=False) as cur:
            cur.execute(
                """
                SELECT d.sort_name AS artist, d.title, d.format, d.country, d.release_date, d.discogs_id
                FROM discogs_releases d
                WHERE d.release_date IS NOT NULL
                ORDER BY length(d.release_date) DESC, d.release_date, d.sort_name, d.title, d.discogs_id
                """
            )
            items = cur.fetchall()

        rows = []
        for item in items:
            release_date = item["release_date"]
            if not self._matches_month_day(release_date, month, day):
                continue
            discogs_id = item["discogs_id"]
            mb_row = db_musicbrainz.fetch_row(self.cfg.db_path, discogs_id=discogs_id)
            score = mb_row.score if mb_row and mb_row.score is not None else 0
            rows.append(
                {
                    "artist": item["artist"],
                    "title": item["title"],
                    "format": item["format"],
                    "country": item["country"],
                    "release_date": release_date,
                    "release_date_human": parse_and_humanize_date(release_date),
                    "release_date_delta": humanize_date_delta(release_date),
                    "discogs_id": discogs_id,
                    "match_stars": score_stars(score),
                    "image_exists": os.path.exists(os.path.join(self.cfg.images_folder, f"{discogs_id}.jpg")),
                }
            )
        return rows

    def get_random_release(self, filters: Optional[CollectionFilters] = None):
        rows = self.list_collection(filters)
        if not rows:
            return None
        chosen = random.choice(rows)
        return self.get_release_detail(chosen["discogs_id"])

    def mark_played(self, discogs_id: int):
        increment_play_stats(self.cfg.db_path, discogs_id)
        return self.get_release_detail(discogs_id)

    def mark_cleaned(self, discogs_id: int):
        set_last_cleaned(self.cfg.db_path, discogs_id)
        return self.get_release_detail(discogs_id)

    def update_release_date(self, discogs_id: int, release_date: str, locked: bool):
        with context_manager(self.cfg.db_path) as cur:
            cur.execute(
                "UPDATE discogs_releases SET release_date = ?, release_date_locked = ? WHERE discogs_id = ?",
                (release_date, int(bool(locked)), discogs_id),
            )
        return self.get_release_detail(discogs_id)

    @staticmethod
    def _matches_month_day(release_date: str, month: int, day: int) -> bool:
        if not release_date:
            return False

        try:
            if len(release_date) == 10 and "-" in release_date:
                _, parsed_month, parsed_day = map(int, release_date.split("-"))
                return parsed_month == month and parsed_day == day
            if len(release_date) == 7 and "-" in release_date:
                _, parsed_month = release_date.split("-")
                return int(parsed_month) == month
        except (TypeError, ValueError):
            return False

        return False


def score_stars(score):
    if score >= 95:
        return "🟢"
    if score >= 80:
        return "🟡"
    if score >= 60:
        return "🟠"
    return "🔴"
