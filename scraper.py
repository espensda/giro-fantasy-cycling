"""Web scraping utilities for Giro fantasy data."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup


def _normalize_name(name: str) -> str:
    # PCS often lists surnames first in uppercase; title case is more readable in UI.
    return " ".join(name.strip().split()).title()


def _to_surname_first(name: str) -> str:
    """Convert `First Last` style names to `Last First` for DB consistency.

    Supports common surname particles such as `van`, `del`, `de`, `di`.
    """
    cleaned = " ".join(name.strip().split())
    if not cleaned:
        return ""

    parts = cleaned.split(" ")
    if len(parts) <= 1:
        return cleaned

    surname_particles = {
        "da", "de", "del", "della", "di", "du", "la", "le",
        "van", "von", "der", "den", "ter", "ten",
    }

    surname_start = len(parts) - 1
    while surname_start - 1 >= 0 and parts[surname_start - 1].lower() in surname_particles:
        surname_start -= 1

    surname = parts[surname_start:]
    given_names = parts[:surname_start]
    if not given_names:
        return cleaned

    return _normalize_name(" ".join(surname + given_names))


def _normalize_team_rows(rows: list[dict]) -> list[dict]:
    """Ensure youth riders are always assigned to the youth category."""
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["team"], []).append(dict(row))

    normalized: list[dict] = []
    for team_rows in grouped.values():
        non_youth_index = 0
        for row in team_rows:
            if row["category"] == "ds":
                normalized.append(row)
                continue

            if row.get("youth"):
                row["category"] = "youth"
            elif non_youth_index == 0:
                row["category"] = "captain"
                non_youth_index += 1
            elif non_youth_index in (1, 2):
                row["category"] = "sprinter"
                non_youth_index += 1
            elif non_youth_index in (3, 4):
                row["category"] = "climber"
                non_youth_index += 1
            else:
                row["category"] = "water_carrier"
                non_youth_index += 1

            normalized.append(row)

    return normalized


class GiroScraper:
    """Small wrapper around requests/BeautifulSoup for future data collection."""

    def __init__(self, base_url: str = "https://www.procyclingstats.com") -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(__file__).resolve().parent / "data" / "startlist_cache"
        self.bootstrap_startlist_dir = Path(__file__).resolve().parent / "data" / "bootstrap_startlist"
        self.results_cache_dir = Path(__file__).resolve().parent / "data" / "results_cache"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": self.base_url,
            }
        )

    def _cache_path(self, year: int) -> Path:
        return self.cache_dir / f"giro_{year}.json"

    def _save_cache(self, year: int, rows: list[dict]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache_path(year).write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_cache(self, year: int) -> list[dict]:
        path = self._cache_path(year)
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return _normalize_team_rows(data)
        return []

    def _load_bootstrap_startlist(self, year: int) -> list[dict]:
        path = self.bootstrap_startlist_dir / f"giro_{year}.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return _normalize_team_rows(data)
        return []

    def _stage_results_cache_path(self, year: int, stage_number: int) -> Path:
        return self.results_cache_dir / f"giro_{year}_stage_{stage_number}.json"

    def _save_stage_results_cache(self, year: int, stage_number: int, payload: dict) -> None:
        self.results_cache_dir.mkdir(parents=True, exist_ok=True)
        self._stage_results_cache_path(year, stage_number).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_stage_results_cache(self, year: int, stage_number: int) -> dict | None:
        path = self._stage_results_cache_path(year, stage_number)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            data.setdefault("source", "cache")
            return data
        return None

    def _get_soup(self, url: str) -> BeautifulSoup:
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")

    def _get_jina_text(self, url: str) -> str:
        mirror_url = f"https://r.jina.ai/http://{url}"
        response = self.session.get(mirror_url, timeout=40)
        response.raise_for_status()
        return response.text

    def _guess_rider_category(self, non_youth_index_in_team: int) -> str:
        if non_youth_index_in_team == 0:
            return "captain"
        if non_youth_index_in_team in (1, 2):
            return "sprinter"
        if non_youth_index_in_team in (3, 4):
            return "climber"
        return "water_carrier"

    def _parse_startlist_rows(self, soup: BeautifulSoup, year: int) -> list[dict]:
        """Parse team blocks by scanning team links and following rider/staff links in order."""
        rows: list[dict] = []
        team_links = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "/team/" not in href:
                continue
            if "/team-in-race/" in href:
                continue
            if f"-{year}" not in href:
                continue
            team_links.append(a)

        seen_teams: set[str] = set()
        valid_team_links: list = []
        for a in team_links:
            href = a.get("href", "")
            if href in seen_teams:
                continue
            seen_teams.add(href)
            valid_team_links.append(a)

        for idx, team_link in enumerate(valid_team_links):
            team_name = " ".join(team_link.get_text(" ", strip=True).split())
            if not team_name:
                continue

            block_nodes = []
            node = team_link.parent
            next_team = valid_team_links[idx + 1] if idx + 1 < len(valid_team_links) else None
            while node is not None and node is not next_team.parent:
                block_nodes.append(node)
                node = node.find_next_sibling()
                if node is None:
                    break

            non_youth_index = 0
            for block in block_nodes:
                for rider_link in block.select('a[href^="/rider/"]'):
                    rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
                    if not rider_name:
                        continue

                    parent_text = rider_link.parent.get_text(" ", strip=True) if rider_link.parent else ""
                    is_youth = "*" in parent_text

                    category = "youth" if is_youth else self._guess_rider_category(non_youth_index)

                    rows.append(
                        {
                            "name": rider_name,
                            "team": team_name,
                            "category": category,
                            "youth": is_youth,
                        }
                    )
                    if not is_youth:
                        non_youth_index += 1

                staff_links = block.select('a[href^="/staff/"]')
                for staff_link in staff_links:
                    staff_name = _normalize_name(staff_link.get_text(" ", strip=True))
                    if not staff_name:
                        continue
                    rows.append(
                        {
                            "name": staff_name,
                            "team": team_name,
                            "category": "ds",
                            "youth": False,
                        }
                    )

        # Deduplicate by name, preserving first occurrence.
        deduped: list[dict] = []
        seen_names: set[str] = set()
        for row in rows:
            if row["name"] in seen_names:
                continue
            seen_names.add(row["name"])
            deduped.append(row)
        return _normalize_team_rows(deduped)

    def _parse_startlist_markdown(self, text: str, year: int) -> list[dict]:
        """Parse startlist from the r.jina.ai markdown mirror of the PCS page."""
        rows: list[dict] = []
        current_team = None
        non_youth_index = 0
        in_startlist = False

        lines = text.splitlines()
        for line in lines:
            stripped = line.strip()

            if not in_startlist and re.search(r"\briders\b", stripped, flags=re.IGNORECASE):
                in_startlist = True
                continue

            if not in_startlist and f"/team/" in line and f"-{year})" in line:
                in_startlist = True

            if not in_startlist:
                continue

            if stripped.startswith("* = competes for youth GC"):
                break

            links = re.findall(r"\[([^\]]*)\]\((https?://[^)]+)\)", line)
            if links:
                team_candidates = [
                    (name, url) for name, url in links
                    if "/team/" in url and "/team-in-race/" not in url
                ]
                if team_candidates:
                    current_team = team_candidates[-1][0].strip()
                    non_youth_index = 0
                    continue

            if current_team is None:
                continue

            rider_match = re.match(r"^\s*\*?\s*\d+\s*\[([^\]]+)\]\((https?://[^)]+/rider/[^)]+)\)(.*)$", line)
            if rider_match:
                rider_name = _normalize_name(rider_match.group(1))
                tail = rider_match.group(3)
                is_youth = "*" in tail
                category = "youth" if is_youth else self._guess_rider_category(non_youth_index)

                rows.append(
                    {
                        "name": rider_name,
                        "team": current_team,
                        "category": category,
                        "youth": is_youth,
                    }
                )
                if not is_youth:
                    non_youth_index += 1
                continue

            if "DS" in line and "/staff/" in line:
                staff_links = [
                    (name, url) for name, url in re.findall(r"\[([^\]]+)\]\((https?://[^)]+)\)", line)
                    if "/staff/" in url
                ]
                for staff_name, _ in staff_links:
                    normalized = _normalize_name(staff_name)
                    if not normalized:
                        continue
                    rows.append(
                        {
                            "name": normalized,
                            "team": current_team,
                            "category": "ds",
                            "youth": False,
                        }
                    )

        deduped: list[dict] = []
        seen_names: set[str] = set()
        for row in rows:
            if row["name"] in seen_names:
                continue
            seen_names.add(row["name"])
            deduped.append(row)
        return _normalize_team_rows(deduped)

    def _parse_stage_results_html(self, soup: BeautifulSoup, stage_number: int, year: int) -> dict:
        results: list[dict] = []

        for row in soup.select("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            rider_link = row.select_one('a[href*="/rider/"]')
            if rider_link is None:
                continue

            position = None
            for cell in cells[:3]:
                cell_text = " ".join(cell.get_text(" ", strip=True).split())
                if cell_text.isdigit():
                    position = int(cell_text)
                    break
            if position is None:
                continue

            rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
            if not rider_name:
                continue

            team_link = row.select_one('a[href*="/team/"]')
            team_name = " ".join(team_link.get_text(" ", strip=True).split()) if team_link else ""

            row_text = " ".join(row.get_text(" ", strip=True).split())
            time_match = re.search(r"(s\.t\.|\+?\d+:\d{2}(?::\d{2})?)", row_text, flags=re.IGNORECASE)
            result_time = time_match.group(1) if time_match else ""

            results.append(
                {
                    "position": position,
                    "name": rider_name,
                    "team": team_name,
                    "time": result_time,
                }
            )

        deduped: list[dict] = []
        seen_positions: set[int] = set()
        for row in sorted(results, key=lambda item: item["position"]):
            if row["position"] in seen_positions:
                continue
            seen_positions.add(row["position"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "stage": f"stage-{stage_number}",
            "url": f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}/result",
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def _parse_stage_results_markdown(self, text: str, stage_number: int, year: int) -> dict:
        results: list[dict] = []

        for line in text.splitlines():
            if "/rider/" not in line:
                continue

            position_match = re.match(r"^\s*\|?\s*(\d+)\s*(?:\||\.)", line)
            if position_match is None:
                continue

            rider_links = re.findall(r"\[([^\]]+)\]\((https?://[^)]+)\)", line)
            rider_name = ""
            team_name = ""
            for label, url in rider_links:
                if "/rider/" in url and not rider_name:
                    rider_name = _normalize_name(label)
                elif "/team/" in url and not team_name:
                    team_name = " ".join(label.strip().split())

            if not rider_name:
                continue

            tail = re.sub(r"\[[^\]]+\]\([^)]*\)", "", line)
            time_match = re.search(r"(s\.t\.|\+?\d+:\d{2}(?::\d{2})?)", tail, flags=re.IGNORECASE)
            result_time = time_match.group(1) if time_match else ""

            results.append(
                {
                    "position": int(position_match.group(1)),
                    "name": rider_name,
                    "team": team_name,
                    "time": result_time,
                }
            )

        deduped: list[dict] = []
        seen_positions: set[int] = set()
        for row in sorted(results, key=lambda item: item["position"]):
            if row["position"] in seen_positions:
                continue
            seen_positions.add(row["position"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "stage": f"stage-{stage_number}",
            "url": f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}/result",
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def _parse_cyclingstage_results(self, soup: BeautifulSoup, stage_number: int, year: int, url: str) -> dict:
        """Parse top stage results from Cyclingstage result pages."""
        results: list[dict] = []

        target_h2 = None
        for heading in soup.find_all("h2"):
            text = " ".join(heading.get_text(" ", strip=True).split())
            if "Results" in text and "stage" in text.lower():
                target_h2 = heading
                break

        candidate_paragraph = target_h2.find_next("p") if target_h2 else None
        if candidate_paragraph is not None:
            paragraph_text = candidate_paragraph.get_text("\n", strip=True)
            for raw_line in paragraph_text.splitlines():
                line = " ".join(raw_line.strip().split())
                # Common format: "1. Joshua Tarling (gbr) 16.07" or "2. ... + 0.03"
                common_match = re.match(
                    r"^(\d+)\.\s+(.+?)\s+\([a-z]{2,3}\)\s*(s\.t\.|(?:\+\s*)?\d+(?:[:\.]\d{2}){1,2})?$",
                    line,
                    flags=re.IGNORECASE,
                )
                if common_match is not None:
                    position = int(common_match.group(1))
                    name = _to_surname_first(common_match.group(2))
                    result_time = (common_match.group(3) or "").replace(" ", "")
                else:
                    fallback_match = re.match(
                        r"^(\d+)\.\s+(.+?)(?:\s+\([a-z]{2,3}\))?(?:\s+(s\.t\.|\+\s*\d+(?:[:\.]\d{2}){1,2}))?$",
                        line,
                        flags=re.IGNORECASE,
                    )
                    if fallback_match is None:
                        continue

                    position = int(fallback_match.group(1))
                    name = _to_surname_first(fallback_match.group(2))
                    result_time = (fallback_match.group(3) or "").replace(" ", "")

                results.append(
                    {
                        "position": position,
                        "name": name,
                        "team": "",
                        "time": result_time,
                    }
                )

        deduped: list[dict] = []
        seen_positions: set[int] = set()
        for row in sorted(results, key=lambda item: item["position"]):
            if row["position"] in seen_positions:
                continue
            seen_positions.add(row["position"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "stage": f"stage-{stage_number}",
            "url": url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def _parse_firstcycling_html(self, soup: BeautifulSoup, stage_number: int, year: int, url: str) -> dict:
        """Parse stage result rows from a FirstCycling HTML race page."""
        results: list[dict] = []

        title_text = " ".join((soup.title.get_text(" ", strip=True) if soup.title else "").split()).lower()
        if "complete and up to date cycling database" in title_text:
            # Generic landing content should not be treated as stage results.
            return {
                "year": year,
                "stage_number": stage_number,
                "stage": f"stage-{stage_number}",
                "url": url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
            }

        for row in soup.select("tr"):
            rider_link = row.select_one('a[href*="rider.php?r="]')
            if rider_link is None:
                continue

            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            position = None
            for cell in cells[:4]:
                token = " ".join(cell.get_text(" ", strip=True).split()).replace(".", "")
                if token.isdigit():
                    position = int(token)
                    break
            if position is None:
                continue

            rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
            if not rider_name:
                continue

            team_link = row.select_one('a[href*="team.php?"]')
            team_name = " ".join(team_link.get_text(" ", strip=True).split()) if team_link else ""

            row_text = " ".join(row.get_text(" ", strip=True).split())
            time_match = re.search(
                r"(s\.t\.|(?:\+\s*)?\d+(?:[:\.]\d{2}){1,2})",
                row_text,
                flags=re.IGNORECASE,
            )
            result_time = (time_match.group(1) if time_match else "").replace(" ", "")

            results.append(
                {
                    "position": position,
                    "name": rider_name,
                    "team": team_name,
                    "time": result_time,
                }
            )

        deduped: list[dict] = []
        seen_positions: set[int] = set()
        for row in sorted(results, key=lambda item: item["position"]):
            if row["position"] in seen_positions:
                continue
            seen_positions.add(row["position"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "stage": f"stage-{stage_number}",
            "url": url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def _parse_firstcycling_markdown(self, text: str, stage_number: int, year: int, url: str) -> dict:
        """Parse stage result rows from the FirstCycling r.jina.ai markdown mirror."""
        results: list[dict] = []

        expected_marker = f"race.php?r=13&y={year}&e={stage_number:02d}"
        # Mirror may return a generic "What's on" feed; require stage marker to appear in content.
        if text.count(expected_marker) < 2:
            return {
                "year": year,
                "stage_number": stage_number,
                "stage": f"stage-{stage_number}",
                "url": url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
            }

        for line in text.splitlines():
            if "rider.php?r=" not in line:
                continue

            position_match = re.match(r"^\s*\|?\s*(\d+)\s*(?:\||\.)", line)
            if position_match is None:
                continue

            links = re.findall(r"\[([^\]]+)\]\((https?://[^)]+)\)", line)
            rider_name = ""
            team_name = ""
            for label, link_url in links:
                if "rider.php?r=" in link_url and not rider_name:
                    rider_name = _normalize_name(label)
                elif "team.php?" in link_url and not team_name:
                    team_name = " ".join(label.strip().split())

            if not rider_name:
                continue

            tail = re.sub(r"\[[^\]]+\]\([^)]*\)", "", line)
            time_match = re.search(
                r"(s\.t\.|(?:\+\s*)?\d+(?:[:\.]\d{2}){1,2})",
                tail,
                flags=re.IGNORECASE,
            )
            result_time = (time_match.group(1) if time_match else "").replace(" ", "")

            results.append(
                {
                    "position": int(position_match.group(1)),
                    "name": rider_name,
                    "team": team_name,
                    "time": result_time,
                }
            )

        deduped: list[dict] = []
        seen_positions: set[int] = set()
        for row in sorted(results, key=lambda item: item["position"]):
            if row["position"] in seen_positions:
                continue
            seen_positions.add(row["position"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "stage": f"stage-{stage_number}",
            "url": url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def scrape_giro_startlist(self, year: int = 2025) -> list[dict]:
        """Scrape Giro startlist riders and DS from ProCyclingStats for a given year."""
        url = f"{self.base_url}/race/giro-d-italia/{year}/startlist/startlist"
        errors: list[str] = []

        try:
            soup = self._get_soup(url)
            rows = self._parse_startlist_rows(soup, year=year)
            if rows:
                self._save_cache(year, rows)
                return rows
        except requests.RequestException as exc:
            errors.append(f"direct fetch: {exc}")

        try:
            text = self._get_jina_text(url)
            rows = self._parse_startlist_markdown(text, year=year)
            if rows:
                self._save_cache(year, rows)
                return rows
            errors.append("mirror fetch: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"mirror fetch: {exc}")

        cached_rows = self._load_cache(year)
        if cached_rows:
            return cached_rows

        bootstrap_rows = self._load_bootstrap_startlist(year)
        if bootstrap_rows:
            # Refresh writable cache so the app can continue using local fallback.
            self._save_cache(year, bootstrap_rows)
            return bootstrap_rows

        joined = " | ".join(errors) if errors else "unknown error"
        raise RuntimeError(
            f"Unable to fetch startlist for {year}. External sources are blocked and no local/bootstrap cache was found ({joined})."
        )

    def get_rider_data(self, rider_slug: str) -> dict:
        """Fetch a single rider page and return parsed data.

        Parsing is intentionally minimal until the full extraction rules are defined.
        """
        url = f"{self.base_url}/rider/{rider_slug}"
        soup = self._get_soup(url)
        title = soup.title.text.strip() if soup.title and soup.title.text else rider_slug
        return {"slug": rider_slug, "title": title, "url": url}

    def get_stage_results(self, stage_slug: str) -> dict:
        """Fetch a stage page and return placeholder structured data."""
        url = f"{self.base_url}/race/giro-d-italia/{stage_slug}"
        soup = self._get_soup(url)
        title = soup.title.text.strip() if soup.title and soup.title.text else stage_slug
        return {"stage": stage_slug, "title": title, "url": url}

    def scrape_stage_results(self, stage_number: int, year: int, include_firstcycling: bool = False) -> dict:
        """Fetch Giro stage results with cache fallback."""
        url = f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}/result"
        firstcycling_url = f"https://www.firstcycling.com/race.php?r=13&y={year}&e={stage_number:02d}"
        cyclingstage_url = (
            f"https://www.cyclingstage.com/giro-{year}-results/"
            f"stage-{stage_number}-italy-results-{year}/"
        )
        errors: list[str] = []

        try:
            soup = self._get_soup(url)
            payload = self._parse_stage_results_html(soup, stage_number=stage_number, year=year)
            if payload["results"]:
                payload["source"] = "direct"
                self._save_stage_results_cache(year, stage_number, payload)
                return payload
            errors.append("direct fetch: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"direct fetch: {exc}")

        try:
            text = self._get_jina_text(url)
            payload = self._parse_stage_results_markdown(text, stage_number=stage_number, year=year)
            if payload["results"]:
                payload["source"] = "mirror"
                self._save_stage_results_cache(year, stage_number, payload)
                return payload
            errors.append("mirror fetch: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"mirror fetch: {exc}")

        if include_firstcycling:
            try:
                soup = self._get_soup(firstcycling_url)
                payload = self._parse_firstcycling_html(
                    soup,
                    stage_number=stage_number,
                    year=year,
                    url=firstcycling_url,
                )
                if payload["results"]:
                    payload["source"] = "firstcycling"
                    self._save_stage_results_cache(year, stage_number, payload)
                    return payload
                errors.append("firstcycling fetch: no parseable rows")
            except requests.RequestException as exc:
                errors.append(f"firstcycling fetch: {exc}")

            try:
                mirror_text = self._get_jina_text(firstcycling_url)
                payload = self._parse_firstcycling_markdown(
                    mirror_text,
                    stage_number=stage_number,
                    year=year,
                    url=firstcycling_url,
                )
                if payload["results"]:
                    payload["source"] = "firstcycling-mirror"
                    self._save_stage_results_cache(year, stage_number, payload)
                    return payload
                errors.append("firstcycling mirror: no parseable rows")
            except requests.RequestException as exc:
                errors.append(f"firstcycling mirror: {exc}")

        try:
            soup = self._get_soup(cyclingstage_url)
            payload = self._parse_cyclingstage_results(
                soup,
                stage_number=stage_number,
                year=year,
                url=cyclingstage_url,
            )
            if payload["results"]:
                payload["source"] = "cyclingstage"
                self._save_stage_results_cache(year, stage_number, payload)
                return payload
            errors.append("cyclingstage fetch: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"cyclingstage fetch: {exc}")

        cached_payload = self._load_stage_results_cache(year, stage_number)
        if cached_payload is not None:
            return cached_payload

        joined = " | ".join(errors) if errors else "unknown error"
        raise RuntimeError(
            f"Unable to fetch results for stage {stage_number} in {year}. No parseable result and no local cache found ({joined})."
        )
