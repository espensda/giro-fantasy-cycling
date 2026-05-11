"""Web scraping utilities for Giro fantasy data."""

from __future__ import annotations

from io import BytesIO
import json
import re
from datetime import UTC, datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

try:
    import cloudscraper
except ImportError:  # pragma: no cover - optional dependency fallback
    cloudscraper = None


def _normalize_name(name: str) -> str:
    # PCS often lists surnames first in uppercase; title case is more readable in UI.
    return " ".join(name.strip().split()).title()


def _safe_number(value: str) -> float | None:
    cleaned = str(value or "").strip().replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def _extract_slug_from_href(href: str) -> str:
    cleaned = str(href or "").strip().lstrip("/")
    if not cleaned:
        return ""
    marker = "rider/"
    if marker not in cleaned:
        return ""
    return cleaned.split(marker, 1)[1].split("?", 1)[0].split("#", 1)[0].strip("/")


def _parse_age_text(age_text: str) -> float | None:
    match = re.search(r"(\d+)(?:y|\s*years?)", str(age_text or ""), flags=re.IGNORECASE)
    if not match:
        return None
    return _safe_number(match.group(1))


def _is_known_youth(name: str) -> bool:
    """Best-effort youth detection for sources that omit youth markers (e.g. PDFs)."""
    youth_names = {
        "arrieta igor",
        "baroncini filippo",
        "busatto francesco",
        "del toro isaac",
        "double paul",
        "eulalio afonso",
        "frigo marco",
        "garofoli gianmarco",
        "heiduk kim",
        "kielich timo",
        "lamperti luke",
        "leemreize gijs",
        "magnier paul",
        "markl niklas",
        "milesi lorenzo",
        "paleni enzo",
        "pellizzari giulio",
        "piganzoli davide",
        "planckaert edward",
        "plowright jensen",
        "rafferty darren",
        "steinhauser georg",
        "tarling joshua",
        "vacek mathias",
        "vader milan",
        "van uden casper",
    }
    key = " ".join((name or "").lower().split())
    return key in youth_names


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
        # Preserve explicit role/category assignments when enrichment metadata exists.
        has_enriched_roles = any(
            any(key in row for key in ("gc_rank", "sprinter_rank", "climber_rank", "role"))
            for row in team_rows
        )
        if has_enriched_roles:
            for row in team_rows:
                item = dict(row)
                if item.get("category") == "youth":
                    item["youth"] = True
                normalized.append(item)
            continue

        non_youth_index = 0
        for row in team_rows:
            if row["category"] == "ds":
                normalized.append(row)
                continue

            if _is_known_youth(row.get("name", "")):
                row["youth"] = True

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
        self.cloudflare_session = None
        if cloudscraper is not None:
            self.cloudflare_session = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "linux", "mobile": False}
            )

    def _cache_path(self, year: int) -> Path:
        return self.cache_dir / f"giro_{year}.json"

    def _age_cache_path(self, year: int) -> Path:
        return self.cache_dir / f"giro_{year}_ages.json"

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

    def _load_age_cache(self, year: int) -> dict[str, float]:
        path = self._age_cache_path(year)
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(data, dict):
            return {}
        cached: dict[str, float] = {}
        for key, value in data.items():
            parsed = _safe_number(str(value))
            if key and parsed is not None and 16 <= float(parsed) <= 50:
                cached[str(key)] = float(parsed)
        return cached

    def _save_age_cache(self, year: int, ages_by_slug: dict[str, float]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._age_cache_path(year)
        serializable = {key: float(value) for key, value in ages_by_slug.items() if key}
        path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")

    def _fetch_rider_age_by_slug(self, rider_slug: str) -> float | None:
        if not rider_slug:
            return None
        url = f"{self.base_url}/rider/{rider_slug}"
        try:
            soup = self._get_soup(url)
        except requests.RequestException:
            return None

        for li in soup.select("li"):
            li_text = " ".join(li.get_text(" ", strip=True).split())
            if "Date of birth" not in li_text:
                continue
            match = re.search(r"\(\s*(\d{1,2})\s*\)", li_text)
            if match is None:
                continue
            age = _safe_number(match.group(1))
            if age is None:
                return None
            if 16 <= age <= 50:
                return age
            return None
        return None

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
        if response.status_code in (401, 403) and self.cloudflare_session is not None:
            cf_response = self.cloudflare_session.get(url, timeout=30)
            cf_response.raise_for_status()
            return BeautifulSoup(cf_response.content, "html.parser")

        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")

    def _get_jina_text(self, url: str) -> str:
        cleaned_url = (url or "").strip()
        if not cleaned_url.startswith(("http://", "https://")):
            cleaned_url = f"https://{cleaned_url}"
        mirror_url = f"https://r.jina.ai/{cleaned_url}"
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

    def _parse_specialty_startlist(self, soup: BeautifulSoup) -> list[dict]:
        """Parse a PCS specialty startlist page into ranked rider entries."""
        entries: list[dict] = []
        for row in soup.select("tr"):
            rider_link = row.select_one('a[href*="rider/"]')
            if rider_link is None:
                continue

            cells = [" ".join(cell.get_text(" ", strip=True).split()) for cell in row.select("td")]
            if len(cells) < 5:
                continue

            rank_value = _safe_number(cells[0])
            points_value = _safe_number(cells[-2])
            if rank_value is None or points_value is None:
                continue

            entries.append(
                {
                    "name": _normalize_name(rider_link.get_text(" ", strip=True)),
                    "slug": _extract_slug_from_href(rider_link.get("href", "")),
                    "rank": int(rank_value),
                    "points": float(points_value),
                }
            )

        entries.sort(key=lambda item: item["rank"])
        return entries

    def _fetch_specialty_rankings(self, year: int) -> dict[str, list[dict]]:
        """Fetch PCS specialty pages used for rider role and price quality signals."""
        slugs = {
            "gc": "top-gc-riders",
            "sprinter": "sprinters",
            "climber": "climbers",
            "tt": "tt-specialists",
            "classic": "classic-riders",
        }
        rankings: dict[str, list[dict]] = {}
        for key, slug in slugs.items():
            url = f"{self.base_url}/race/giro-d-italia/{year}/startlist/{slug}"
            try:
                soup = self._get_soup(url)
                parsed = self._parse_specialty_startlist(soup)
                if parsed:
                    rankings[key] = parsed
            except requests.RequestException:
                continue
        return rankings

    def _fetch_youngest_oldest_age_map(self, year: int) -> tuple[dict[str, float], dict[str, float]]:
        """Fetch available rider ages from the PCS youngest/oldest startlist page."""
        url = f"{self.base_url}/race/giro-d-italia/{year}/startlist/youngest-oldest"
        by_slug: dict[str, float] = {}
        by_name: dict[str, float] = {}
        try:
            soup = self._get_soup(url)
        except requests.RequestException:
            return by_slug, by_name

        for row in soup.select("tr"):
            rider_link = row.select_one('a[href*="rider/"]')
            if rider_link is None:
                continue
            cells = [" ".join(cell.get_text(" ", strip=True).split()) for cell in row.select("td")]
            if not cells:
                continue
            age_value = _parse_age_text(cells[-1])
            if age_value is None:
                continue

            rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
            rider_slug = _extract_slug_from_href(rider_link.get("href", ""))
            if rider_slug:
                by_slug[rider_slug] = age_value
            by_name[rider_name] = age_value
        return by_slug, by_name

    def _fetch_startlist_slug_map(self, year: int) -> dict[str, str]:
        """Fetch rider slug mapping from alphabetical startlist page."""
        url = f"{self.base_url}/race/giro-d-italia/{year}/startlist/alphabetical"
        mapping: dict[str, str] = {}
        try:
            soup = self._get_soup(url)
        except requests.RequestException:
            return mapping

        for row in soup.select("tr"):
            rider_link = row.select_one('a[href*="rider/"]')
            if rider_link is None:
                continue
            rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
            rider_slug = _extract_slug_from_href(rider_link.get("href", ""))
            if rider_name and rider_slug and rider_name not in mapping:
                mapping[rider_name] = rider_slug
        return mapping

    def enrich_rows_with_specialty_scores(self, rows: list[dict], year: int) -> list[dict]:
        """Attach specialty-derived role scores for pricing and categorization.

        This enrichment is best-effort and never raises; rows are returned unchanged
        if specialty pages are unavailable.
        """
        if not rows:
            return rows

        rankings = self._fetch_specialty_rankings(year)
        if not rankings:
            return rows

        slug_by_name = self._fetch_startlist_slug_map(year)
        age_by_slug, age_by_name = self._fetch_youngest_oldest_age_map(year)
        cached_ages = self._load_age_cache(year)
        age_by_slug.update(cached_ages)

        # Build percentile maps for each specialty key.
        percentile_by_key: dict[str, dict[str, float]] = {}
        rank_by_key: dict[str, dict[str, int]] = {}
        for key, entries in rankings.items():
            total = len(entries)
            if total <= 1:
                percentile_by_key[key] = {
                    (entry.get("slug") or entry["name"]): 1.0 for entry in entries
                }
                rank_by_key[key] = {
                    (entry.get("slug") or entry["name"]): int(entry["rank"]) for entry in entries
                }
                continue

            metric: dict[str, float] = {}
            ranks: dict[str, int] = {}
            for entry in entries:
                rank = entry["rank"]
                identity = entry.get("slug") or entry["name"]
                metric[identity] = max(0.0, 1.0 - ((rank - 1) / (total - 1)))
                ranks[identity] = int(rank)
            percentile_by_key[key] = metric
            rank_by_key[key] = ranks

        enriched: list[dict] = []
        for row in rows:
            item = dict(row)
            if item.get("category") == "ds":
                enriched.append(item)
                continue

            rider_name = _normalize_name(str(item.get("name", "")))
            rider_slug = str(item.get("slug") or slug_by_name.get(rider_name, "")).strip()
            identity_keys = [identity for identity in (rider_slug, rider_name) if identity]

            def _lookup(metric_key: str) -> tuple[float, int | None]:
                metric = percentile_by_key.get(metric_key, {})
                ranks = rank_by_key.get(metric_key, {})
                for identity in identity_keys:
                    if identity in metric:
                        return metric[identity], ranks.get(identity)
                return 0.0, None

            gc, gc_rank = _lookup("gc")
            spr, spr_rank = _lookup("sprinter")
            clm, clm_rank = _lookup("climber")
            tt, tt_rank = _lookup("tt")
            cls, classic_rank = _lookup("classic")

            rider_age = None
            if rider_slug:
                rider_age = age_by_slug.get(rider_slug)
            if rider_age is None:
                rider_age = age_by_name.get(rider_name)
            if rider_age is None and rider_slug:
                fetched_age = self._fetch_rider_age_by_slug(rider_slug)
                if fetched_age is not None:
                    rider_age = fetched_age
                    age_by_slug[rider_slug] = fetched_age

            candidate_roles: list[tuple[str, int]] = []
            if isinstance(gc_rank, int) and gc_rank <= 20:
                candidate_roles.append(("captain", gc_rank))
            if isinstance(spr_rank, int) and spr_rank <= 20:
                candidate_roles.append(("sprinter", spr_rank))
            if isinstance(clm_rank, int) and clm_rank <= 20:
                candidate_roles.append(("climber", clm_rank))

            if candidate_roles:
                role = sorted(candidate_roles, key=lambda candidate: candidate[1])[0][0]
            elif rider_age is not None and rider_age <= 25:
                role = "youth"
            else:
                role = "water_carrier"

            captain_score = round((gc * 0.55 + clm * 0.25 + tt * 0.15 + cls * 0.05) * 100, 2)
            sprinter_score = round((spr * 0.65 + cls * 0.2 + tt * 0.1 + gc * 0.05) * 100, 2)
            climber_score = round((clm * 0.6 + gc * 0.3 + tt * 0.1) * 100, 2)
            water_carrier_score = round((cls * 0.35 + tt * 0.25 + gc * 0.2 + spr * 0.2) * 100, 2)

            item["captain_score"] = captain_score
            item["sprinter_score"] = sprinter_score
            item["climber_score"] = climber_score
            item["water_carrier_score"] = water_carrier_score
            item["role"] = role
            item["category"] = role
            item["youth"] = role == "youth"
            item["age"] = rider_age
            item["gc_rank"] = gc_rank
            item["sprinter_rank"] = spr_rank
            item["climber_rank"] = clm_rank
            item["tt_rank"] = tt_rank
            item["classic_rank"] = classic_rank
            item["web_score"] = round(max(captain_score, sprinter_score, climber_score), 2)
            enriched.append(item)

        if age_by_slug:
            self._save_age_cache(year, age_by_slug)

        return enriched

    def _parse_startlist_rows(self, soup: BeautifulSoup, year: int) -> list[dict]:
        """Parse team blocks by scanning team links and following rider/staff links in order."""
        rows: list[dict] = []
        team_links = []
        for a in soup.select("a[href]"):
            href = str(a.get("href", "") or "").strip()
            href_clean = href.lstrip("/")
            if "team/" not in href_clean:
                continue
            if "team-in-race/" in href_clean:
                continue
            if f"-{year}" not in href_clean:
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
            ds_added = False
            for block in block_nodes:
                for rider_link in block.select('a[href*="rider/"]'):
                    rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
                    rider_slug = _extract_slug_from_href(rider_link.get("href", ""))
                    if not rider_name:
                        continue

                    parent_text = rider_link.parent.get_text(" ", strip=True) if rider_link.parent else ""
                    is_youth = "*" in parent_text

                    category = "youth" if is_youth else self._guess_rider_category(non_youth_index)

                    rows.append(
                        {
                            "name": rider_name,
                            "slug": rider_slug,
                            "team": team_name,
                            "category": category,
                            "youth": is_youth,
                        }
                    )
                    if not is_youth:
                        non_youth_index += 1

                staff_links = block.select('a[href*="staff/"]')
                if staff_links and not ds_added:
                    rows.append(
                        {
                            "name": team_name,
                            "team": team_name,
                            "category": "ds",
                            "youth": False,
                        }
                    )
                    ds_added = True

        # Deduplicate by name, preserving first occurrence.
        deduped: list[dict] = []
        seen_names: set[str] = set()
        for row in rows:
            if row["name"] in seen_names:
                continue
            seen_names.add(row["name"])
            deduped.append(row)
        return _normalize_team_rows(deduped)

    def _parse_startlist_rows_modern(self, soup: BeautifulSoup) -> list[dict]:
        """Parse PCS startlist pages that render team blocks as `a.team` + adjacent `ul`.

        Newer PCS markup uses relative links (`team/...`, `rider/...`) and does not
        always match the legacy block traversal in `_parse_startlist_rows`.
        """
        rows: list[dict] = []

        team_links = soup.select('a.team[href*="team/"]')
        for team_link in team_links:
            team_name_raw = " ".join(team_link.get_text(" ", strip=True).split())
            team_name = team_name_raw.replace(" (WT)", "").strip()
            if not team_name:
                continue

            team_container = team_link.find_parent("div")
            if team_container is None:
                continue
            rider_list = team_container.find_next_sibling("ul")
            if rider_list is None:
                continue

            non_youth_index = 0
            ds_added = False
            for item in rider_list.select("li"):
                rider_link = item.select_one('a[href*="rider/"]')
                if rider_link is not None:
                    rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
                    rider_slug = _extract_slug_from_href(rider_link.get("href", ""))
                    if not rider_name:
                        continue

                    li_text = item.get_text(" ", strip=True)
                    is_youth = "*" in li_text
                    category = "youth" if is_youth else self._guess_rider_category(non_youth_index)

                    rows.append(
                        {
                            "name": rider_name,
                            "slug": rider_slug,
                            "team": team_name,
                            "category": category,
                            "youth": is_youth,
                        }
                    )
                    if not is_youth:
                        non_youth_index += 1
                    continue

                staff_link = item.select_one('a[href*="staff/"]')
                if staff_link is not None and not ds_added:
                    rows.append(
                        {
                            "name": team_name,
                            "team": team_name,
                            "category": "ds",
                            "youth": False,
                        }
                    )
                    ds_added = True

            if not ds_added:
                rows.append(
                    {
                        "name": team_name,
                        "team": team_name,
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

    def parse_startlist_html_content(self, html_content: str, year: int) -> list[dict]:
        """Parse PCS startlist rows from raw HTML content.

        This is intended for manual fallback when direct requests are blocked.
        Users can save the startlist page in a browser and upload the HTML.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        return self._parse_startlist_rows(soup, year=year)

    def parse_startlist_pdf_bytes(self, pdf_bytes: bytes) -> list[dict]:
        """Parse PCS startlist rows from an uploaded PDF export.

        Expected format is the PCS startlist PDF with numbered team headers,
        8 rider rows per team, and a trailing `DS:` line.
        """
        reader = PdfReader(BytesIO(pdf_bytes))
        text = "\n".join((page.extract_text() or "") for page in reader.pages)
        raw_lines = [" ".join(line.split()) for line in text.splitlines() if line.strip()]

        lines: list[str] = []
        index = 0
        while index < len(raw_lines):
            line = raw_lines[index]

            team_match = re.match(r"^(\d+)\s+(.+)$", line)
            rider_match = re.match(r"^\d+\.\s*(.+)$", line)
            if team_match and not rider_match and not line.startswith("DS:"):
                merged = line
                index += 1
                while index < len(raw_lines):
                    next_line = raw_lines[index]
                    next_team_match = re.match(r"^(\d+)\s+(.+)$", next_line)
                    next_rider_match = re.match(r"^\d+\.\s*(.+)$", next_line)
                    if next_line.startswith("DS:") or next_rider_match or next_team_match:
                        break
                    merged = f"{merged} {next_line}".strip()
                    index += 1
                lines.append(merged)
                continue

            if line.startswith("DS:"):
                merged = line
                index += 1
                while index < len(raw_lines):
                    next_line = raw_lines[index]
                    next_team_match = re.match(r"^(\d+)\s+(.+)$", next_line)
                    next_rider_match = re.match(r"^\d+\.\s*(.+)$", next_line)
                    if next_line.startswith("DS:") or next_rider_match or next_team_match:
                        break
                    merged = f"{merged} {next_line}".strip()
                    index += 1
                lines.append(merged)
                continue

            lines.append(line)
            index += 1

        rows: list[dict] = []
        current_team = ""
        non_youth_index = 0

        for raw_line in lines:
            line = raw_line.strip()

            team_match = re.match(r"^(\d+)\s+(.+)$", line)
            rider_match = re.match(r"^\d+\.\s*(.+)$", line)
            if team_match and rider_match is None and not line.startswith("DS:"):
                prefix = team_match.group(1)
                if len(prefix) <= 2:
                    current_team = team_match.group(2).strip()
                    non_youth_index = 0
                    continue

            if rider_match and current_team:
                rider_name = _normalize_name(rider_match.group(1))
                if rider_name:
                    rows.append(
                        {
                            "name": rider_name,
                            "team": current_team,
                            "category": self._guess_rider_category(non_youth_index),
                            "youth": False,
                        }
                    )
                    non_youth_index += 1
                continue

            if line.startswith("DS:") and current_team:
                rows.append(
                    {
                        "name": current_team,
                        "team": current_team,
                        "category": "ds",
                        "youth": False,
                    }
                )

        deduped: list[dict] = []
        seen_names: set[str] = set()
        for row in rows:
            if not row["name"] or row["name"] in seen_names:
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
                if staff_links:
                    rows.append(
                        {
                            "name": current_team,
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

            rider_link = row.select_one('a[href*="/rider/"], a[href*="rider/"]')
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

            team_link = row.select_one('a[href*="/team/"], a[href*="team/"]')
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

    def _parse_stage_classification_html(
        self,
        soup: BeautifulSoup,
        stage_number: int,
        year: int,
        classification: str,
        url: str,
    ) -> dict:
        """Parse a PCS stage classification table (e.g. GC after a stage)."""
        results: list[dict] = []

        table = soup.select_one("#resultsCont table.results") or soup.select_one("table.results")
        if table is None:
            return {
                "year": year,
                "stage_number": stage_number,
                "classification": classification,
                "url": url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
            }

        header_codes: list[str] = []
        for th in table.select("thead th"):
            code = (th.get("data-code") or "").strip()
            if not code:
                code = " ".join(th.get_text(" ", strip=True).split()).lower().replace(" ", "_")
            header_codes.append(code)

        for row in table.select("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            rider_link = row.select_one('a[href*="/rider/"], a[href*="rider/"]')
            if rider_link is None:
                continue

            rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
            if not rider_name:
                continue

            team_link = row.select_one('a[href*="/team/"], a[href*="team/"]')
            team_name = " ".join(team_link.get_text(" ", strip=True).split()) if team_link else ""

            values_by_code: dict[str, str] = {}
            for idx, cell in enumerate(cells):
                code = header_codes[idx] if idx < len(header_codes) else f"col_{idx}"
                values_by_code[code] = " ".join(cell.get_text(" ", strip=True).split())

            position = None
            preferred_codes = ["rnk"]
            if classification == "gc":
                preferred_codes = ["gc", "rnk"]

            for code in preferred_codes:
                token = values_by_code.get(code, "").replace(".", "").strip()
                if token.isdigit():
                    position = int(token)
                    break

            if position is None:
                for cell in cells[:3]:
                    cell_text = " ".join(cell.get_text(" ", strip=True).split()).replace(".", "")
                    if cell_text.isdigit():
                        position = int(cell_text)
                        break
            if position is None:
                continue

            # Keep textual GC value/time gap so scoring can still fall back to position tables.
            result_value = values_by_code.get("gc_timelag", "") or values_by_code.get("time", "")

            results.append(
                {
                    "position": position,
                    "name": rider_name,
                    "team": team_name,
                    "value": result_value,
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
            "classification": classification,
            "url": url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def scrape_stage_gc_results(self, stage_number: int, year: int) -> dict:
        """Fetch GC standings after a stage from PCS."""
        gc_url = f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}-gc"
        stage_url = f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}/result"
        errors: list[str] = []

        try:
            soup = self._get_soup(gc_url)
            payload = self._parse_stage_classification_html(
                soup,
                stage_number=stage_number,
                year=year,
                classification="gc",
                url=gc_url,
            )
            if payload["results"]:
                payload["source"] = "direct-gc"
                return payload
            errors.append("direct-gc: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"direct-gc: {exc}")

        try:
            soup = self._get_soup(stage_url)
            payload = self._parse_stage_classification_html(
                soup,
                stage_number=stage_number,
                year=year,
                classification="gc",
                url=stage_url,
            )
            if payload["results"]:
                payload["source"] = "direct-stage"
                return payload
            errors.append("direct-stage: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"direct-stage: {exc}")

        joined = " | ".join(errors) if errors else "unknown error"
        raise RuntimeError(
            f"Unable to fetch GC results for stage {stage_number} in {year} ({joined})."
        )

    def _parse_stage_metric_table_html(
        self,
        soup: BeautifulSoup,
        stage_number: int,
        year: int,
        metric: str,
        metric_code: str,
        url: str,
    ) -> dict:
        """Parse a PCS result table and extract a cumulative metric per rider."""
        results: list[dict] = []

        table = soup.select_one("#resultsCont table.results") or soup.select_one("table.results")
        if table is None:
            return {
                "year": year,
                "stage_number": stage_number,
                "metric": metric,
                "url": url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
            }

        header_codes: list[str] = []
        for th in table.select("thead th"):
            code = (th.get("data-code") or "").strip()
            if not code:
                code = " ".join(th.get_text(" ", strip=True).split()).lower().replace(" ", "_")
            header_codes.append(code)

        # Check if the expected metric column is present first
        metric_index = None
        for idx, code in enumerate(header_codes):
            if code == metric_code:
                metric_index = idx
                break

        # When anti-bot protection serves the generic stage-result table on `stage-*-kom`,
        # it includes GC/time columns and does not represent the KOM standings table.
        # Return early if metric column is missing (anti-bot detected).
        if metric_index is None:
            return {
                "year": year,
                "stage_number": stage_number,
                "metric": metric,
                "url": url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
            }

        for row in table.select("tr"):
            cells = row.find_all("td")
            if len(cells) <= metric_index:
                continue

            rider_link = row.select_one('a[href*="/rider/"], a[href*="rider/"]')
            if rider_link is None:
                continue

            rider_name = _normalize_name(rider_link.get_text(" ", strip=True))
            if not rider_name:
                continue

            team_link = row.select_one('a[href*="/team/"], a[href*="team/"]')
            team_name = " ".join(team_link.get_text(" ", strip=True).split()) if team_link else ""

            metric_text = " ".join(cells[metric_index].get_text(" ", strip=True).split())
            digits = "".join(ch for ch in metric_text if ch.isdigit())
            metric_value = int(digits) if digits else 0

            results.append(
                {
                    "name": rider_name,
                    "team": team_name,
                    "value": metric_value,
                }
            )

        deduped: list[dict] = []
        seen_names: set[str] = set()
        for row in results:
            if row["name"] in seen_names:
                continue
            seen_names.add(row["name"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "metric": metric,
            "url": url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
        }

    def scrape_stage_metric_points(self, stage_number: int, year: int, metric: str) -> dict:
        """Fetch cumulative per-rider metric values for a stage from PCS.

        Supported metrics:
        - `kom_cumulative`: KOM classification points after stage (column `pnt` on `stage-N-kom` page)
        - `bonis_cumulative`: cumulative bonus seconds after stage (column `bonis` on stage result page)
        """
        if metric == "kom_cumulative":
            url = f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}-kom"
            metric_code = "pnt"
        elif metric == "bonis_cumulative":
            url = f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}/result"
            metric_code = "bonis"
        else:
            raise ValueError(f"Unsupported metric: {metric}")

        soup = self._get_soup(url)
        payload = self._parse_stage_metric_table_html(
            soup,
            stage_number=stage_number,
            year=year,
            metric=metric,
            metric_code=metric_code,
            url=url,
        )
        payload["source"] = "direct"
        return payload

    def scrape_firstcycling_cumulative_points(
        self,
        stage_number: int,
        year: int,
        classification: str,
    ) -> dict:
        """Fetch cumulative points standings from FirstCycling stage widget.

        Supported classifications:
        - `mountains`: KOM cumulative points after stage
        - `points`: points jersey cumulative points after stage
        """
        widget_url = f"https://firstcycling.com/widget/?r=13&y={year}&s={stage_number}&lang=EN&cn=1"
        soup = self._get_soup(widget_url)
        tables = soup.select("table")

        # Widget table order (observed):
        # 1 stage result, 2 GC, 3 youth, 4 points, 5 KOM, 6 teams.
        if classification == "points":
            table_index = 3
        elif classification == "mountains":
            table_index = 4
        else:
            raise ValueError(f"Unsupported FirstCycling classification: {classification}")

        if len(tables) <= table_index:
            return {
                "year": year,
                "stage_number": stage_number,
                "classification": classification,
                "url": widget_url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
                "source": "firstcycling-widget",
            }

        table = tables[table_index]
        results: list[dict] = []
        for row in table.select("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            rider_link = row.select_one('a[href*="rider.php?r="]')
            if rider_link is not None:
                rider_name = _to_surname_first(rider_link.get_text(" ", strip=True))
            else:
                rider_cell = cells[2] if len(cells) > 2 else None
                if rider_cell is None:
                    continue

                rider_span = rider_cell.find("span")
                rider_text = " ".join((rider_span.get_text(" ", strip=True) if rider_span else rider_cell.get_text(" ", strip=True)).split())
                rider_name = _normalize_name(rider_text)

            if not rider_name:
                continue

            team_link = row.select_one('a[href*="team.php?"]')
            if team_link is not None:
                team_name = " ".join(team_link.get_text(" ", strip=True).split())
            else:
                team_name = ""
                rider_cell = cells[2] if len(cells) > 2 else None
                if rider_cell is not None:
                    spans = rider_cell.find_all("span")
                    if len(spans) >= 2:
                        team_name = " ".join(spans[-1].get_text(" ", strip=True).split())

            points_text = " ".join(cells[-1].get_text(" ", strip=True).split())
            digits = "".join(ch for ch in points_text if ch.isdigit())
            points_value = int(digits) if digits else 0

            if points_value <= 0:
                continue

            results.append(
                {
                    "name": rider_name,
                    "team": team_name,
                    "value": points_value,
                }
            )

        deduped: list[dict] = []
        seen_names: set[str] = set()
        for row in results:
            if row["name"] in seen_names:
                continue
            seen_names.add(row["name"])
            deduped.append(row)

        return {
            "year": year,
            "stage_number": stage_number,
            "classification": classification,
            "url": widget_url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": deduped,
            "source": "firstcycling-widget",
        }

    def scrape_stage_red_bull_sprint_points(self, stage_number: int, year: int) -> dict:
        """Fetch exact Red Bull KM sprint points for a stage from PCS result breakdowns."""
        stage_url = f"{self.base_url}/race/giro-d-italia/{year}/stage-{stage_number}/result"
        stage_soup = self._get_soup(stage_url)

        event_input = stage_soup.select_one('input[name="event_id"]')
        event_id = (event_input.get("value") if event_input else "").strip()
        if not event_id:
            raise RuntimeError("Could not resolve PCS event id for stage.")

        boni_url = f"https://www.procyclingstats.com/race.php?event={event_id}&p=results&s=most-bonifications"
        boni_soup = self._get_soup(boni_url)
        tables = boni_soup.select("table")
        if not tables:
            return {
                "year": year,
                "stage_number": stage_number,
                "url": boni_url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "results": [],
                "source": "pcs-most-bonifications",
            }

        rider_entries: list[dict] = []
        for row in tables[0].select("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 4:
                continue

            breakdown_link = row.select_one('a[href*="rider="]')
            if breakdown_link is None:
                continue

            rider_match = re.search(r"rider=(\d+)", breakdown_link.get("href", ""))
            if rider_match is None:
                continue

            rider_anchor = row.select_one('a[href*="/rider/"], a[href*="rider/"]')
            rider_name = _normalize_name(rider_anchor.get_text(" ", strip=True)) if rider_anchor else ""
            if not rider_name:
                continue

            team_name = " ".join(cells[2].get_text(" ", strip=True).split())
            rider_entries.append(
                {
                    "rider_id": rider_match.group(1),
                    "name": rider_name,
                    "team": team_name,
                }
            )

        rider_entries = list({entry["rider_id"]: entry for entry in rider_entries}.values())

        red_bull_bonus_by_name: dict[str, float] = {}
        team_by_name: dict[str, str] = {}

        for rider in rider_entries:
            breakdown_url = (
                f"https://www.procyclingstats.com/race.php?event={event_id}&p=results"
                f"&s=most-bonifications&rider={rider['rider_id']}"
            )
            try:
                rider_soup = self._get_soup(breakdown_url)
            except requests.RequestException:
                continue

            rider_tables = rider_soup.select("table")
            if len(rider_tables) < 2:
                continue

            breakdown_table = rider_tables[1]
            rider_bonus_seconds = 0.0
            for row in breakdown_table.select("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue

                stage_label = " ".join(cells[0].get_text(" ", strip=True).split())
                title = " ".join(cells[1].get_text(" ", strip=True).split())
                value_text = " ".join(cells[2].get_text(" ", strip=True).split())

                stage_pattern = rf"^Stage\s+{int(stage_number)}(?:\b|\s*\()"
                if re.match(stage_pattern, stage_label) is None:
                    continue
                if "Red Bull KM" not in title:
                    continue

                numeric_match = re.search(r"-?\d+(?:\.\d+)?", value_text)
                if numeric_match is None:
                    continue
                rider_bonus_seconds += float(numeric_match.group(0))

            if rider_bonus_seconds > 0:
                key = _normalize_name(rider["name"])
                red_bull_bonus_by_name[key] = red_bull_bonus_by_name.get(key, 0.0) + rider_bonus_seconds
                team_by_name.setdefault(key, rider.get("team", ""))

        ranked_rows = sorted(
            red_bull_bonus_by_name.items(),
            key=lambda item: (-item[1], item[0]),
        )
        sprint_points_by_position = {1: 10, 2: 8, 3: 6, 4: 4, 5: 2}
        results = [
            {
                "position": idx,
                "name": rider_name,
                "team": team_by_name.get(rider_name, ""),
                "value": str(sprint_points_by_position.get(idx, 0)),
            }
            for idx, (rider_name, _) in enumerate(ranked_rows, start=1)
            if sprint_points_by_position.get(idx, 0) > 0
        ]

        return {
            "year": year,
            "stage_number": stage_number,
            "url": boni_url,
            "fetched_at": datetime.now(UTC).isoformat(),
            "results": results,
            "source": "pcs-most-bonifications",
        }

    def scrape_giro_startlist(self, year: int = 2025) -> list[dict]:
        """Scrape Giro startlist riders and DS from ProCyclingStats for a given year."""
        url = f"{self.base_url}/race/giro-d-italia/{year}/startlist/startlist"
        errors: list[str] = []

        try:
            soup = self._get_soup(url)
            rows = self._parse_startlist_rows(soup, year=year)
            if not rows:
                rows = self._parse_startlist_rows_modern(soup)
            if rows:
                rows = self.enrich_rows_with_specialty_scores(rows, year=year)
                self._save_cache(year, rows)
                return rows
        except requests.RequestException as exc:
            errors.append(f"direct fetch: {exc}")

        try:
            text = self._get_jina_text(url)
            rows = self._parse_startlist_markdown(text, year=year)
            if rows:
                rows = self.enrich_rows_with_specialty_scores(rows, year=year)
                self._save_cache(year, rows)
                return rows
            errors.append("mirror fetch: no parseable rows")
        except requests.RequestException as exc:
            errors.append(f"mirror fetch: {exc}")

        cached_rows = self._load_cache(year)
        if cached_rows:
            enriched_cache = self.enrich_rows_with_specialty_scores(cached_rows, year=year)
            if enriched_cache:
                self._save_cache(year, enriched_cache)
                return enriched_cache
            return cached_rows

        bootstrap_rows = self._load_bootstrap_startlist(year)
        if bootstrap_rows:
            enriched_bootstrap = self.enrich_rows_with_specialty_scores(bootstrap_rows, year=year)
            if enriched_bootstrap:
                self._save_cache(year, enriched_bootstrap)
                return enriched_bootstrap
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
        best_payload: dict | None = None
        best_count = 0

        def _consider(payload: dict, source: str) -> bool:
            nonlocal best_payload, best_count
            rows = payload.get("results", [])
            if not isinstance(rows, list) or not rows:
                errors.append(f"{source}: no parseable rows")
                return False

            payload["source"] = source
            row_count = len(rows)
            if row_count > best_count:
                best_payload = payload
                best_count = row_count

            # We only score top 20 for stage placings, so stop searching when we have enough rows.
            return row_count >= 20

        try:
            soup = self._get_soup(url)
            payload = self._parse_stage_results_html(soup, stage_number=stage_number, year=year)
            if _consider(payload, "direct"):
                self._save_stage_results_cache(year, stage_number, payload)
                return payload
        except requests.RequestException as exc:
            errors.append(f"direct fetch: {exc}")

        try:
            text = self._get_jina_text(url)
            payload = self._parse_stage_results_markdown(text, stage_number=stage_number, year=year)
            if _consider(payload, "mirror"):
                self._save_stage_results_cache(year, stage_number, payload)
                return payload
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
                if _consider(payload, "firstcycling"):
                    self._save_stage_results_cache(year, stage_number, payload)
                    return payload
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
                if _consider(payload, "firstcycling-mirror"):
                    self._save_stage_results_cache(year, stage_number, payload)
                    return payload
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
            if _consider(payload, "cyclingstage"):
                self._save_stage_results_cache(year, stage_number, payload)
                return payload
        except requests.RequestException as exc:
            errors.append(f"cyclingstage fetch: {exc}")

        cached_payload = self._load_stage_results_cache(year, stage_number)
        if cached_payload is not None:
            cached_rows = cached_payload.get("results", [])
            if isinstance(cached_rows, list) and len(cached_rows) > best_count:
                return cached_payload

        if best_payload is not None:
            self._save_stage_results_cache(year, stage_number, best_payload)
            return best_payload

        joined = " | ".join(errors) if errors else "unknown error"
        raise RuntimeError(
            f"Unable to fetch results for stage {stage_number} in {year}. No parseable result and no local cache found ({joined})."
        )
