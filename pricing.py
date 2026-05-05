"""Pricing logic for assigning fantasy prices to riders and DS staff."""

import math

from config import PRICE_RANGES


class Rider:
    def __init__(self, name, category):
        self.name = name
        self.category = category
        self.price = 0

    def assign_price(self):
        self.price = pricing_engine(self.category)

    def __str__(self):
        return f'Rider: {self.name}, Category: {self.category}, Price: {self.price}'


def pricing_engine(category: str, rank: int | None = None, total: int | None = None) -> float:
    """Return a rider price within the configured category range.

    When rank and total are provided, earlier ranks get higher prices.
    """
    low, high = PRICE_RANGES.get(category, (0.5, 1.0))

    if rank is None or total is None or total <= 0:
        return round((low + high) / 2, 1)

    if total == 1:
        return round(high, 1)

    clamped_rank = max(0, min(rank, total - 1))
    quality_ratio = 1 - (clamped_rank / (total - 1))
    return round(low + ((high - low) * quality_ratio), 1)


def _safe_float(value) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if text == "":
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _extract_quality_score(row: dict) -> float | None:
    for key in (
        'web_score',
        'score',
        'uci_points',
        'fantasy_score',
    ):
        parsed = _safe_float(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _normalized_quality_map(rows: list[dict]) -> dict[int, float]:
    """Return quality percentile by row index in [0, 1]."""
    scored: list[tuple[int, float]] = []
    for index, row in enumerate(rows):
        score = _extract_quality_score(row)
        if score is None or row.get('category') == 'ds':
            continue
        scored.append((index, score))

    if not scored:
        return {}

    values = [value for _, value in scored]
    low = min(values)
    high = max(values)
    if math.isclose(low, high):
        return {index: 0.5 for index, _ in scored}

    return {
        index: (value - low) / (high - low)
        for index, value in scored
    }


def _role_from_score_hints(row: dict) -> str | None:
    """Infer rider role when optional hint columns are provided."""
    role_text = str(row.get('role', '')).strip().lower()
    if role_text:
        if role_text in {'captain', 'gc', 'general_classification'}:
            return 'captain'
        if role_text in {'sprinter', 'sprint'}:
            return 'sprinter'
        if role_text in {'climber', 'climb'}:
            return 'climber'
        if role_text in {'youth', 'u25'}:
            return 'youth'
        if role_text in {'water_carrier', 'domestique', 'helper'}:
            return 'water_carrier'

    scored_roles = {
        'captain': _safe_float(row.get('captain_score')),
        'sprinter': _safe_float(row.get('sprinter_score')),
        'climber': _safe_float(row.get('climber_score')),
        'water_carrier': _safe_float(row.get('water_carrier_score')),
    }
    valid = {role: value for role, value in scored_roles.items() if value is not None}
    if not valid:
        return None
    return max(valid.items(), key=lambda item: item[1])[0]


def _maybe_recategorize_rows(rows: list[dict]) -> list[dict]:
    """Apply optional category hints from uploaded rating data.

    Existing explicit categories from scraping remain unchanged unless a role hint
    is available for a specific row.
    """
    updated: list[dict] = []
    for row in rows:
        item = dict(row)
        if item.get('category') == 'ds':
            updated.append(item)
            continue

        hinted_role = _role_from_score_hints(item)
        if hinted_role:
            item['category'] = hinted_role
            item['youth'] = hinted_role == 'youth'
        updated.append(item)
    return updated


def assign_prices(rows: list[dict]) -> list[dict]:
    """Assign prices to rows based on rank within each category.

    If rows include optional web-derived strength fields (`web_score`, `score`, `uci_points`),
    those are used to rank riders inside each category before pricing.
    """
    recategorized = _maybe_recategorize_rows(rows)
    normalized_quality = _normalized_quality_map(recategorized)

    category_indices: dict[str, list[int]] = {}
    for index, row in enumerate(recategorized):
        category = row['category']
        category_indices.setdefault(category, []).append(index)

    def _strength(row: dict, fallback_rank: int) -> float:
        for key in ('web_score', 'score', 'uci_points'):
            value = row.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        # Preserve existing ordering when no web-derived score is available.
        return float(-fallback_rank)

    prices_by_index: dict[int, float] = {}
    for category, indices in category_indices.items():
        ranked_indices = sorted(
            indices,
            key=lambda idx: _strength(rows[idx], fallback_rank=indices.index(idx)),
            reverse=True,
        )
        total = len(ranked_indices)
        for rank, index in enumerate(ranked_indices):
            base_price = pricing_engine(category, rank=rank, total=total)
            # Blend category rank price with global quality percentile when available.
            quality = normalized_quality.get(index)
            if quality is None:
                prices_by_index[index] = base_price
                continue

            low, high = PRICE_RANGES.get(category, (0.5, 1.0))
            quality_price = round(low + ((high - low) * quality), 1)
            prices_by_index[index] = round((base_price * 0.6) + (quality_price * 0.4), 1)

    priced_rows = []
    for index, row in enumerate(recategorized):
        priced_row = dict(row)
        priced_row['price'] = prices_by_index.get(index, pricing_engine(row['category']))
        priced_rows.append(priced_row)

    return priced_rows