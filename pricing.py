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
        'captain_score',
        'sprinter_score',
        'climber_score',
        'water_carrier_score',
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


def _top20_quality(rank_value) -> float:
    rank = _safe_float(rank_value)
    if rank is None:
        return 0.0
    if rank <= 0:
        return 0.0
    if rank > 20:
        return 0.0
    return max(0.0, (21.0 - rank) / 20.0)


def _rank_quality(rank_value, depth: int = 120, curve: float = 0.85) -> float:
    """Return rank quality in [0, 1] using broader list depth.

    This is used for pricing variation, not role assignment. Role assignment
    stays strict top-20 via `_top20_quality` and `_enforce_list_categories`.
    """
    rank = _safe_float(rank_value)
    if rank is None or rank <= 0:
        return 0.0
    if rank > depth:
        return 0.0

    normalized = (depth + 1.0 - rank) / depth
    normalized = max(0.0, min(1.0, normalized))
    return normalized ** curve


def _price_from_quality(category: str, quality: float) -> float:
    low, high = PRICE_RANGES.get(category, (0.5, 1.0))
    clamped = max(0.0, min(1.0, float(quality)))
    return round(low + ((high - low) * clamped), 1)


def _enforce_list_categories(rows: list[dict]) -> list[dict]:
    """Apply strict category rules from list ranks and age when rank metadata exists."""
    if not any(any(key in row for key in ('gc_rank', 'sprinter_rank', 'climber_rank')) for row in rows):
        return rows

    updated: list[dict] = []
    for row in rows:
        item = dict(row)
        if str(item.get('category', '')).strip().lower() == 'ds':
            updated.append(item)
            continue

        gc_rank = _safe_float(item.get('gc_rank'))
        spr_rank = _safe_float(item.get('sprinter_rank'))
        clm_rank = _safe_float(item.get('climber_rank'))
        age = _safe_float(item.get('age'))

        candidates: list[tuple[str, float]] = []
        if gc_rank is not None and 0 < gc_rank <= 20:
            candidates.append(('captain', gc_rank))
        if spr_rank is not None and 0 < spr_rank <= 20:
            candidates.append(('sprinter', spr_rank))
        if clm_rank is not None and 0 < clm_rank <= 20:
            candidates.append(('climber', clm_rank))

        if candidates:
            role = sorted(candidates, key=lambda item: item[1])[0][0]
        elif age is not None and age <= 25:
            role = 'youth'
        else:
            role = 'water_carrier'

        item['category'] = role
        item['youth'] = role == 'youth'
        item['role'] = role
        updated.append(item)
    return updated


def _list_based_quality(row: dict) -> float | None:
    """Compute quality directly from PCS list placement and age rules."""
    category = str(row.get('category', '')).strip().lower()

    gc_q = _top20_quality(row.get('gc_rank'))
    spr_q = _top20_quality(row.get('sprinter_rank'))
    clm_q = _top20_quality(row.get('climber_rank'))
    tt_q = _top20_quality(row.get('tt_rank'))
    classic_q = _top20_quality(row.get('classic_rank'))

    # Broader list depth used for pricing variation among youth/water carriers.
    gc_ext = _rank_quality(row.get('gc_rank'))
    spr_ext = _rank_quality(row.get('sprinter_rank'))
    clm_ext = _rank_quality(row.get('climber_rank'))
    tt_ext = _rank_quality(row.get('tt_rank'))
    classic_ext = _rank_quality(row.get('classic_rank'))

    # Score fallback captures riders who are not in top slices of specialty pages.
    score_fallback = max(
        0.0,
        min(
            1.0,
            max(
                (_safe_float(row.get('captain_score')) or 0.0) / 100.0,
                (_safe_float(row.get('sprinter_score')) or 0.0) / 100.0,
                (_safe_float(row.get('climber_score')) or 0.0) / 100.0,
                (_safe_float(row.get('water_carrier_score')) or 0.0) / 100.0,
            ),
        ),
    )

    if category == 'captain':
        return gc_q
    if category == 'sprinter':
        return spr_q
    if category == 'climber':
        return clm_q
    if category == 'water_carrier':
        # TT/classics remain primary, but broader lists + fallback score add spread.
        primary_q = max(tt_ext, classic_ext)
        secondary_q = max(gc_ext, spr_ext, clm_ext)
        return min(1.0, (primary_q * 0.6) + (secondary_q * 0.25) + (score_fallback * 0.15))
    if category == 'youth':
        age = _safe_float(row.get('age'))
        if age is None:
            age_q = 0.35
        else:
            age_q = max(0.0, min(1.0, (26.0 - age) / 8.0))

        # Youth riders get upside from broader specialty placement (e.g. decent sprinters).
        specialty_q = max(
            spr_ext,
            gc_ext * 0.95,
            clm_ext * 0.95,
            tt_ext * 0.85,
            classic_ext * 0.85,
        )
        return min(1.0, (age_q * 0.45) + (specialty_q * 0.4) + (score_fallback * 0.15))
    return None


def assign_prices(rows: list[dict]) -> list[dict]:
    """Assign prices to rows based on PCS list placement and category quality.

    With specialty list metadata present (e.g. `gc_rank`, `sprinter_rank`, `climber_rank`,
    `tt_rank`, `classic_rank`), prices are computed directly from list placement.
    Otherwise, falls back to rank-within-category blending using available quality scores.
    """
    recategorized = _enforce_list_categories(_maybe_recategorize_rows(rows))
    normalized_quality = _normalized_quality_map(recategorized)

    has_specialty_ranks = any(
        any(key in row for key in ('gc_rank', 'sprinter_rank', 'climber_rank', 'tt_rank', 'classic_rank'))
        for row in recategorized
    )

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

    if has_specialty_ranks:
        for index, row in enumerate(recategorized):
            category = row.get('category', 'water_carrier')
            if category == 'ds':
                prices_by_index[index] = pricing_engine(category)
                continue

            quality = _list_based_quality(row)
            if quality is None:
                prices_by_index[index] = pricing_engine(category)
            else:
                # Blend in normalized quality for categories with many close riders.
                if category in {'youth', 'water_carrier'}:
                    extra_quality = normalized_quality.get(index)
                    if extra_quality is not None:
                        quality = (quality * 0.8) + (extra_quality * 0.2)
                prices_by_index[index] = _price_from_quality(category, quality)

        priced_rows = []
        for index, row in enumerate(recategorized):
            priced_row = dict(row)
            priced_row['price'] = prices_by_index.get(index, pricing_engine(row['category']))
            priced_rows.append(priced_row)
        return priced_rows

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