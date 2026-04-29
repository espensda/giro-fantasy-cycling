"""Pricing logic for assigning fantasy prices to riders and DS staff."""

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


def assign_prices(rows: list[dict]) -> list[dict]:
    """Assign prices to rows based on rank within each category.

    If rows include optional web-derived strength fields (`web_score`, `score`, `uci_points`),
    those are used to rank riders inside each category before pricing.
    """
    category_indices: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
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
            prices_by_index[index] = pricing_engine(category, rank=rank, total=total)

    priced_rows = []
    for index, row in enumerate(rows):
        priced_row = dict(row)
        priced_row['price'] = prices_by_index.get(index, pricing_engine(row['category']))
        priced_rows.append(priced_row)

    return priced_rows