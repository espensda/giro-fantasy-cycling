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
    """Assign prices to rows based on position within each category."""
    category_totals: dict[str, int] = {}
    category_seen: dict[str, int] = {}

    for row in rows:
        category = row['category']
        category_totals[category] = category_totals.get(category, 0) + 1

    priced_rows = []
    for row in rows:
        category = row['category']
        rank = category_seen.get(category, 0)
        category_seen[category] = rank + 1

        priced_row = dict(row)
        priced_row['price'] = pricing_engine(category, rank=rank, total=category_totals[category])
        priced_rows.append(priced_row)

    return priced_rows