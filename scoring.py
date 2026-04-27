"""Scoring helpers for calculating leaderboard totals from saved stage results."""

from __future__ import annotations


class ScoringSystem:
    def __init__(self) -> None:
        self.stage_points = {
            1: 90,
            2: 80,
            3: 60,
            4: 50,
            5: 40,
            6: 36,
            7: 32,
            8: 28,
            9: 24,
            10: 21,
            11: 18,
            12: 15,
            13: 12,
            14: 10,
            15: 8,
            16: 6,
            17: 4,
            18: 3,
            19: 2,
            20: 1,
        }
        self.stage_winner_team_rider_bonus = 10
        self.stage_winner_team_bonus = 10
        self.category_multiplier = {
            'captain': 2,
            'sprinter': 2,
            'climber': 3,
            'water_carrier': 4,
            'youth': 4,
        }
        self.classification_points = {
            'gc': {
                1: 25,
                2: 20,
                3: 16,
                4: 13,
                5: 10,
                6: 8,
                7: 6,
                8: 4,
                9: 2,
                10: 1,
            },
            'mountains': {
                1: 10,
                2: 8,
                3: 6,
                4: 4,
                5: 2,
            },
            'sprints': {
                1: 10,
                2: 8,
                3: 6,
                4: 4,
                5: 2,
            },
        }

    def calculate_stage_score(self, position: int) -> int:
        """Return points for an individual stage placing."""
        return self.stage_points.get(position, 0)

    def calculate_rider_stage_score(
        self,
        position: int,
        rider_team: str,
        winning_team: str | None,
        category: str,
    ) -> int:
        """Return fantasy rider points for a stage with team bonus and category multiplier."""
        base_points = self.calculate_stage_score(position)
        if winning_team and rider_team == winning_team:
            base_points += self.stage_winner_team_rider_bonus

        multiplier = self.category_multiplier.get(category, 1)
        return base_points * multiplier

    def calculate_uploaded_rider_points(
        self,
        base_points: float,
        rider_team: str,
        winning_team: str | None,
        category: str,
    ) -> float:
        """Convert uploaded base points into fantasy points.

        Uploaded points are assumed to be base points before winner-team bonus
        and category multiplier are applied.
        """
        adjusted_points = float(base_points or 0)
        if winning_team and rider_team == winning_team:
            adjusted_points += self.stage_winner_team_rider_bonus

        multiplier = self.category_multiplier.get(category, 1)
        return adjusted_points * multiplier

    def calculate_ds_stage_score(self, ds_team: str, stage_rows: list[tuple]) -> int:
        """DS scores from the top five finishers on the selected team plus a winner bonus."""
        team_points = [
            self.calculate_stage_score(position)
            for _, position, _, team, _, _ in stage_rows
            if team == ds_team
        ]
        score = sum(sorted(team_points, reverse=True)[:5])

        winning_team = next((team for _, position, _, team, _, _ in stage_rows if position == 1), None)
        if winning_team == ds_team:
            score += self.stage_winner_team_bonus
        return score

    def calculate_classification_score(self, classification: str, position: int, value: str = "") -> int:
        """Return points for a classification entry.

        Uses explicit uploaded points when provided, and falls back to legacy
        position-based tables for older saved data.
        """
        cleaned_value = str(value or "").strip()
        if cleaned_value:
            try:
                return int(float(cleaned_value))
            except ValueError:
                pass

        return self.classification_points.get(classification, {}).get(position, 0)


def calculate_leaderboard(
    players: list[str],
    player_teams: dict[str, list[tuple]],
    stage_results: list[tuple],
    classification_results: dict[str, list[tuple]] | None = None,
    stage_points: list[tuple[int, str, str, int, float]] | None = None,
) -> tuple[list[dict], dict[str, list[dict]]]:
    """Build total leaderboard rows and per-player stage breakdowns from saved results."""
    scoring = ScoringSystem()
    classification_results = classification_results or {}
    stage_points = stage_points or []

    stages: dict[int, list[tuple]] = {}
    for row in stage_results:
        stage_number = row[0]
        stages.setdefault(stage_number, []).append(row)

    classifications_by_stage: dict[int, dict[str, list[tuple]]] = {}
    for classification_key, rows in classification_results.items():
        for row in rows:
            stage_number = row[0]
            classifications_by_stage.setdefault(stage_number, {}).setdefault(classification_key, []).append(row)

    uploaded_points_by_stage: dict[int, list[tuple[str, str, int, float]]] = {}
    for stage_number, rider_name, rider_team, rider_id, points in stage_points:
        uploaded_points_by_stage.setdefault(stage_number, []).append(
            (rider_name, rider_team, rider_id, float(points))
        )

    stages_to_score = sorted(set(stages.keys()) | set(classifications_by_stage.keys()) | set(uploaded_points_by_stage.keys()))

    leaderboard_rows: list[dict] = []
    breakdown_by_player: dict[str, list[dict]] = {}

    for player in players:
        team_rows = player_teams.get(player, [])
        selected_rider_meta = {
            row[0]: {
                'team': row[2],
                'category': row[3],
            }
            for row in team_rows
            if row[3] != 'ds'
        }
        selected_rider_ids = set(selected_rider_meta.keys())
        ds_row = next((row for row in team_rows if row[3] == 'ds'), None)
        ds_team = ds_row[2] if ds_row else None
        ds_rider_id = ds_row[0] if ds_row else None

        player_breakdown: list[dict] = []
        total_points = 0

        for stage_number in stages_to_score:
            stage_rows = stages.get(stage_number, [])
            uploaded_stage_points = uploaded_points_by_stage.get(stage_number)
            if uploaded_stage_points:
                uploaded_points_by_rider_id: dict[int, float] = {}
                for _, _, rider_id, points in uploaded_stage_points:
                    uploaded_points_by_rider_id[rider_id] = (
                        uploaded_points_by_rider_id.get(rider_id, 0.0) + float(points)
                    )

                winning_team = next((team for _, position, _, team, _, _ in stage_rows if position == 1), None)
                if winning_team is None and uploaded_stage_points:
                    winner_row = max(uploaded_stage_points, key=lambda row: row[3])
                    winning_team = winner_row[1]

                rider_points = 0
                for rider_id, rider_meta in selected_rider_meta.items():
                    rider_points += scoring.calculate_uploaded_rider_points(
                        base_points=uploaded_points_by_rider_id.get(rider_id, 0),
                        rider_team=rider_meta['team'],
                        winning_team=winning_team,
                        category=rider_meta['category'],
                    )

                ds_points = scoring.calculate_ds_stage_score(ds_team, stage_rows) if ds_team and stage_rows else 0
                classification_points = 0
            else:
                winning_team = next((team for _, position, _, team, _, _ in stage_rows if position == 1), None)
                stage_position_by_rider_id = {
                    rider_id: position
                    for _, position, _, _, rider_id, _ in stage_rows
                }
                rider_points = 0
                for rider_id, rider_meta in selected_rider_meta.items():
                    rider_points += scoring.calculate_rider_stage_score(
                        position=stage_position_by_rider_id.get(rider_id, 0),
                        rider_team=rider_meta['team'],
                        winning_team=winning_team,
                        category=rider_meta['category'],
                    )
                ds_points = scoring.calculate_ds_stage_score(ds_team, stage_rows) if ds_team else 0

                stage_classification_rows = classifications_by_stage.get(stage_number, {})
                classification_points = 0
                for classification_key, rows in stage_classification_rows.items():
                    classification_points += sum(
                        scoring.calculate_classification_score(classification_key, position, value)
                        for _, _, position, _, _, rider_id, value in rows
                        if rider_id in selected_rider_ids
                    )

            stage_total = rider_points + ds_points + classification_points
            total_points += stage_total

            player_breakdown.append(
                {
                    'Stage': stage_number,
                    'Rider Points': rider_points,
                    'DS Points': ds_points,
                    'Classification Points': classification_points,
                    'Stage Total': stage_total,
                }
            )

        breakdown_by_player[player] = player_breakdown
        leaderboard_rows.append(
            {
                'Player': player,
                'Total Points': total_points,
                'Stages Scored': len(player_breakdown),
            }
        )

    leaderboard_rows.sort(key=lambda row: (-row['Total Points'], row['Player']))
    for idx, row in enumerate(leaderboard_rows, start=1):
        row['Rank'] = idx

    return leaderboard_rows, breakdown_by_player