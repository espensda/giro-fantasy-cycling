# Scoring System for Giro Fantasy Cycling

"""
This module calculates the fantasy scoring for the Giro cycling event based on various parameters including stage points, general classification bonuses, category multipliers, and directeur sportif (DS) scoring.
"""

class ScoringSystem:
    def __init__(self):
        # Define stage points for the top finishers
        self.stage_points = {
            1: 50,
            2: 40,
            3: 30,
            4: 25,
            5: 20,
            6: 15,
            7: 10,
            8: 5,
            9: 3,
            10: 1,
        }

        # Define general classification bonuses
        self.gc_bonuses = {
            "overall_winner": 100,
            "second_place": 50,
            "third_place": 25,
        }

        # Category multipliers (1-5)
        self.category_multipliers = {
            "category_1": 2,
            "category_2": 1.5,
            "category_3": 1.2,
            "category_4": 1.1,
            "category_5": 1.0,
        }

        # Directeur sportif scoring
        self.ds_points = 10  # Points for each entry by a DS

    def calculate_stage_score(self, position):
        """Calculate the score based on finishing position in a stage."""
        return self.stage_points.get(position, 0)

    def calculate_gc_bonus(self, position):
        """Calculate GC bonus points based on overall standings."""
        if position in self.gc_bonuses:
            return self.gc_bonuses[position]
        return 0

    def calculate_category_score(self, category, base_score):
        """Calculate score with category multiplier."""
        multiplier = self.category_multipliers.get(category, 1)
        return base_score * multiplier

    def calculate_ds_score(self, entries):
        """Calculate total points for directeur sportif entries."""
        return self.ds_points * entries

# Example of how to use the scoring system
if __name__ == '__main__':
    scoring = ScoringSystem()
    print("Stage Score (2nd Place):", scoring.calculate_stage_score(2))
    print("GC Bonus (Overall Winner):", scoring.calculate_gc_bonus('overall_winner'))
    print("Category Score (category_1 with base score 30):", scoring.calculate_category_score('category_1', 30))
    print("DS Score (3 entries):", scoring.calculate_ds_score(3))