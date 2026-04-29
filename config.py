# Configuration for the Giro Fantasy Cycling Game

# Runtime season setting
SEASON_YEAR = 2026

# App compatibility constants used by Streamlit UI
TEAM_COMPOSITION = {
	'captain': 3,
	'sprinter': 2,
	'climber': 2,
	'youth': 2,
	'water_carrier': 3,
	'ds': 1,
}

PRICE_RANGES = {
	'captain': (1.0, 1.8),
	'sprinter': (1.2, 1.9),
	'climber': (0.6, 1.4),
	'youth': (0.5, 1.0),
	'water_carrier': (0.5, 0.9),
	'ds': (1.0, 1.8),
}

BUDGET_LIMIT = 15
MAX_TRANSFERS = 12
PLAYERS = [
	'Per Kristian',
	'Erik',
	'Atle',
	'Espen',
]

# Player PINs for authentication (change these to unique 4-digit codes)
PLAYER_PINS = {
	'Per Kristian': '1234',
	'Erik': '5678',
	'Atle': '9012',
	'Espen': '3456',
}
