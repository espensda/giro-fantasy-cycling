# Configuration for the Giro Fantasy Cycling Game

# Game Settings
GAME_NAME = 'Giro Fantasy Cycling'
SEASON_YEAR = 2026
PRIZE_POOL = 10000  # Total prize pool for the competition

# User Settings
DEFAULT_TEAM_SIZE = 12  # Number of cyclists in a team (excluding DS)
ALLOW_SUBSTITUTIONS = True  # Allow users to make substitutions

# Scoring Settings
POINTS_FOR_WIN = 50  # Points for winning stage
POINTS_FOR_SECOND_PLCE = 30  # Points for second place
POINTS_FOR_TOP_10 = 10  # Points for finishing in top 10

# URL Settings
API_BASE_URL = 'https://api.girofantasy.com'  # Base URL for the game API

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
	'Player 1',
	'Player 2',
	'Player 3',
	'Player 4',
]
