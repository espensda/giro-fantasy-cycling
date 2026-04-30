"""
Main Streamlit application for Giro Fantasy Cycling
"""
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import streamlit as st
import pandas as pd
from difflib import get_close_matches
from database import (
    init_db, add_player, add_rider, get_all_riders, get_riders_by_category,
    save_player_team, get_player_team, count_transfers, add_transfer, clear_riders,
    save_stage_results, get_stage_results, save_classification_results, get_classification_results,
    save_stage_points, get_stage_points, update_rider_overrides,
    set_rider_lock, set_all_riders_lock, get_all_riders_with_lock,
    get_database_file_info, list_database_backups, create_database_backup, restore_database_backup,
    read_database_backup_bytes, get_current_database_counts, get_backup_database_counts,
    restore_database_from_bytes,
    upsert_rider_withdrawal, get_rider_withdrawals, delete_rider_withdrawal,
)
from config import TEAM_COMPOSITION, PRICE_RANGES, BUDGET_LIMIT, MAX_TRANSFERS, PLAYERS, SEASON_YEAR, PLAYER_PINS
from pricing import assign_prices
from scraper import GiroScraper
from scoring import calculate_leaderboard, ScoringSystem

# Page config
st.set_page_config(page_title="Giro Fantasy Cycling", layout="wide")

# Initialize session state
if 'db_initialized' not in st.session_state:
    init_db()
    # Auto-seed riders from bootstrap if the DB is empty (e.g. fresh cloud deploy)
    if not get_all_riders():
        try:
            scraper = GiroScraper()
            rows = scraper.scrape_giro_startlist(year=SEASON_YEAR)
            for row in rows:
                add_rider(
                    name=row['name'],
                    team=row.get('team', ''),
                    category=row.get('category', 'water_carrier'),
                    price=row.get('price', 0),
                    is_youth=row.get('is_youth', False),
                )
            assign_prices(get_all_riders())
        except Exception:
            pass  # silently skip; admin can trigger manually
    st.session_state.db_initialized = True


TEAM_CATEGORY_ORDER = ['captain', 'sprinter', 'climber', 'youth', 'water_carrier', 'ds']
TEAM_CATEGORY_LABELS = {
    'captain': 'Captain',
    'sprinter': 'Sprinter',
    'climber': 'Climber',
    'youth': 'Youth',
    'water_carrier': 'Water Carrier',
    'ds': 'DS',
}

CLASSIFICATION_LABELS = {
    'gc': 'GC Points',
    'mountains': 'Mountain Points',
    'sprints': 'Sprint Points',
}

RACE_TIMEZONE = ZoneInfo("Europe/Rome")
FIRST_STAGE_START_LOCAL = datetime(SEASON_YEAR, 5, 8, 14, 0, tzinfo=RACE_TIMEZONE)
FINAL_STAGE_DATE = date(SEASON_YEAR, 5, 31)
TRANSFER_CUTOFF_DEFAULT = time(12, 0)
TRANSFER_OPEN_AFTER_RACE = time(21, 0)
REST_DAYS = {
    date(SEASON_YEAR, 5, 11),
    date(SEASON_YEAR, 5, 18),
    date(SEASON_YEAR, 5, 25),
}


def _giro_race_dates() -> set[date]:
    """Return all race dates for the season, excluding configured rest days."""
    race_dates: set[date] = set()
    current_day = date(SEASON_YEAR, 5, 8)
    while current_day <= FINAL_STAGE_DATE:
        if current_day not in REST_DAYS:
            race_dates.add(current_day)
        current_day = date.fromordinal(current_day.toordinal() + 1)
    return race_dates


GIRO_RACE_DATES = _giro_race_dates()


def get_game_window_status(now: datetime | None = None) -> dict[str, str | bool]:
    """Compute whether team selection/transfers are currently open in local Giro time."""
    now_local = now.astimezone(RACE_TIMEZONE) if now else datetime.now(RACE_TIMEZONE)

    if now_local < FIRST_STAGE_START_LOCAL:
        return {
            "team_selection_open": True,
            "transfers_open": False,
            "team_selection_message": (
                f"Team selection is open until {FIRST_STAGE_START_LOCAL.strftime('%d %b %Y %H:%M')} "
                f"({RACE_TIMEZONE.key})."
            ),
            "transfers_message": (
                "Transfers open after stage 1 has started. "
                f"First transfer window begins after {FIRST_STAGE_START_LOCAL.strftime('%d %b %Y %H:%M')} "
                f"({RACE_TIMEZONE.key})."
            ),
        }

    current_day = now_local.date()
    current_time = now_local.time()

    if current_day > FINAL_STAGE_DATE:
        return {
            "team_selection_open": False,
            "transfers_open": False,
            "team_selection_message": "Team selection is locked after stage 1 start.",
            "transfers_message": "Transfers are closed because the Giro stage calendar has ended.",
        }

    if current_day in REST_DAYS:
        return {
            "team_selection_open": False,
            "transfers_open": True,
            "team_selection_message": "Team selection is locked after stage 1 start.",
            "transfers_message": "Rest day: transfers are open all day.",
        }

    if current_day in GIRO_RACE_DATES:
        # Transfer windows on race days:
        # - Open from 00:00 to 12:00 (deadline before the stage)
        # - Closed during race/scoring period from 12:00 to 21:00
        # - Open again from 21:00 onward
        if current_time < TRANSFER_CUTOFF_DEFAULT or current_time >= TRANSFER_OPEN_AFTER_RACE:
            return {
                "team_selection_open": False,
                "transfers_open": True,
                "team_selection_message": "Team selection is locked after stage 1 start.",
                "transfers_message": (
                    f"Race day transfer window is open. Daily lock period is "
                    f"{TRANSFER_CUTOFF_DEFAULT.strftime('%H:%M')}-{TRANSFER_OPEN_AFTER_RACE.strftime('%H:%M')} "
                    f"({RACE_TIMEZONE.key})."
                ),
            }

        return {
            "team_selection_open": False,
            "transfers_open": False,
            "team_selection_message": "Team selection is locked after stage 1 start.",
            "transfers_message": (
                f"Race day lock: transfers are closed between "
                f"{TRANSFER_CUTOFF_DEFAULT.strftime('%H:%M')} and "
                f"{TRANSFER_OPEN_AFTER_RACE.strftime('%H:%M')} ({RACE_TIMEZONE.key}) "
                f"to allow stage scoring to be finalized first."
            ),
        }

    return {
        "team_selection_open": False,
        "transfers_open": True,
        "team_selection_message": "Team selection is locked after stage 1 start.",
        "transfers_message": "Transfers are open.",
    }


def _build_ds_rest_day_rows_by_player(
    player_teams: dict[str, list[tuple]],
    withdrawals: list[tuple[int, str, int, str, str, str]],
) -> dict[str, list[dict]]:
    """Return per-player rest-day DS bonus/penalty rows from withdrawal records."""
    today_local = datetime.now(RACE_TIMEZONE).date()
    eligible_rest_days = sorted(day for day in REST_DAYS if day <= today_local)
    if not eligible_rest_days:
        return {player: [] for player in PLAYERS}

    withdrawal_events: list[tuple[date, str]] = []
    for _, withdrawal_date_str, _, _, rider_team, _ in withdrawals:
        try:
            withdrawal_day = datetime.fromisoformat(str(withdrawal_date_str)).date()
        except ValueError:
            continue
        withdrawal_events.append((withdrawal_day, rider_team))

    rows_by_player: dict[str, list[dict]] = {}
    for player in PLAYERS:
        team_rows = player_teams.get(player, [])
        ds_row = next((row for row in team_rows if row[3] == 'ds'), None)
        ds_team = ds_row[2] if ds_row else None

        player_rows: list[dict] = []
        for idx, rest_day in enumerate(eligible_rest_days, start=1):
            if ds_team is None:
                ds_points = 0
            else:
                if idx == 1:
                    withdrawal_count = sum(
                        1
                        for withdrawal_day, rider_team in withdrawal_events
                        if rider_team == ds_team
                        and FIRST_STAGE_START_LOCAL.date() <= withdrawal_day <= rest_day
                    )
                else:
                    prev_rest_day = eligible_rest_days[idx - 2]
                    withdrawal_count = sum(
                        1
                        for withdrawal_day, rider_team in withdrawal_events
                        if rider_team == ds_team
                        and prev_rest_day < withdrawal_day <= rest_day
                    )

                ds_points = 100 if withdrawal_count == 0 else (-30 * withdrawal_count)

            player_rows.append(
                {
                    'Stage': f"Rest Day {idx} ({rest_day.isoformat()})",
                    'Rider Points': 0,
                    'DS Points': ds_points,
                    'Classification Points': 0,
                    'Stage Total': ds_points,
                }
            )

        rows_by_player[player] = player_rows

    return rows_by_player

def read_excel_with_normalized_columns(file_obj):
    """Read an Excel file and normalize column names."""
    try:
        file_obj.seek(0)
        df = pd.read_excel(file_obj)
    except Exception as exc:
        raise ValueError(
            "Could not read Excel file. Please upload a valid .xlsx file."
        ) from exc

    df.columns = df.columns.str.strip().str.lower()
    return df

def validate_columns(df, required_cols):
    """Validate that uploaded data has required columns (case-insensitive).
    
    Returns list of missing column names, or empty list if all found.
    """
    df_cols_lower = set(df.columns.str.lower())
    missing = [col for col in required_cols if col.lower() not in df_cols_lower]
    return missing

def fuzzy_match_rider_name(csv_name, database_names, cutoff=0.80):
    """Find closest matching rider name in database using fuzzy matching.
    
    Args:
        csv_name: Name from uploaded file
        database_names: List of names in database
        cutoff: Similarity threshold (0.0-1.0), default 0.80 = 80% match
    
    Returns:
        Matched name from database, or None if no good match found
    """
    matches = get_close_matches(csv_name, database_names, n=1, cutoff=cutoff)
    return matches[0] if matches else None


def _name_key(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def _apply_web_ratings(rows: list[dict], ratings_df: pd.DataFrame | None) -> list[dict]:
    """Attach optional web ratings and category overrides to scraped rows."""
    if ratings_df is None or ratings_df.empty:
        return rows

    normalized = ratings_df.copy()
    normalized.columns = [str(column).strip().lower() for column in normalized.columns]
    if "name" not in normalized.columns:
        return rows

    allowed_categories = set(TEAM_CATEGORY_ORDER)
    score_by_name: dict[str, float] = {}
    category_by_name: dict[str, str] = {}

    for _, record in normalized.iterrows():
        key = _name_key(str(record.get("name", "")))
        if not key:
            continue

        score_value = record.get("score")
        if score_value is not None and str(score_value).strip() != "":
            try:
                score_by_name[key] = float(score_value)
            except (TypeError, ValueError):
                pass

        category_value = str(record.get("category", "")).strip().lower()
        if category_value in allowed_categories:
            category_by_name[key] = category_value

    enriched: list[dict] = []
    for row in rows:
        item = dict(row)
        key = _name_key(str(item.get("name", "")))
        if key in score_by_name:
            item["web_score"] = score_by_name[key]
        if key in category_by_name:
            item["category"] = category_by_name[key]
            item["youth"] = category_by_name[key] == "youth"
        enriched.append(item)

    return enriched


def _import_rows_to_db(rows: list[dict]) -> tuple[int, int]:
    """Upsert rows into riders table and return inserted/updated counts."""
    priced_rows = assign_prices(rows)
    existing_names = {row[1] for row in get_all_riders()}
    inserted = 0
    updated = 0
    for row in priced_rows:
        add_rider(
            name=row['name'],
            team=row['team'],
            category=row['category'],
            price=row['price'],
            youth=row['youth'],
        )
        if row['name'] in existing_names:
            updated += 1
        else:
            inserted += 1
    return inserted, updated


def _build_web_ratings_template(rows: list[dict]) -> bytes:
    """Build CSV template for post-scrape web rating enrichment."""
    template_df = pd.DataFrame(rows)[['name', 'team']].drop_duplicates().sort_values(['team', 'name'])
    template_df['score'] = ''
    template_df['category'] = ''
    return template_df.to_csv(index=False).encode('utf-8')

def main():
    st.title("🚴 Giro d'Italia Fantasy Cycling 2026")
    st.markdown("---")
    
    # Initialize authentication in session state
    if 'authenticated_player' not in st.session_state:
        st.session_state.authenticated_player = None
    
    # Show login page if not authenticated
    if st.session_state.authenticated_player is None:
        show_login()
        return
    
    # Sidebar menu with logout button
    col1, col2 = st.sidebar.columns([3, 1])
    with col1:
        st.sidebar.write(f"**Logged in as:** {st.session_state.authenticated_player}")
    with col2:
        if st.sidebar.button("🚪 Logout"):
            st.session_state.authenticated_player = None
            st.rerun()
    
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "Navigate",
        ["Home", "Team Selection", "Live Leaderboard", "Rider Points", "Transfers", "Admin"]
    )
    
    if page == "Home":
        show_home()
    elif page == "Team Selection":
        show_team_selection()
    elif page == "Live Leaderboard":
        show_leaderboard()
    elif page == "Rider Points":
        show_rider_points()
    elif page == "Transfers":
        show_transfers()
    elif page == "Admin":
        show_admin()

def show_login():
    """Login page"""
    st.title("🚴 Giro d'Italia Fantasy Cycling 2026")
    st.markdown("---")
    
    st.header("Player Login")
    st.write("Enter your name and PIN to continue.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        selected_player = st.selectbox("Select your name:", PLAYERS)
    
    with col2:
        pin = st.text_input("Enter your PIN:", type="password")
    
    if st.button("Login"):
        if selected_player in PLAYER_PINS and PLAYER_PINS[selected_player] == pin:
            st.session_state.authenticated_player = selected_player
            st.success(f"Welcome, {selected_player}!")
            st.rerun()
        else:
            st.error("Invalid player name or PIN. Please try again.")


def show_home():
    """Home page with game info"""
    st.header("Welcome to Giro Fantasy Cycling!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Game Rules")
        st.write("""
        - **Team Size**: 12 cyclists + 1 DS
        - **Budget**: 15 points per player
        - **Max 2 cyclists per pro team** (excluding DS)
        - **Transfers**: Max 12 total transfers
        - **Transfer deadline**: 12:00 local time on race days
        """
        )
    
    with col2:
        st.subheader("🎯 Scoring System")
        st.write("""
        - Stage placings (top 20)
        - Mountain & sprint points
        - GC standings (top 10)
        - Category multipliers (2x-4x)
        - Stage winner team bonus (+10)
        - DS scoring (top 5 riders)
        """
        )
    
    st.subheader("📅 Race Schedule")
    st.info("Giro d'Italia 2026 starts on May 8th")

    st.subheader("⏱️ Team & Transfer Timing")
    st.markdown(
        f"""
        - **Timezone**: All deadlines use local race time (**{RACE_TIMEZONE.key}**).
        - **Team Selection**: Open until **08 May {SEASON_YEAR} 14:00**, then locked for the rest of the Giro.
        - **Transfers before Giro start**: Closed.
        - **Race days (all 21 stages)**:
          - Open **00:00-12:00**
          - Closed **12:00-21:00** (scoring window)
          - Open again **from 21:00**
        - **Rest days**: Transfers open all day on **11 May**, **18 May**, and **25 May**.
        """
    )

    window_status = get_game_window_status()
    now_local = datetime.now(RACE_TIMEZONE)
    st.caption(f"Local race time: {now_local.strftime('%d %b %Y %H:%M')} ({RACE_TIMEZONE.key})")
    st.write(f"Team selection: {'Open' if window_status['team_selection_open'] else 'Closed'}")
    st.caption(str(window_status['team_selection_message']))
    st.write(f"Transfers: {'Open' if window_status['transfers_open'] else 'Closed'}")
    st.caption(str(window_status['transfers_message']))
    
    st.subheader("👥 Players")
    for idx, player in enumerate(PLAYERS, 1):
        st.write(f"{idx}. {player}")

def show_team_selection():
    """Team selection page"""
    st.header("🎯 Team Selection")
    
    # Use authenticated player
    player = st.session_state.authenticated_player
    st.write(f"**Player:** {player}")

    window_status = get_game_window_status()
    if not window_status['team_selection_open']:
        st.warning(str(window_status['team_selection_message']))
        st.info("Use Transfers for rider changes during open transfer windows.")
        return
    
    st.write(f"**Budget: {BUDGET_LIMIT}** (Remaining: TBD)")
    
    st.subheader("Select Your Cyclists")
    
    # Get all riders
    riders = get_all_riders()
    riders_df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
    if riders_df.empty:
        st.info("No riders in database yet. Use Admin → Scrape Riders first.")
        return

    existing_team = get_player_team(player)
    existing_by_category = {}
    if existing_team:
        existing_df = pd.DataFrame(existing_team, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
        existing_by_category = {
            category: existing_df[existing_df['Category'] == category]['Name'].tolist()
            for category in TEAM_CATEGORY_ORDER
        }

    st.subheader("All Riders Overview")
    category_filter = st.multiselect(
        "Filter table by category",
        options=TEAM_CATEGORY_ORDER,
        default=TEAM_CATEGORY_ORDER,
        format_func=lambda category: TEAM_CATEGORY_LABELS[category],
        key=f"team_table_category_filter_{player}",
    )

    filtered_overview_df = riders_df[riders_df['Category'].isin(category_filter)].copy()
    filtered_overview_df['Category'] = filtered_overview_df['Category'].map(TEAM_CATEGORY_LABELS)
    st.dataframe(filtered_overview_df[['ID', 'Name', 'Team', 'Category', 'Price']], use_container_width=True)

    st.subheader("Build Your Team")
    st.caption("Pick one rider per slot. Slots enforce the category requirements.")

    selected_riders = {category: [] for category in TEAM_CATEGORY_ORDER}

    rider_slot_total = sum(TEAM_COMPOSITION.get(category, 0) for category in TEAM_CATEGORY_ORDER if category != 'ds')
    st.write(f"**Rider Slots ({rider_slot_total})**")
    selector_columns = st.columns(2)

    non_ds_categories = [category for category in TEAM_CATEGORY_ORDER if category != 'ds']
    for idx, category in enumerate(non_ds_categories):
        with selector_columns[idx % 2]:
            category_riders = riders_df[riders_df['Category'] == category].sort_values(['Team', 'Name'])
            needed = TEAM_COMPOSITION.get(category, 1)
            st.write(f"**{TEAM_CATEGORY_LABELS[category]} slots: {needed}**")

            option_ids = [""] + category_riders['ID'].tolist()
            id_to_name = category_riders.set_index('ID')['Name'].to_dict()
            name_to_id = category_riders.set_index('Name')['ID'].to_dict()
            id_to_label = {
                row['ID']: f"{row['Name']} | {row['Team']} | {float(row['Price']):.1f}"
                for _, row in category_riders.iterrows()
            }

            default_names = existing_by_category.get(category, [])
            for slot_idx in range(needed):
                default_id = ""
                if slot_idx < len(default_names):
                    default_name = default_names[slot_idx]
                    default_id = name_to_id.get(default_name, "")

                selected_id = st.selectbox(
                    f"{TEAM_CATEGORY_LABELS[category]} slot {slot_idx + 1}",
                    options=option_ids,
                    index=option_ids.index(default_id) if default_id in option_ids else 0,
                    format_func=lambda rider_id: "-- Select rider --" if rider_id == "" else id_to_label[rider_id],
                    key=f"slot_{player}_{category}_{slot_idx}",
                )
                if selected_id != "":
                    selected_riders[category].append(id_to_name[selected_id])

    st.write("**DS Slot (1)**")
    ds_riders = riders_df[riders_df['Category'] == 'ds'].sort_values(['Team', 'Name'])
    ds_riders_unique = ds_riders.drop_duplicates(subset=['Team'], keep='first')
    
    ds_team_to_info = {
        row['Team']: {'Name': row['Name'], 'Price': row['Price']}
        for _, row in ds_riders_unique.iterrows()
    }
    ds_team_options = ['-- Select DS Team --'] + sorted(ds_team_to_info.keys())
    
    team_to_label = {
        team: f"{info['Name']} | {team} | {float(info['Price']):.1f}"
        for team, info in ds_team_to_info.items()
    }

    default_team = '-- Select DS Team --'
    default_ds = existing_by_category.get('ds', [])
    if default_ds:
        default_ds_name = default_ds[0]
        default_match = ds_riders[(ds_riders['Name'] == default_ds_name) | (ds_riders['Team'] == default_ds_name)]
        if not default_match.empty:
            default_team = default_match.iloc[0]['Team']

    selected_team = st.selectbox(
        "DS slot",
        ds_team_options,
        index=ds_team_options.index(default_team) if default_team in ds_team_options else 0,
        format_func=lambda team: "-- Select DS Team --" if team == "-- Select DS Team --" else team_to_label.get(team, team),
        key=f"slot_{player}_ds",
    )
    selected_name = ds_team_to_info.get(selected_team, {}).get('Name') if selected_team != '-- Select DS Team --' else None
    selected_riders['ds'] = [selected_name] if selected_name else []

    chosen_names = []
    for category in TEAM_CATEGORY_ORDER:
        chosen_names.extend(selected_riders.get(category, []))

    preview_df = riders_df[riders_df['Name'].isin(chosen_names)]
    preview_total = float(preview_df['Price'].sum()) if not preview_df.empty else 0.0
    st.write(f"**Current selection total:** {preview_total:.1f} / {BUDGET_LIMIT}")
    
    # Confirm selection
    if st.button("Confirm Team Selection"):
        for category in TEAM_CATEGORY_ORDER:
            expected = TEAM_COMPOSITION.get(category, 1)
            selected_count = len(selected_riders.get(category, []))
            if selected_count != expected:
                st.error(f"Please select exactly {expected} rider(s) for {TEAM_CATEGORY_LABELS[category]}.")
                return

        if len(chosen_names) != len(set(chosen_names)):
            st.error("A rider can only be selected once.")
            return

        selected_df = riders_df[riders_df['Name'].isin(chosen_names)]
        total_cost = float(selected_df['Price'].sum())
        if total_cost > BUDGET_LIMIT:
            st.error(f"Team exceeds budget: {total_cost:.1f} / {BUDGET_LIMIT}")
            return

        # Max 2 cyclists per pro team; DS is excluded from this rule.
        team_counts = selected_df[selected_df['Category'] != 'ds']['Team'].value_counts()
        if not team_counts.empty and team_counts.max() > 2:
            st.error("You can select max 2 cyclists from the same pro team.")
            return

        save_player_team(player, chosen_names)
        st.success(f"Team saved. Total cost: {total_cost:.1f} / {BUDGET_LIMIT}")

def _build_rider_points_by_stage(
    df: pd.DataFrame,
) -> dict[int, dict[int, float]]:
    """Return {stage_number: {rider_id: fantasy_points}} for all riders in df."""
    scoring = ScoringSystem()
    stage_points_by_stage: dict[int, dict[int, float]] = {}

    rider_category_by_id = {int(row['ID']): row['Category'] for _, row in df.iterrows()}
    rider_team_by_id = {int(row['ID']): row['Team'] for _, row in df.iterrows()}

    uploaded_stage_points = get_stage_points()
    stage_rows = get_stage_results()
    stage_rows_by_stage: dict[int, list[tuple]] = {}
    for row in stage_rows:
        stage_rows_by_stage.setdefault(row[0], []).append(row)

    if uploaded_stage_points:
        uploaded_by_stage: dict[int, list[tuple]] = {}
        for stage_number, rider_name, rider_team, rider_id, points in uploaded_stage_points:
            uploaded_by_stage.setdefault(stage_number, []).append(
                (rider_name, rider_team, rider_id, float(points))
            )

        for stage_number, rows in uploaded_by_stage.items():
            uploaded_points_by_rider_id: dict[int, float] = {}
            for _, _, rider_id, points in rows:
                uploaded_points_by_rider_id[rider_id] = (
                    uploaded_points_by_rider_id.get(rider_id, 0.0) + float(points)
                )
            legacy_rows = stage_rows_by_stage.get(stage_number, [])
            winning_team = next(
                (team for _, position, _, team, _, _ in legacy_rows if position == 1), None
            )
            if winning_team is None and rows:
                winning_team = max(rows, key=lambda r: r[3])[1]

            stage_points_by_stage.setdefault(stage_number, {})
            for rider_id, category in rider_category_by_id.items():
                if category == 'ds':
                    continue
                stage_points_by_stage[stage_number][rider_id] = (
                    stage_points_by_stage[stage_number].get(rider_id, 0.0)
                    + scoring.calculate_uploaded_rider_points(
                        base_points=uploaded_points_by_rider_id.get(rider_id, 0),
                        rider_team=rider_team_by_id.get(rider_id, ''),
                        winning_team=winning_team,
                        category=category,
                    )
                )
            for _, row in df.iterrows():
                rider_id = int(row['ID'])
                if row['Category'] != 'ds':
                    continue
                stage_points_by_stage[stage_number][rider_id] = (
                    stage_points_by_stage[stage_number].get(rider_id, 0.0)
                    + scoring.calculate_ds_stage_score(rider_team_by_id[rider_id], legacy_rows)
                )
    else:
        classifications_by_stage: dict[int, dict[str, list[tuple]]] = {}
        for classification_key in CLASSIFICATION_LABELS:
            for row in get_classification_results(classification_key):
                classifications_by_stage.setdefault(row[0], {}).setdefault(
                    classification_key, []
                ).append(row)

        for stage_number, rows in stage_rows_by_stage.items():
            winning_team = next(
                (team for _, position, _, team, _, _ in rows if position == 1), None
            )
            stage_position_by_rider_id = {rider_id: position for _, position, _, _, rider_id, _ in rows}
            stage_points_by_stage.setdefault(stage_number, {})

            for rider_id, category in rider_category_by_id.items():
                if category == 'ds':
                    continue
                stage_points_by_stage[stage_number][rider_id] = (
                    stage_points_by_stage[stage_number].get(rider_id, 0.0)
                    + scoring.calculate_rider_stage_score(
                        position=stage_position_by_rider_id.get(rider_id, 0),
                        rider_team=rider_team_by_id.get(rider_id, ''),
                        winning_team=winning_team,
                        category=category,
                    )
                )
            for _, row in df.iterrows():
                rider_id = int(row['ID'])
                if row['Category'] != 'ds':
                    continue
                stage_points_by_stage[stage_number][rider_id] = (
                    stage_points_by_stage[stage_number].get(rider_id, 0.0)
                    + scoring.calculate_ds_stage_score(rider_team_by_id[rider_id], rows)
                )

            for classification_key, class_rows in classifications_by_stage.get(stage_number, {}).items():
                for _, _, position, _, _, rider_id, value in class_rows:
                    stage_points_by_stage[stage_number][rider_id] = (
                        stage_points_by_stage[stage_number].get(rider_id, 0.0)
                        + scoring.calculate_classification_score(classification_key, position, value)
                    )

    return stage_points_by_stage


def show_rider_points():
    """Per-stage rider points table."""
    st.header("🏆 Rider Points Per Stage")

    riders = get_all_riders()
    if not riders:
        st.info("No riders in the database yet. Ask an admin to scrape riders first.")
        return

    df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
    stage_points_by_stage = _build_rider_points_by_stage(df)

    if not stage_points_by_stage:
        st.info("No stage results have been imported yet.")
        return

    stage_numbers = sorted(stage_points_by_stage.keys())
    stage_columns = [f"Stage {s}" for s in stage_numbers]

    for s in stage_numbers:
        df[f"Stage {s}"] = df['ID'].map(
            lambda rid, _s=s: stage_points_by_stage[_s].get(rid, 0)
        )
    df['Total'] = df[stage_columns].sum(axis=1)

    # --- Filter controls ---
    col_search, col_cat, col_stage = st.columns([2, 2, 2])
    with col_search:
        search = st.text_input("Search rider / team", placeholder="Type to filter…")
    with col_cat:
        categories = ["All"] + sorted(df['Category'].dropna().unique().tolist())
        selected_cat = st.selectbox("Category", categories)
    with col_stage:
        stage_view = st.selectbox("Show stage", ["All stages"] + stage_columns)

    filtered = df.copy()
    if search:
        mask = (
            filtered['Name'].str.contains(search, case=False, na=False)
            | filtered['Team'].str.contains(search, case=False, na=False)
        )
        filtered = filtered[mask]
    if selected_cat != "All":
        filtered = filtered[filtered['Category'] == selected_cat]

    if stage_view == "All stages":
        display_cols = ['Name', 'Team', 'Category'] + stage_columns + ['Total']
        filtered = filtered.sort_values(['Total', 'Name'], ascending=[False, True])
    else:
        display_cols = ['Name', 'Team', 'Category', stage_view]
        filtered = filtered.sort_values([stage_view, 'Name'], ascending=[False, True])

    st.dataframe(
        filtered[display_cols].reset_index(drop=True),
        use_container_width=True,
        height=600,
    )
    st.caption(f"Showing {len(filtered)} of {len(df)} riders")


def show_leaderboard():
    """Live leaderboard"""
    st.header("📊 Live Leaderboard")

    stage_results = get_stage_results()
    stage_points = get_stage_points()
    classification_results = {
        'gc': get_classification_results('gc'),
        'mountains': get_classification_results('mountains'),
        'sprints': get_classification_results('sprints'),
    }

    if not stage_points and not stage_results and not any(classification_results.values()):
        st.info("No stage points or legacy results imported yet. Use Admin → Add Results to import data.")
        return

    player_teams = {player: get_player_team(player) for player in PLAYERS}
    withdrawals = get_rider_withdrawals()
    ds_rest_day_rows_by_player = _build_ds_rest_day_rows_by_player(player_teams, withdrawals)
    leaderboard_rows, breakdown_by_player = calculate_leaderboard(
        players=PLAYERS,
        player_teams=player_teams,
        stage_results=stage_results,
        classification_results=classification_results,
        stage_points=stage_points,
        ds_rest_day_rows_by_player=ds_rest_day_rows_by_player,
    )

    df_leaderboard = pd.DataFrame(leaderboard_rows)
    st.dataframe(df_leaderboard[['Rank', 'Player', 'Total Points', 'Stages Scored']], use_container_width=True)

    st.subheader("Stage Breakdown")
    selected_player = st.selectbox("View breakdown for player", PLAYERS, key="leaderboard_player")
    player_breakdown = breakdown_by_player.get(selected_player, [])
    if not player_breakdown:
        st.info("This player has no scored stages yet. Save a team and import stage results to see breakdowns.")
        return

    st.dataframe(pd.DataFrame(player_breakdown), use_container_width=True)

def show_transfers():
    """Transfer management"""
    st.header("🔄 Transfers")
    
    # Use authenticated player
    player = st.session_state.authenticated_player
    st.write(f"**Player:** {player}")

    window_status = get_game_window_status()
    st.caption(str(window_status['transfers_message']))
    if not window_status['transfers_open']:
        st.warning("Transfers are currently closed.")
        return
    
    current_team = get_player_team(player)
    transfers_used = count_transfers(player)
    
    st.write(f"Transfers used: {transfers_used} / {MAX_TRANSFERS}")

    if not current_team:
        st.info("No saved team found. Build a team first in Team Selection.")
        return

    current_team_df = pd.DataFrame(
        current_team,
        columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth']
    )

    st.subheader("Current Team")
    st.dataframe(
        current_team_df[['Name', 'Team', 'Category', 'Price']],
        use_container_width=True,
    )

    all_riders = get_all_riders()
    all_riders_df = pd.DataFrame(all_riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
    current_names = current_team_df['Name'].tolist()

    rider_lookup = all_riders_df.set_index('Name').to_dict('index')
    rider_out_options = ["-- Select rider --"] + sorted(current_names)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Rider Out**")
        rider_out = st.selectbox("Select rider to remove", rider_out_options)

    rider_in_candidates = all_riders_df.iloc[0:0].copy()
    rider_out_category = None
    if rider_out != "-- Select rider --":
        rider_out_category = rider_lookup[rider_out]['Category']
        rider_in_candidates = (
            all_riders_df[
                (all_riders_df['Category'] == rider_out_category)
                & (~all_riders_df['Name'].isin(current_names))
            ]
            .sort_values(['Team', 'Name'])
        )

    rider_in_options = ["-- Select rider --"] + rider_in_candidates['Name'].tolist()
    
    with col2:
        st.write("**Rider In**")
        rider_in = st.selectbox(
            "Select rider to add",
            rider_in_options,
            format_func=lambda name: (
                name
                if name == "-- Select rider --"
                else f"{name} | {rider_lookup[name]['Team']} | {float(rider_lookup[name]['Price']):.1f}"
            ),
        )

    if rider_out != "-- Select rider --":
        st.caption(f"Transfers must stay within category: {TEAM_CATEGORY_LABELS[rider_out_category]}")
        if rider_in_candidates.empty:
            st.warning("No available riders to transfer in for this category.")
    
    if st.button("Confirm Transfer"):
        if transfers_used >= MAX_TRANSFERS:
            st.error("Transfer limit reached.")
            return

        if rider_out == "-- Select rider --" or rider_in == "-- Select rider --":
            st.error("Please select both riders.")
            return

        if rider_lookup[rider_out]['Category'] != rider_lookup[rider_in]['Category']:
            st.error("Transfer must be within the same category.")
            return

        updated_team_names = [name for name in current_names if name != rider_out] + [rider_in]
        updated_df = all_riders_df[all_riders_df['Name'].isin(updated_team_names)]

        total_cost = float(updated_df['Price'].sum())
        if total_cost > BUDGET_LIMIT:
            st.error(f"Transfer exceeds budget: {total_cost:.1f} / {BUDGET_LIMIT}")
            return

        team_counts = updated_df[updated_df['Category'] != 'ds']['Team'].value_counts()
        if not team_counts.empty and team_counts.max() > 2:
            st.error("Transfer breaks max-2-riders-per-team rule.")
            return

        save_player_team(player, updated_team_names)
        add_transfer(player, rider_out, rider_in)
        st.success("Transfer confirmed and saved.")

def show_admin():
    """Admin panel for managing riders and results"""
    st.header("⚙️ Admin Panel")
    
    admin_page = st.selectbox(
        "Select admin function",
        [
            "Initialize Players",
            "Scrape Riders",
            "Clear Riders",
            "Add Results",
            "Manage Withdrawals",
            "Override Riders",
            "Database Backup",
            "View Database",
        ]
    )
    
    if admin_page == "Initialize Players":
        st.subheader("Initialize Players")
        if st.button("Add Default Players"):
            for player_name in PLAYERS:
                add_player(player_name)
            st.success("Players initialized!")
    
    elif admin_page == "Scrape Riders":
        st.subheader("Scrape Riders from Web")
        st.caption(
            "Optional: upload a web ratings CSV with columns `name, score` and optional `category` "
            "to improve pricing and role assignment during import."
        )
        web_ratings_file = st.file_uploader(
            "Web ratings CSV (optional)",
            type=["csv"],
            key="web_ratings_csv",
        )
        ratings_df = None
        if web_ratings_file is not None:
            try:
                ratings_df = pd.read_csv(web_ratings_file)
                ratings_df.columns = [str(column).strip().lower() for column in ratings_df.columns]
                if "name" not in ratings_df.columns:
                    st.error("Web ratings CSV must include a `name` column.")
                    ratings_df = None
                else:
                    st.info(f"Loaded {len(ratings_df)} web rating rows.")
            except Exception as exc:
                st.error(f"Could not read web ratings CSV: {exc}")

        scrape_year = st.number_input("Giro year", min_value=2000, max_value=2100, value=2026, step=1)
        if st.button("Scrape Giro Startlist"):
            with st.spinner("Scraping..."):
                scraper = GiroScraper()
                try:
                    scraped_rows = scraper.scrape_giro_startlist(year=int(scrape_year))
                    st.session_state['last_scraped_rows'] = scraped_rows
                    st.session_state['last_scrape_year'] = int(scrape_year)
                    enriched_rows = _apply_web_ratings(scraped_rows, ratings_df)
                    inserted, updated = _import_rows_to_db(enriched_rows)

                    st.success(f"Scraped {len(enriched_rows)} entries for {scrape_year}.")
                    st.info(f"Inserted {inserted} new entries and updated {updated} existing names.")
                except Exception as exc:
                    st.error(f"Scrape failed: {exc}")

        st.divider()
        st.subheader("📕 Upload PCS Startlist PDF")
        st.caption(
            "Upload the PCS startlist PDF export. The app will parse teams, riders, and DS rows, "
            "save bootstrap for the selected year, and import them immediately."
        )
        pcs_pdf_year = st.number_input(
            "Year for uploaded PDF",
            min_value=2000,
            max_value=2100,
            value=2026,
            step=1,
            key="pcs_pdf_year",
        )
        pcs_pdf_file = st.file_uploader(
            "PCS startlist PDF file",
            type=["pdf"],
            key="pcs_pdf_upload",
        )
        if pcs_pdf_file is not None:
            try:
                pdf_bytes = pcs_pdf_file.read()
                scraper = GiroScraper()
                parsed_rows = scraper.parse_startlist_pdf_bytes(pdf_bytes)
                if not parsed_rows:
                    st.error("No riders were parsed from this PDF file. Check that it is the PCS startlist PDF.")
                else:
                    st.session_state['last_scraped_rows'] = parsed_rows
                    st.session_state['last_scrape_year'] = int(pcs_pdf_year)
                    enriched_rows = _apply_web_ratings(parsed_rows, ratings_df)
                    st.info(f"Parsed {len(enriched_rows)} rows from uploaded PDF.")
                    st.dataframe(pd.DataFrame(enriched_rows[:10]), use_container_width=True)

                    if st.button("Save Parsed PDF as Bootstrap & Import Riders", key="save_pcs_pdf_btn"):
                        from pathlib import Path as _Path
                        import json as _json

                        bootstrap_path = _Path("data/bootstrap_startlist") / f"giro_{int(pcs_pdf_year)}.json"
                        bootstrap_path.parent.mkdir(parents=True, exist_ok=True)
                        bootstrap_path.write_text(
                            _json.dumps(enriched_rows, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )

                        cache_path = _Path("data/startlist_cache") / f"giro_{int(pcs_pdf_year)}.json"
                        if cache_path.exists():
                            cache_path.unlink()

                        inserted, updated = _import_rows_to_db(enriched_rows)

                        st.success(
                            f"Imported {len(enriched_rows)} riders from uploaded PDF for {int(pcs_pdf_year)}."
                        )
                        st.info(f"Inserted {inserted} new entries and updated {updated} existing names.")
            except Exception as exc:
                st.error(f"Failed to parse uploaded PDF: {exc}")

        last_scraped_rows = st.session_state.get('last_scraped_rows')
        last_scrape_year = st.session_state.get('last_scrape_year')
        if last_scraped_rows:
            st.divider()
            st.subheader("⭐ Post-Scrape Ratings Workflow")
            st.caption(
                "Optional second step: download template, fill web-derived `score` and optional `category`, "
                "then apply to re-categorize/re-price the last scraped roster."
            )
            st.download_button(
                "Download Ratings Template CSV",
                data=_build_web_ratings_template(last_scraped_rows),
                file_name=f"web_ratings_template_{int(last_scrape_year)}.csv",
                mime="text/csv",
                key="download_web_ratings_template",
            )

            post_ratings_file = st.file_uploader(
                "Upload filled Ratings CSV",
                type=["csv"],
                key="post_scrape_ratings_csv",
            )
            if post_ratings_file is not None:
                try:
                    post_df = pd.read_csv(post_ratings_file)
                    post_df.columns = [str(column).strip().lower() for column in post_df.columns]
                    if 'name' not in post_df.columns:
                        st.error("Ratings CSV must include a `name` column.")
                    else:
                        st.info(f"Loaded {len(post_df)} post-scrape rating rows.")
                        if st.button("Apply Ratings To Last Scrape", key="apply_post_scrape_ratings"):
                            enriched_rows = _apply_web_ratings(last_scraped_rows, post_df)
                            inserted, updated = _import_rows_to_db(enriched_rows)
                            st.success("Applied ratings and refreshed categories/prices from last scrape.")
                            st.info(f"Inserted {inserted} new entries and updated {updated} existing names.")
                except Exception as exc:
                    st.error(f"Could not read ratings CSV: {exc}")

    elif admin_page == "Clear Riders":
        st.subheader("Clear Riders")
        st.warning("This removes all riders, saved teams, transfers, stage results, and classification results.")
        confirm_clear = st.checkbox("I understand this action cannot be undone.")
        if st.button("Clear All Riders"):
            if not confirm_clear:
                st.error("Please confirm before clearing riders.")
                return

            removed = clear_riders()
            st.success(f"Cleared {removed} rider entries and related data.")
    
    elif admin_page == "Add Results":
        st.subheader("Add Stage Results")
        result_year = st.number_input("Giro year", min_value=2000, max_value=2100, value=SEASON_YEAR, step=1, key="results_year")
        stage_num = st.number_input("Stage number", 1, 21, 1)
        enable_firstcycling = st.checkbox(
            "Try experimental FirstCycling fallback (best effort)",
            value=False,
            help=(
                "Attempts FirstCycling before Cyclingstage."
                " If blocked or unparseable, fetch continues with other sources."
            ),
        )
        st.info(f"Fetch and preview results for stage {stage_num} before wiring them into scoring.")

        rider_rows = get_all_riders()
        riders_df = pd.DataFrame(rider_rows, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
        rider_names = riders_df['Name'].tolist()
        rider_lookup = riders_df.set_index('Name').to_dict('index') if not riders_df.empty else {}

        if st.button("Fetch Stage Results"):
            with st.spinner("Fetching stage results..."):
                scraper = GiroScraper()
                try:
                    stage_payload = scraper.scrape_stage_results(
                        year=int(result_year),
                        stage_number=int(stage_num),
                        include_firstcycling=enable_firstcycling,
                    )
                    st.session_state["last_stage_results"] = stage_payload
                    st.success(
                        f"Fetched {len(stage_payload['results'])} result rows for stage {int(stage_num)} "
                        f"from {stage_payload.get('source', 'unknown')} source."
                    )
                except Exception as exc:
                    st.error(f"Stage fetch failed: {exc}")

        stage_payload = st.session_state.get("last_stage_results")
        if stage_payload and stage_payload.get("stage_number") == int(stage_num) and stage_payload.get("year") == int(result_year):
            st.caption(
                f"Showing cached preview for Giro {stage_payload['year']} stage {stage_payload['stage_number']} "
                f"from {stage_payload.get('source', 'unknown')} source."
            )
            results_df = pd.DataFrame(stage_payload.get("results", []))
            if results_df.empty:
                st.warning("No stage rows were parsed for this stage yet.")
            else:
                st.dataframe(results_df, use_container_width=True)
                st.info("Importing will replace any previously saved results for this stage.")

                if st.button("Import Stage Results"):
                    inserted, replaced, unmatched = save_stage_results(
                        stage_number=int(stage_num),
                        result_rows=stage_payload.get("results", []),
                    )
                    st.success(f"Imported {inserted} stage rows for stage {int(stage_num)}.")
                    if replaced:
                        st.info(f"Replaced {replaced} existing saved result rows for this stage.")
                    if unmatched:
                        st.warning(
                            f"Skipped {len(unmatched)} unmatched names not found in the rider table."
                        )
                        st.write(unmatched)

        st.divider()
        st.subheader("🟣 Fetch GC Standings (After Stage)")
        st.caption("Fetch GC standings after this stage from PCS and import them as classification results.")

        if st.button("Fetch GC Standings"):
            with st.spinner("Fetching GC standings..."):
                scraper = GiroScraper()
                try:
                    gc_payload = scraper.scrape_stage_gc_results(
                        year=int(result_year),
                        stage_number=int(stage_num),
                    )
                    st.session_state["last_gc_results"] = gc_payload
                    st.success(
                        f"Fetched {len(gc_payload['results'])} GC rows for stage {int(stage_num)} "
                        f"from {gc_payload.get('source', 'unknown')} source."
                    )
                except Exception as exc:
                    st.error(f"GC fetch failed: {exc}")

        gc_payload = st.session_state.get("last_gc_results")
        if gc_payload and gc_payload.get("stage_number") == int(stage_num) and gc_payload.get("year") == int(result_year):
            st.caption(
                f"Showing cached GC preview for Giro {gc_payload['year']} stage {gc_payload['stage_number']} "
                f"from {gc_payload.get('source', 'unknown')} source."
            )
            gc_df = pd.DataFrame(gc_payload.get("results", []))
            if gc_df.empty:
                st.warning("No GC rows were parsed for this stage yet.")
            else:
                preview_cols = [col for col in ['position', 'name', 'team', 'value'] if col in gc_df.columns]
                st.dataframe(gc_df[preview_cols], use_container_width=True, height=260)
                st.info("Importing will replace previously saved GC rows for this stage.")

                if st.button("Import GC Standings"):
                    inserted, replaced, unmatched = save_classification_results(
                        classification='gc',
                        stage_number=int(stage_num),
                        result_rows=gc_payload.get("results", []),
                    )
                    st.success(f"Imported {inserted} GC rows for stage {int(stage_num)}.")
                    if replaced:
                        st.info(f"Replaced {replaced} existing GC rows for this stage.")
                    if unmatched:
                        st.warning(f"Skipped {len(unmatched)} unmatched riders.")

        saved_gc_rows = get_classification_results('gc', stage_number=int(stage_num))
        if saved_gc_rows:
            st.subheader("Saved GC Standings")
            saved_gc_df = pd.DataFrame(
                saved_gc_rows,
                columns=['Stage', 'Classification', 'Position', 'Name', 'Team', 'Rider ID', 'Value'],
            )
            st.dataframe(
                saved_gc_df[['Position', 'Name', 'Team', 'Value']].sort_values(['Position', 'Name']),
                use_container_width=True,
                height=260,
            )

        st.divider()
        st.subheader("🟠 Fetch Stage KOM & Red Bull Sprint Points")
        st.caption(
            "KOM stage points are derived as delta from cumulative KOM standings. "
            "Red Bull sprint points are ranked from Red Bull KM data and imported as sprint points."
        )

        if st.button("Fetch KOM + Red Bull Sprint", key="fetch_kom_redbull"):
            with st.spinner("Fetching KOM and Red Bull sprint points..."):
                scraper = GiroScraper()
                try:
                    stage_number_int = int(stage_num)
                    result_year_int = int(result_year)

                    kom_fetch_error = None
                    kom_source = "unknown"
                    try:
                        kom_current = scraper.scrape_stage_metric_points(
                            year=result_year_int,
                            stage_number=stage_number_int,
                            metric="kom_cumulative",
                        )
                        kom_source = kom_current.get("source", "unknown")
                    except Exception as exc:
                        kom_fetch_error = str(exc)
                        kom_current = {
                            "results": [],
                            "source": "unavailable",
                        }

                    # Fallback: derive KOM stage points from FirstCycling cumulative KOM standings.
                    if not kom_current.get("results"):
                        try:
                            kom_current = scraper.scrape_firstcycling_cumulative_points(
                                year=result_year_int,
                                stage_number=stage_number_int,
                                classification="mountains",
                            )
                            kom_source = kom_current.get("source", "firstcycling-widget")
                            kom_fetch_error = None
                        except Exception:
                            pass

                    kom_previous_rows = []
                    if stage_number_int > 1 and kom_current.get("results"):
                        try:
                            if kom_source == "firstcycling-widget":
                                kom_previous = scraper.scrape_firstcycling_cumulative_points(
                                    year=result_year_int,
                                    stage_number=stage_number_int - 1,
                                    classification="mountains",
                                )
                            else:
                                kom_previous = scraper.scrape_stage_metric_points(
                                    year=result_year_int,
                                    stage_number=stage_number_int - 1,
                                    metric="kom_cumulative",
                                )
                            kom_previous_rows = kom_previous.get("results", [])
                        except Exception:
                            kom_previous_rows = []

                    kom_previous_by_name = {
                        _name_key(row.get("name", "")): int(row.get("value", 0) or 0)
                        for row in kom_previous_rows
                        if row.get("name")
                    }

                    kom_stage_points_rows: list[dict] = []
                    for row in kom_current.get("results", []):
                        rider_name = row.get("name", "")
                        if not rider_name:
                            continue
                        key = _name_key(rider_name)
                        current_points = int(row.get("value", 0) or 0)
                        previous_points = kom_previous_by_name.get(key, 0)
                        stage_points_delta = max(current_points - previous_points, 0)
                        if stage_points_delta <= 0:
                            continue
                        kom_stage_points_rows.append(
                            {
                                "name": rider_name,
                                "team": row.get("team", ""),
                                "points": stage_points_delta,
                            }
                        )

                    kom_stage_points_rows.sort(key=lambda item: (-item["points"], item["name"]))
                    kom_import_rows = [
                        {
                            "position": idx,
                            "name": row["name"],
                            "team": row["team"],
                            "value": str(row["points"]),
                        }
                        for idx, row in enumerate(kom_stage_points_rows, start=1)
                    ]

                    sprint_source = "unknown"
                    sprint_import_rows: list[dict] = []
                    try:
                        sprint_payload = scraper.scrape_stage_red_bull_sprint_points(
                            year=result_year_int,
                            stage_number=stage_number_int,
                        )
                        sprint_import_rows = sprint_payload.get("results", [])
                        sprint_source = sprint_payload.get("source", "unknown")
                    except Exception:
                        sprint_import_rows = []

                    # Fallback to inferred method if direct Red Bull extraction is unavailable.
                    if not sprint_import_rows:
                        bonus_current = scraper.scrape_stage_metric_points(
                            year=result_year_int,
                            stage_number=stage_number_int,
                            metric="bonis_cumulative",
                        )
                        bonus_previous_rows = []
                        if stage_number_int > 1:
                            bonus_previous = scraper.scrape_stage_metric_points(
                                year=result_year_int,
                                stage_number=stage_number_int - 1,
                                metric="bonis_cumulative",
                            )
                            bonus_previous_rows = bonus_previous.get("results", [])

                        bonus_previous_by_name = {
                            _name_key(row.get("name", "")): int(row.get("value", 0) or 0)
                            for row in bonus_previous_rows
                            if row.get("name")
                        }

                        if (
                            not stage_payload
                            or stage_payload.get("stage_number") != stage_number_int
                            or stage_payload.get("year") != result_year_int
                        ):
                            try:
                                stage_payload = scraper.scrape_stage_results(
                                    year=result_year_int,
                                    stage_number=stage_number_int,
                                    include_firstcycling=enable_firstcycling,
                                )
                            except Exception:
                                stage_payload = {
                                    "results": [],
                                    "source": "unavailable",
                                }

                        finish_bonus_by_name: dict[str, int] = {}
                        finish_bonus_for_position = {1: 10, 2: 6, 3: 4}
                        for row in stage_payload.get("results", []):
                            pos = int(row.get("position", 0) or 0)
                            rider_name = row.get("name", "")
                            if not rider_name:
                                continue
                            finish_bonus = finish_bonus_for_position.get(pos, 0)
                            if finish_bonus <= 0:
                                continue
                            finish_bonus_by_name[_name_key(rider_name)] = finish_bonus

                        sprint_stage_points_rows: list[dict] = []
                        for row in bonus_current.get("results", []):
                            rider_name = row.get("name", "")
                            if not rider_name:
                                continue
                            key = _name_key(rider_name)
                            current_bonus = int(row.get("value", 0) or 0)
                            previous_bonus = bonus_previous_by_name.get(key, 0)
                            stage_bonus_delta = max(current_bonus - previous_bonus, 0)
                            red_bull_bonus_seconds = max(stage_bonus_delta - finish_bonus_by_name.get(key, 0), 0)
                            if red_bull_bonus_seconds <= 0:
                                continue
                            sprint_stage_points_rows.append(
                                {
                                    "name": rider_name,
                                    "team": row.get("team", ""),
                                    "bonus_seconds": red_bull_bonus_seconds,
                                }
                            )

                        sprint_stage_points_rows.sort(
                            key=lambda item: (-item["bonus_seconds"], item["name"])
                        )
                        sprint_points_by_position = ScoringSystem().classification_points.get("sprints", {})
                        sprint_import_rows = [
                            {
                                "position": idx,
                                "name": row["name"],
                                "team": row["team"],
                                "value": str(sprint_points_by_position.get(idx, 0)),
                            }
                            for idx, row in enumerate(sprint_stage_points_rows, start=1)
                            if sprint_points_by_position.get(idx, 0) > 0
                        ]
                        sprint_source = "derived-from-bonifications"

                    # Game rule: always award sprint points to five riders (10/8/6/4/2).
                    sprint_points_by_position = ScoringSystem().classification_points.get("sprints", {})
                    max_sprint_rank = max(sprint_points_by_position.keys(), default=0)
                    if len(sprint_import_rows) < max_sprint_rank:
                        if (
                            not stage_payload
                            or stage_payload.get("stage_number") != stage_number_int
                            or stage_payload.get("year") != result_year_int
                        ):
                            try:
                                stage_payload = scraper.scrape_stage_results(
                                    year=result_year_int,
                                    stage_number=stage_number_int,
                                    include_firstcycling=enable_firstcycling,
                                )
                            except Exception:
                                stage_payload = {
                                    "results": [],
                                    "source": "unavailable",
                                }

                        existing_names = {
                            _name_key(row.get("name", ""))
                            for row in sprint_import_rows
                            if row.get("name")
                        }
                        for row in stage_payload.get("results", []):
                            rider_name = row.get("name", "")
                            if not rider_name:
                                continue
                            rider_key = _name_key(rider_name)
                            if rider_key in existing_names:
                                continue
                            sprint_import_rows.append(
                                {
                                    "name": rider_name,
                                    "team": row.get("team", ""),
                                    "value": "0",
                                }
                            )
                            existing_names.add(rider_key)
                            if len(sprint_import_rows) >= max_sprint_rank:
                                break

                        sprint_import_rows = [
                            {
                                "position": idx,
                                "name": row.get("name", ""),
                                "team": row.get("team", ""),
                                "value": str(sprint_points_by_position.get(idx, 0)),
                            }
                            for idx, row in enumerate(sprint_import_rows[:max_sprint_rank], start=1)
                            if row.get("name") and sprint_points_by_position.get(idx, 0) > 0
                        ]

                    st.session_state["last_stage_kom_redbull"] = {
                        "year": result_year_int,
                        "stage_number": stage_number_int,
                        "kom_rows": kom_import_rows,
                        "sprint_rows": sprint_import_rows,
                        "kom_source": kom_source,
                        "bonus_source": sprint_source,
                    }

                    st.success(
                        f"Fetched KOM stage rows: {len(kom_import_rows)} | "
                        f"Red Bull sprint rows: {len(sprint_import_rows)}"
                    )
                    if stage_payload.get("source") == "unavailable":
                        st.warning(
                            "Stage result rows are unavailable for this stage/year right now. "
                            "Red Bull sprint derivation may be incomplete."
                        )
                    if kom_fetch_error:
                        st.warning(
                            "KOM endpoint is currently unavailable from PCS for this stage/year. "
                            "Red Bull sprint points were still fetched. "
                            f"Details: {kom_fetch_error}"
                        )
                    elif not kom_import_rows:
                        st.warning(
                            "KOM stage points are currently unavailable from PCS in this environment. "
                            "No KOM rows were generated."
                        )
                except Exception as exc:
                    st.error(f"KOM/Red Bull fetch failed: {exc}")

        stage_kom_redbull_payload = st.session_state.get("last_stage_kom_redbull")
        if (
            stage_kom_redbull_payload
            and stage_kom_redbull_payload.get("stage_number") == int(stage_num)
            and stage_kom_redbull_payload.get("year") == int(result_year)
        ):
            st.caption(
                f"KOM source: {stage_kom_redbull_payload.get('kom_source', 'unknown')} | "
                f"Bonus source: {stage_kom_redbull_payload.get('bonus_source', 'unknown')}"
            )

            kom_preview_rows = stage_kom_redbull_payload.get("kom_rows", [])
            sprint_preview_rows = stage_kom_redbull_payload.get("sprint_rows", [])

            col_kom, col_sprint = st.columns(2)
            with col_kom:
                st.markdown("**Stage KOM points (derived)**")
                if kom_preview_rows:
                    kom_preview_df = pd.DataFrame(kom_preview_rows)
                    st.dataframe(
                        kom_preview_df[["position", "name", "team", "value"]],
                        use_container_width=True,
                        height=240,
                    )
                else:
                    st.info("No KOM stage points found for this stage.")

            with col_sprint:
                st.markdown("**Stage Red Bull sprint points**")
                if sprint_preview_rows:
                    sprint_preview_df = pd.DataFrame(sprint_preview_rows)
                    st.dataframe(
                        sprint_preview_df[["position", "name", "team", "value"]],
                        use_container_width=True,
                        height=240,
                    )
                else:
                    st.info("No Red Bull sprint points found for this stage.")

            if st.button("Import KOM + Red Bull Sprint", key="import_kom_redbull"):
                kom_inserted, kom_replaced, kom_unmatched = 0, 0, []
                sprint_inserted, sprint_replaced, sprint_unmatched = 0, 0, []

                if kom_preview_rows:
                    kom_inserted, kom_replaced, kom_unmatched = save_classification_results(
                        classification='mountains',
                        stage_number=int(stage_num),
                        result_rows=kom_preview_rows,
                    )
                if sprint_preview_rows:
                    sprint_inserted, sprint_replaced, sprint_unmatched = save_classification_results(
                        classification='sprints',
                        stage_number=int(stage_num),
                        result_rows=sprint_preview_rows,
                    )

                st.success(
                    f"Imported KOM rows: {kom_inserted}; Imported Red Bull sprint rows: {sprint_inserted}."
                )
                if kom_replaced or sprint_replaced:
                    st.info(
                        f"Replaced existing rows - KOM: {kom_replaced}, Red Bull sprint: {sprint_replaced}."
                    )
                if kom_unmatched or sprint_unmatched:
                    st.warning(
                        f"Skipped unmatched riders - KOM: {len(kom_unmatched)}, "
                        f"Red Bull sprint: {len(sprint_unmatched)}."
                    )

        saved_kom_rows = get_classification_results('mountains', stage_number=int(stage_num))
        saved_sprint_rows = get_classification_results('sprints', stage_number=int(stage_num))
        if saved_kom_rows or saved_sprint_rows:
            st.subheader("Saved Stage KOM / Sprint Rows")
            if saved_kom_rows:
                saved_kom_df = pd.DataFrame(
                    saved_kom_rows,
                    columns=['Stage', 'Classification', 'Position', 'Name', 'Team', 'Rider ID', 'Value'],
                )
                st.markdown("**Saved KOM rows**")
                st.dataframe(
                    saved_kom_df[['Position', 'Name', 'Team', 'Value']].sort_values(['Position', 'Name']),
                    use_container_width=True,
                    height=220,
                )
            if saved_sprint_rows:
                saved_sprint_df = pd.DataFrame(
                    saved_sprint_rows,
                    columns=['Stage', 'Classification', 'Position', 'Name', 'Team', 'Rider ID', 'Value'],
                )
                st.markdown("**Saved Red Bull sprint rows**")
                st.dataframe(
                    saved_sprint_df[['Position', 'Name', 'Team', 'Value']].sort_values(['Position', 'Name']),
                    use_container_width=True,
                    height=220,
                )

        st.divider()
        st.subheader("📤 Upload Stage Points (Excel)")
        st.caption(
            "Upload one Excel file (.xlsx) with columns: name, points. "
            "The points include GC, mountain and sprint points of the stage."
        )

        if riders_df.empty:
            st.warning("No riders are loaded yet. Scrape riders first before importing stage points.")
        else:
            points_excel_file = st.file_uploader(
                "Excel file (.xlsx) with stage points",
                type=["xlsx"],
                key=f"stage_points_xlsx_{int(stage_num)}",
            )

            if points_excel_file is not None:
                try:
                    upload_df = read_excel_with_normalized_columns(points_excel_file)
                    st.caption(f"Columns: {', '.join(upload_df.columns)}")

                    required_cols = ['name', 'points']
                    missing_cols = validate_columns(upload_df, required_cols)
                    if missing_cols:
                        st.error(f"Missing required columns: {', '.join(missing_cols)}")
                    else:
                        csv_rows = []
                        fuzzy_corrections = {}

                        for _, row in upload_df.iterrows():
                            csv_name = str(row['name']).strip()
                            if not csv_name:
                                continue

                            matched_name = csv_name
                            if csv_name not in rider_names:
                                fuzzy_match = fuzzy_match_rider_name(csv_name, rider_names, cutoff=0.80)
                                if fuzzy_match:
                                    matched_name = fuzzy_match
                                    fuzzy_corrections[csv_name] = fuzzy_match

                            csv_rows.append(
                                {
                                    'name': matched_name,
                                    'points': str(row['points']).strip(),
                                }
                            )

                        st.write(f"**Preview ({len(csv_rows)} rows)**")
                        preview_df = pd.DataFrame(csv_rows)
                        st.dataframe(preview_df, use_container_width=True, height=220)

                        if fuzzy_corrections:
                            st.info(f"🔄 Corrected {len(fuzzy_corrections)} rider name(s):")
                            for orig, corrected in list(fuzzy_corrections.items())[:5]:
                                st.caption(f"  • '{orig}' → '{corrected}'")

                        unmatched = [row['name'] for row in csv_rows if row['name'] not in rider_names]
                        if unmatched:
                            st.warning(f"⚠️ {len(set(unmatched))} rider(s) still not found: {', '.join(sorted(set(unmatched))[:5])}")

                        if st.button("Import Stage Points", key=f"import_stage_points_xlsx_{int(stage_num)}"):
                            inserted, replaced, unmatched_rows = save_stage_points(
                                stage_number=int(stage_num),
                                result_rows=csv_rows,
                            )
                            st.success(f"Imported {inserted} stage points rows for stage {int(stage_num)}.")
                            if replaced:
                                st.info(f"Replaced {replaced} existing stage points rows for this stage.")
                            if unmatched_rows:
                                st.warning(f"Skipped {len(unmatched_rows)} unmatched riders.")
                except Exception as e:
                    st.error(f"Error reading Excel file: {e}")
            
            # Show all saved classifications below
            st.divider()
            st.subheader("Saved Stage Points")
            saved_stage_points = get_stage_points(stage_number=int(stage_num))
            if saved_stage_points:
                saved_points_df = pd.DataFrame(
                    saved_stage_points,
                    columns=['Stage', 'Name', 'Team', 'Rider ID', 'Points'],
                )
                st.dataframe(
                    saved_points_df[['Name', 'Team', 'Points']].sort_values(['Points', 'Name'], ascending=[False, True]),
                    use_container_width=True,
                    height=260,
                )

    elif admin_page == "Manage Withdrawals":
        st.subheader("Manage Rider Withdrawals")
        st.caption(
            "Use this section to register DNS/withdrawn riders. "
            "Rest-day DS bonus uses these records by team."
        )

        riders = get_all_riders()
        if not riders:
            st.info("No riders in database yet. Scrape riders first.")
        else:
            riders_df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
            rider_names = sorted(riders_df['Name'].tolist())

            col_rider, col_date = st.columns(2)
            with col_rider:
                selected_rider = st.selectbox("Rider", rider_names, key="withdrawal_rider")
            with col_date:
                selected_date = st.date_input(
                    "Withdrawal date",
                    value=datetime.now(RACE_TIMEZONE).date(),
                    key="withdrawal_date",
                )

            withdrawal_note = st.text_input("Optional note", key="withdrawal_note")
            if st.button("Save Withdrawal", key="save_withdrawal_btn"):
                try:
                    withdrawal_id = upsert_rider_withdrawal(
                        rider_name=selected_rider,
                        withdrawal_date=selected_date.isoformat(),
                        note=withdrawal_note,
                    )
                    st.success(f"Saved withdrawal record (id {withdrawal_id}) for {selected_rider}.")
                except Exception as exc:
                    st.error(f"Could not save withdrawal: {exc}")

            st.divider()
            st.markdown("**Recorded Withdrawals**")
            withdrawal_rows = get_rider_withdrawals()
            if not withdrawal_rows:
                st.info("No withdrawals registered yet.")
            else:
                withdrawal_df = pd.DataFrame(
                    withdrawal_rows,
                    columns=['ID', 'Date', 'Rider ID', 'Rider', 'Team', 'Note'],
                )
                st.dataframe(
                    withdrawal_df[['ID', 'Date', 'Rider', 'Team', 'Note']],
                    use_container_width=True,
                    height=260,
                )

                delete_options = {
                    int(row['ID']): f"#{int(row['ID'])} | {row['Date']} | {row['Rider']} | {row['Team']}"
                    for _, row in withdrawal_df.iterrows()
                }
                selected_delete_id = st.selectbox(
                    "Select withdrawal record to delete",
                    options=list(delete_options.keys()),
                    format_func=lambda withdrawal_id: delete_options[withdrawal_id],
                    key="delete_withdrawal_id",
                )
                if st.button("Delete Selected Withdrawal", key="delete_withdrawal_btn"):
                    if delete_rider_withdrawal(int(selected_delete_id)):
                        st.success("Withdrawal removed.")
                    else:
                        st.warning("Withdrawal record not found.")

    elif admin_page == "Override Riders":
        st.subheader("Override Rider Category & Price")
        st.caption("Use this editor to manually adjust rider category and price, and lock them before race start.")
        st.info("Locked riders keep category/price when you scrape riders again.")

        col_lock_all, col_unlock_all = st.columns(2)
        with col_lock_all:
            if st.button("🔒 Lock All Riders"):
                affected = set_all_riders_lock(True)
                st.success(f"Locked {affected} riders.")
        with col_unlock_all:
            if st.button("🔓 Unlock All Riders"):
                affected = set_all_riders_lock(False)
                st.success(f"Unlocked {affected} riders.")

        riders = get_all_riders_with_lock()
        if not riders:
            st.info("No riders in database yet. Scrape riders first.")
        else:
            df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth', 'Locked'])

            search_query = st.text_input("Search rider", placeholder="Type rider or team name")
            if search_query:
                mask = (
                    df['Name'].str.contains(search_query, case=False, na=False)
                    | df['Team'].str.contains(search_query, case=False, na=False)
                )
                filtered_df = df[mask].copy()
            else:
                filtered_df = df.copy()

            if filtered_df.empty:
                st.warning("No riders match your search.")
            else:
                st.dataframe(
                    filtered_df[['ID', 'Name', 'Team', 'Category', 'Price', 'Locked']].sort_values(['Team', 'Name']),
                    use_container_width=True,
                    height=280,
                )

                rider_names = filtered_df['Name'].tolist()
                selected_name = st.selectbox("Select rider to edit", rider_names)
                selected_row = filtered_df[filtered_df['Name'] == selected_name].iloc[0]

                current_category = str(selected_row['Category'])
                current_price = float(selected_row['Price'])

                category_options = TEAM_CATEGORY_ORDER
                default_category_index = category_options.index(current_category) if current_category in category_options else 0
                new_category = st.selectbox(
                    "Override category",
                    options=category_options,
                    index=default_category_index,
                )
                new_price = st.number_input(
                    "Override price",
                    min_value=0.0,
                    value=current_price,
                    step=0.1,
                    format="%.1f",
                )
                lock_value = st.checkbox("Lock this rider's category/price", value=bool(selected_row['Locked']))

                if st.button("Save Rider Override"):
                    updated = update_rider_overrides(
                        rider_id=int(selected_row['ID']),
                        category=new_category,
                        price=float(new_price),
                    )
                    lock_updated = set_rider_lock(int(selected_row['ID']), lock_value)
                    if updated:
                        lock_text = "locked" if lock_value else "unlocked"
                        st.success(f"Saved override for {selected_name}: {new_category}, {new_price:.1f} points ({lock_text}).")
                    elif not lock_updated:
                        st.error("Could not save lock state. Rider was not found.")
                    else:
                        st.error("Could not save override. Rider was not found.")
    
    elif admin_page == "Database Backup":
        st.subheader("Database Backup & Restore")

        db_info = get_database_file_info()
        if db_info["exists"]:
            size_kb = db_info["size_bytes"] / 1024
            st.caption(
                f"DB: {db_info['path']} | Size: {size_kb:.1f} KB | Last modified: {db_info['modified_at']}"
            )
            current_counts = get_current_database_counts()
            st.write(
                "Current DB rows: "
                f"riders={current_counts['riders']}, players={current_counts['players']}, "
                f"stage_results={current_counts['stage_results']}, stage_points={current_counts['stage_points']}"
            )
        else:
            st.warning("Database file is missing. Initialize DB first.")

        if st.button("Create Backup Snapshot"):
            try:
                backup_name = create_database_backup()
                st.success(f"Backup created: {backup_name}")
            except Exception as exc:
                st.error(f"Backup failed: {exc}")

        backups = list_database_backups()
        if not backups:
            st.info("No backups found in backups/ yet.")
        else:
            selected_backup = st.selectbox("Available backups", options=backups)

            try:
                selected_counts = get_backup_database_counts(selected_backup)
                st.write(
                    "Selected backup rows: "
                    f"riders={selected_counts['riders']}, players={selected_counts['players']}, "
                    f"stage_results={selected_counts['stage_results']}, stage_points={selected_counts['stage_points']}"
                )
                if selected_counts["riders"] == 0:
                    st.warning("This backup has 0 riders. Restoring it will still leave riders empty.")
            except Exception as exc:
                st.error(f"Could not read selected backup contents: {exc}")

            try:
                backup_bytes = read_database_backup_bytes(selected_backup)
                st.download_button(
                    "Download Selected Backup",
                    data=backup_bytes,
                    file_name=selected_backup,
                    mime="application/octet-stream",
                )
            except Exception as exc:
                st.error(f"Could not open backup for download: {exc}")

            confirm_restore = st.checkbox("I understand restore will replace current database file.")
            if st.button("Restore Selected Backup"):
                if not confirm_restore:
                    st.error("Please confirm restore before continuing.")
                else:
                    try:
                        restore_database_backup(selected_backup)
                        st.success(f"Restored database from {selected_backup}.")
                        st.session_state.pop("last_stage_results", None)
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Restore failed: {exc}")

        st.divider()
        st.subheader("📤 Upload External Database")
        st.caption(
            "Upload a giro_fantasy.db SQLite file from another environment or a previous season. "
            "It will be saved as a backup snapshot and immediately restored."
        )
        uploaded_db = st.file_uploader("SQLite .db file", type=["db"], key="upload_db_file")
        if uploaded_db is not None:
            db_bytes = uploaded_db.read()
            st.info(f"Uploaded: {uploaded_db.name} ({len(db_bytes) / 1024:.1f} KB)")
            confirm_upload_restore = st.checkbox(
                "I understand this will replace the current database.",
                key="confirm_upload_restore",
            )
            if st.button("Restore Uploaded Database", key="restore_uploaded_db_btn"):
                if not confirm_upload_restore:
                    st.error("Please confirm before restoring.")
                else:
                    try:
                        backup_name = restore_database_from_bytes(db_bytes)
                        st.success(f"Uploaded database saved as {backup_name} and restored successfully.")
                        st.session_state.pop("last_stage_results", None)
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Restore failed: {exc}")

    elif admin_page == "View Database":
        st.subheader("Database Contents")
        riders = get_all_riders()
        if riders:
            df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])

            stage_points_by_stage = _build_rider_points_by_stage(df)

            for stage_number in sorted(stage_points_by_stage.keys()):
                df[f"Stage {stage_number}"] = df['ID'].map(
                    lambda rider_id: stage_points_by_stage.get(stage_number, {}).get(rider_id, 0)
                )

            stage_columns = [f"Stage {stage_number}" for stage_number in sorted(stage_points_by_stage.keys())]
            df['Total Points'] = df[stage_columns].sum(axis=1) if stage_columns else 0
            st.dataframe(
                df[['ID', 'Name', 'Team', 'Category', 'Price', 'Total Points'] + stage_columns]
                .sort_values(['Total Points', 'Name'], ascending=[False, True]),
                use_container_width=True,
            )
        else:
            st.info("No riders in database yet")

if __name__ == "__main__":
    main()
