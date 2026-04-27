"""
Main Streamlit application for Giro Fantasy Cycling
"""
import streamlit as st
import pandas as pd
from difflib import get_close_matches
from database import (
    init_db, add_player, add_rider, get_all_riders, get_riders_by_category,
    save_player_team, get_player_team, count_transfers, add_transfer, clear_riders,
    save_stage_results, get_stage_results, save_classification_results, get_classification_results,
    save_stage_points, get_stage_points, update_rider_overrides,
    set_rider_lock, set_all_riders_lock, get_all_riders_with_lock,
)
from config import TEAM_COMPOSITION, PRICE_RANGES, BUDGET_LIMIT, MAX_TRANSFERS, PLAYERS, SEASON_YEAR
from pricing import assign_prices
from scraper import GiroScraper
from scoring import calculate_leaderboard, ScoringSystem
import os

# Page config
st.set_page_config(page_title="Giro Fantasy Cycling", layout="wide")

# Initialize session state
if 'db_initialized' not in st.session_state:
    init_db()
    st.session_state.db_initialized = True


TEAM_CATEGORY_ORDER = ['captain', 'sprinter', 'climber', 'youth', 'water_carrier', 'ds']
TEAM_CATEGORY_LABELS = {
    'captain': 'Captains',
    'sprinter': 'Sprinters',
    'climber': 'Climbers',
    'youth': 'Youth Riders',
    'water_carrier': 'Water Carriers',
    'ds': 'DS',
}

CLASSIFICATION_LABELS = {
    'gc': 'GC Points',
    'mountains': 'Mountain Points',
    'sprints': 'Sprint Points',
}

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

def main():
    st.title("🚴 Giro d'Italia Fantasy Cycling 2026")
    st.markdown("---")
    
    # Sidebar menu
    page = st.sidebar.radio(
        "Navigate",
        ["Home", "Team Selection", "Live Leaderboard", "Transfers", "Admin"]
    )
    
    if page == "Home":
        show_home()
    elif page == "Team Selection":
        show_team_selection()
    elif page == "Live Leaderboard":
        show_leaderboard()
    elif page == "Transfers":
        show_transfers()
    elif page == "Admin":
        show_admin()

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
        - **Deadline**: Before next stage starts
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
    
    st.subheader("👥 Players")
    for idx, player in enumerate(PLAYERS, 1):
        st.write(f"{idx}. {player}")

def show_team_selection():
    """Team selection page"""
    st.header("🎯 Team Selection")
    
    # Player selector
    player = st.selectbox("Select your team", PLAYERS)
    
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
        key="team_table_category_filter",
    )

    filtered_overview_df = riders_df[riders_df['Category'].isin(category_filter)].copy()
    filtered_overview_df['Category'] = filtered_overview_df['Category'].map(TEAM_CATEGORY_LABELS)
    st.dataframe(filtered_overview_df[['ID', 'Name', 'Team', 'Category', 'Price']], use_container_width=True)

    st.subheader("Build Your Team")
    selected_riders = {}
    selector_columns = st.columns(2)

    for idx, category in enumerate(TEAM_CATEGORY_ORDER):
        with selector_columns[idx % 2]:
            category_riders = riders_df[riders_df['Category'] == category].sort_values(['Team', 'Name'])
            options = category_riders['Name'].tolist()
            needed = TEAM_COMPOSITION.get(category, 1)
            st.write(f"**{TEAM_CATEGORY_LABELS[category]}**: select {needed}")

            if category == 'ds':
                ds_team_to_name = (
                    category_riders
                    .drop_duplicates(subset=['Team'], keep='first')
                    .set_index('Team')['Name']
                    .to_dict()
                )
                ds_team_options = ['-- Select DS Team --'] + sorted(ds_team_to_name.keys())

                default_ds = existing_by_category.get(category, [])
                default_team = '-- Select DS Team --'
                if default_ds:
                    default_match = category_riders[category_riders['Name'] == default_ds[0]]
                    if not default_match.empty:
                        default_team = default_match.iloc[0]['Team']

                selected_team = st.selectbox(
                    "Select DS Team",
                    ds_team_options,
                    index=ds_team_options.index(default_team),
                    key=f"select_{category}",
                )
                selected_name = ds_team_to_name.get(selected_team)
                selected_riders[category] = [selected_name] if selected_name else []
            else:
                option_ids = category_riders['ID'].tolist()
                id_to_name = category_riders.set_index('ID')['Name'].to_dict()
                name_to_id = category_riders.set_index('Name')['ID'].to_dict()
                id_to_label = {
                    row['ID']: f"{row['Name']} | {row['Team']} | {float(row['Price']):.1f}"
                    for _, row in category_riders.iterrows()
                }
                default_ids = [
                    name_to_id[name]
                    for name in existing_by_category.get(category, [])
                    if name in name_to_id
                ]
                selected_ids = st.multiselect(
                    f"Select {TEAM_CATEGORY_LABELS[category]}",
                    option_ids,
                    default=default_ids,
                    max_selections=needed,
                    format_func=lambda rider_id: id_to_label[rider_id],
                    key=f"select_{category}",
                )
                selected_riders[category] = [id_to_name[rider_id] for rider_id in selected_ids]

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
    leaderboard_rows, breakdown_by_player = calculate_leaderboard(
        players=PLAYERS,
        player_teams=player_teams,
        stage_results=stage_results,
        classification_results=classification_results,
        stage_points=stage_points,
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
    
    player = st.selectbox("Select your team", PLAYERS, key="transfer_player")
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
        ["Initialize Players", "Scrape Riders", "Clear Riders", "Add Results", "Override Riders", "View Database"]
    )
    
    if admin_page == "Initialize Players":
        st.subheader("Initialize Players")
        if st.button("Add Default Players"):
            for player_name in PLAYERS:
                add_player(player_name)
            st.success("Players initialized!")
    
    elif admin_page == "Scrape Riders":
        st.subheader("Scrape Riders from Web")
        scrape_year = st.number_input("Giro year", min_value=2000, max_value=2100, value=2025, step=1)
        if st.button("Scrape Giro Startlist"):
            with st.spinner("Scraping..."):
                scraper = GiroScraper()
                try:
                    scraped_rows = scraper.scrape_giro_startlist(year=int(scrape_year))
                    priced_rows = assign_prices(scraped_rows)
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

                    st.success(f"Scraped {len(priced_rows)} entries for {scrape_year}.")
                    st.info(f"Inserted {inserted} new entries and updated {updated} existing names.")
                except Exception as exc:
                    st.error(f"Scrape failed: {exc}")

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
    
    elif admin_page == "View Database":
        st.subheader("Database Contents")
        riders = get_all_riders()
        if riders:
            df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])

            scoring = ScoringSystem()
            stage_points_by_stage: dict[int, dict[int, float]] = {}
            uploaded_stage_points = get_stage_points()
            stage_rows = get_stage_results()
            stage_rows_by_stage: dict[int, list[tuple]] = {}
            rider_category_by_id = {
                int(row['ID']): row['Category']
                for _, row in df.iterrows()
            }
            rider_team_by_id = {
                int(row['ID']): row['Team']
                for _, row in df.iterrows()
            }

            if uploaded_stage_points:
                uploaded_by_stage: dict[int, list[tuple[str, str, int, float]]] = {}
                for stage_number, rider_name, rider_team, rider_id, points in uploaded_stage_points:
                    uploaded_by_stage.setdefault(stage_number, []).append(
                        (rider_name, rider_team, rider_id, float(points))
                    )

                for row in stage_rows:
                    stage_rows_by_stage.setdefault(row[0], []).append(row)

                for stage_number, rows in uploaded_by_stage.items():
                    uploaded_points_by_rider_id: dict[int, float] = {}
                    for _, _, rider_id, points in rows:
                        uploaded_points_by_rider_id[rider_id] = (
                            uploaded_points_by_rider_id.get(rider_id, 0.0) + float(points)
                        )

                    legacy_rows = stage_rows_by_stage.get(stage_number, [])
                    winning_team = next((team for _, position, _, team, _, _ in legacy_rows if position == 1), None)
                    if winning_team is None and rows:
                        winner_row = max(rows, key=lambda row: row[3])
                        winning_team = winner_row[1]

                    stage_points_by_stage.setdefault(stage_number, {})

                    for rider_id, category in rider_category_by_id.items():
                        if category == 'ds':
                            continue
                        stage_points_by_stage[stage_number][rider_id] = (
                            stage_points_by_stage[stage_number].get(rider_id, 0)
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

                        ds_team = row['Team']
                        if legacy_rows:
                            stage_points_by_stage[stage_number][rider_id] = (
                                stage_points_by_stage[stage_number].get(rider_id, 0)
                                + scoring.calculate_ds_stage_score(ds_team, legacy_rows)
                            )
            else:
                for row in stage_rows:
                    stage_rows_by_stage.setdefault(row[0], []).append(row)

                classifications_by_stage: dict[int, dict[str, list[tuple]]] = {}
                for classification_key in CLASSIFICATION_LABELS:
                    for row in get_classification_results(classification_key):
                        classifications_by_stage.setdefault(row[0], {}).setdefault(classification_key, []).append(row)

                for stage_number, rows in stage_rows_by_stage.items():
                    winning_team = next((team for _, position, _, team, _, _ in rows if position == 1), None)
                    stage_position_by_rider_id = {
                        rider_id: position
                        for _, position, _, _, rider_id, _ in rows
                    }
                    stage_points_by_stage.setdefault(stage_number, {})
                    for rider_id, category in rider_category_by_id.items():
                        if category == 'ds':
                            continue
                        stage_points_by_stage[stage_number][rider_id] = (
                            stage_points_by_stage[stage_number].get(rider_id, 0)
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

                        ds_team = row['Team']
                        stage_points_by_stage[stage_number][rider_id] = (
                            stage_points_by_stage[stage_number].get(rider_id, 0)
                            + scoring.calculate_ds_stage_score(ds_team, rows)
                        )

                    for classification_key, class_rows in classifications_by_stage.get(stage_number, {}).items():
                        for _, _, position, _, _, rider_id, value in class_rows:
                            stage_points_by_stage[stage_number][rider_id] = (
                                stage_points_by_stage[stage_number].get(rider_id, 0)
                                + scoring.calculate_classification_score(classification_key, position, value)
                            )

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
