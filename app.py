"""
Main Streamlit application for Giro Fantasy Cycling
"""
import streamlit as st
import pandas as pd
from database import (
    init_db, add_player, add_rider, get_all_riders, get_riders_by_category,
    save_player_team, get_player_team, count_transfers, add_transfer
)
from config import TEAM_COMPOSITION, PRICE_RANGES, BUDGET_LIMIT, MAX_TRANSFERS, PLAYERS
from pricing import pricing_engine
from scraper import GiroScraper
import os

# Page config
st.set_page_config(page_title="Giro Fantasy Cycling", layout="wide")

# Initialize session state
if 'db_initialized' not in st.session_state:
    if not os.path.exists('giro_fantasy.db'):
        init_db()
    st.session_state.db_initialized = True

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
        - **Team Size**: 9 cyclists + 1 DS
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
    
    # Organize by category
    tabs = st.tabs(['Captain', 'Sprinter', 'Climber', 'Youth', 'Water Carrier', 'DS'])
    
    selected_riders = {}
    
    for idx, category in enumerate(['captain', 'sprinter', 'climber', 'youth', 'water_carrier', 'ds']):
        with tabs[idx]:
            category_riders = riders_df[riders_df['Category'] == category]
            
            if len(category_riders) > 0:
                st.dataframe(category_riders, use_container_width=True)
                
                needed = TEAM_COMPOSITION.get(category, 1)
                st.write(f"Select **{needed}** rider(s) for this category")
                
                # Multi-select for cyclists, single for DS
                if category == 'ds':
                    selected = st.selectbox(
                        f"Select DS",
                        category_riders['Name'].values,
                        key=f"select_{category}"
                    )
                    selected_riders[category] = [selected] if selected else []
                else:
                    selected = st.multiselect(
                        f"Select {category.title()}s",
                        category_riders['Name'].values,
                        max_selections=needed,
                        key=f"select_{category}"
                    )
                    selected_riders[category] = selected
            else:
                st.warning(f"No {category} riders found")
    
    # Confirm selection
    if st.button("Confirm Team Selection"):
        st.success("Team selected! (This will be saved to database)")

def show_leaderboard():
    """Live leaderboard"""
    st.header("📊 Live Leaderboard")
    
    # Create sample leaderboard data
    leaderboard_data = {
        'Player': PLAYERS,
        'Total Points': [0, 0, 0, 0],
        'Rank': [1, 2, 3, 4]
    }
    
    df_leaderboard = pd.DataFrame(leaderboard_data)
    st.dataframe(df_leaderboard, use_container_width=True)
    
    st.subheader("Stage Breakdown")
    # Will show detailed stage-by-stage scoring

def show_transfers():
    """Transfer management"""
    st.header("🔄 Transfers")
    
    player = st.selectbox("Select your team", PLAYERS, key="transfer_player")
    
    st.write(f"Transfers used: 0 / {MAX_TRANSFERS}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Rider Out**")
        rider_out = st.selectbox("Select rider to remove", ["Rider 1", "Rider 2"])
    
    with col2:
        st.write("**Rider In**")
        rider_in = st.selectbox("Select rider to add", ["Rider A", "Rider B"])
    
    if st.button("Confirm Transfer"):
        st.success("Transfer confirmed! (Will be saved to database)")

def show_admin():
    """Admin panel for managing riders and results"""
    st.header("⚙️ Admin Panel")
    
    admin_page = st.selectbox(
        "Select admin function",
        ["Initialize Players", "Scrape Riders", "Add Results", "View Database"]
    )
    
    if admin_page == "Initialize Players":
        st.subheader("Initialize Players")
        if st.button("Add Default Players"):
            for player_name in PLAYERS:
                add_player(player_name)
            st.success("Players initialized!")
    
    elif admin_page == "Scrape Riders":
        st.subheader("Scrape Riders from Web")
        if st.button("Scrape Giro 2026 Riders"):
            with st.spinner("Scraping..."):
                scraper = GiroScraper()
                # This will be implemented
                st.info("Scraping functionality to be implemented")
    
    elif admin_page == "Add Results":
        st.subheader("Add Stage Results")
        stage_num = st.number_input("Stage number", 1, 21, 1)
        st.info(f"Results entry for stage {stage_num}")
    
    elif admin_page == "View Database":
        st.subheader("Database Contents")
        riders = get_all_riders()
        if riders:
            df = pd.DataFrame(riders, columns=['ID', 'Name', 'Team', 'Category', 'Price', 'Youth'])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No riders in database yet")

if __name__ == "__main__":
    main()
