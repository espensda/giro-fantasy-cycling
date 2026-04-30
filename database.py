"""Database models and helper functions for the Giro Fantasy Cycling app."""

from datetime import datetime, UTC
from pathlib import Path
import sqlite3
import unicodedata

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, create_engine, inspect, text
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker

DB_PATH = "sqlite:///giro_fantasy.db"
PROJECT_DIR = Path(__file__).resolve().parent
BACKUP_DIR = PROJECT_DIR / "backups"

Base = declarative_base()
engine = create_engine(DB_PATH, echo=False)
SessionLocal = sessionmaker(bind=engine)


def _db_file_path() -> Path:
    """Return local SQLite file path from SQLAlchemy DB URL."""
    prefix = "sqlite:///"
    if DB_PATH.startswith(prefix):
        raw = Path(DB_PATH[len(prefix):])
        return raw if raw.is_absolute() else PROJECT_DIR / raw
    return PROJECT_DIR / "giro_fantasy.db"


def _ensure_backup_dir() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return BACKUP_DIR


def get_database_file_info() -> dict:
    """Return basic database file details for admin UI."""
    db_file = _db_file_path()
    if not db_file.exists():
        return {
            "path": str(db_file),
            "exists": False,
            "size_bytes": 0,
            "modified_at": "",
        }

    stat = db_file.stat()
    return {
        "path": str(db_file),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
    }


def list_database_backups() -> list[str]:
    """List available SQLite backups from newest to oldest."""
    backup_dir = _ensure_backup_dir()
    backup_names = [path.name for path in backup_dir.glob("giro_fantasy_*.db") if path.is_file()]
    return sorted(backup_names, reverse=True)


def _table_counts_for_db_file(db_path: Path) -> dict[str, int]:
    """Read row counts for key tables from an SQLite database file."""
    table_names = [
        "riders",
        "players",
        "player_teams",
        "stage_results",
        "stage_points",
        "classification_results",
        "transfers",
        "rider_withdrawals",
    ]
    counts: dict[str, int] = {table_name: 0 for table_name in table_names}
    if not db_path.exists() or not db_path.is_file():
        return counts

    with sqlite3.connect(str(db_path)) as connection:
        cursor = connection.cursor()
        for table_name in table_names:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row = cursor.fetchone()
                counts[table_name] = int(row[0]) if row else 0
            except sqlite3.Error:
                counts[table_name] = 0
    return counts


def get_current_database_counts() -> dict[str, int]:
    """Return row counts for key tables in the active database."""
    return _table_counts_for_db_file(_db_file_path())


def get_backup_database_counts(backup_name: str) -> dict[str, int]:
    """Return row counts for key tables from a named backup file."""
    if not backup_name or "/" in backup_name or "\\" in backup_name:
        raise ValueError("Invalid backup name")

    backup_path = _ensure_backup_dir() / backup_name
    if not backup_path.exists() or not backup_path.is_file():
        raise FileNotFoundError(f"Backup not found: {backup_name}")

    return _table_counts_for_db_file(backup_path)


def read_database_backup_bytes(backup_name: str) -> bytes:
    """Return backup bytes for download from admin UI."""
    if not backup_name or "/" in backup_name or "\\" in backup_name:
        raise ValueError("Invalid backup name")

    backup_path = _ensure_backup_dir() / backup_name
    if not backup_path.exists() or not backup_path.is_file():
        raise FileNotFoundError(f"Backup not found: {backup_name}")
    return backup_path.read_bytes()


def restore_database_from_bytes(data: bytes) -> str:
    """Write raw SQLite bytes as a named backup and restore it. Returns the backup filename."""
    if len(data) < 16 or data[:6] != b"SQLite":
        raise ValueError("Uploaded file does not appear to be a valid SQLite database.")

    backup_dir = _ensure_backup_dir()
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup_name = f"giro_fantasy_{timestamp}.db"
    backup_path = backup_dir / backup_name
    backup_path.write_bytes(data)
    restore_database_backup(backup_name)
    return backup_name


def create_database_backup() -> str:
    """Create a timestamped SQLite backup and return the created filename."""
    db_file = _db_file_path()
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}")

    backup_dir = _ensure_backup_dir()
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup_name = f"giro_fantasy_{timestamp}.db"
    backup_path = backup_dir / backup_name

    with sqlite3.connect(str(db_file)) as source_conn:
        with sqlite3.connect(str(backup_path)) as target_conn:
            source_conn.backup(target_conn)

    return backup_name


def restore_database_backup(backup_name: str) -> None:
    """Restore database from a named backup in the backups folder."""
    if not backup_name or "/" in backup_name or "\\" in backup_name:
        raise ValueError("Invalid backup name")

    backup_path = _ensure_backup_dir() / backup_name
    if not backup_path.exists() or not backup_path.is_file():
        raise FileNotFoundError(f"Backup not found: {backup_name}")

    db_file = _db_file_path()
    db_file.parent.mkdir(parents=True, exist_ok=True)
    engine.dispose()

    with sqlite3.connect(str(backup_path)) as source_conn:
        with sqlite3.connect(str(db_file)) as target_conn:
            source_conn.backup(target_conn)

    # Ensure expected schema migrations are present after restore.
    init_db()


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    teams = relationship("PlayerTeam", back_populates="player", cascade="all, delete-orphan")


class Rider(Base):
    __tablename__ = "riders"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    team = Column(String, nullable=False, default="Unknown")
    category = Column(String, nullable=False)
    price = Column(Float, nullable=False, default=0)
    youth = Column(Boolean, nullable=False, default=False)
    category_locked = Column(Boolean, nullable=False, default=False)


class PlayerTeam(Base):
    __tablename__ = "player_teams"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    rider_id = Column(Integer, ForeignKey("riders.id"), nullable=False)
    player = relationship("Player", back_populates="teams")
    rider = relationship("Rider")


class StageResult(Base):
    __tablename__ = "stage_results"

    id = Column(Integer, primary_key=True)
    rider_id = Column(Integer, ForeignKey("riders.id"), nullable=False)
    stage_number = Column(Integer, nullable=False)
    position = Column(Integer, nullable=False, default=0)
    result_time = Column(String)


class ClassificationResult(Base):
    __tablename__ = "classification_results"

    id = Column(Integer, primary_key=True)
    rider_id = Column(Integer, ForeignKey("riders.id"), nullable=False)
    stage_number = Column(Integer, nullable=False)
    classification = Column(String, nullable=False)
    position = Column(Integer, nullable=False, default=0)
    result_value = Column(String, nullable=False, default="")


class StagePoints(Base):
    __tablename__ = "stage_points"

    id = Column(Integer, primary_key=True)
    rider_id = Column(Integer, ForeignKey("riders.id"), nullable=False)
    stage_number = Column(Integer, nullable=False)
    points = Column(Float, nullable=False, default=0)


class Transfer(Base):
    __tablename__ = "transfers"

    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    rider_out_id = Column(Integer, ForeignKey("riders.id"), nullable=True)
    rider_in_id = Column(Integer, ForeignKey("riders.id"), nullable=True)
    transfer_date = Column(String, nullable=False)


class RiderWithdrawal(Base):
    __tablename__ = "rider_withdrawals"

    id = Column(Integer, primary_key=True)
    rider_id = Column(Integer, ForeignKey("riders.id"), nullable=False, unique=True)
    withdrawal_date = Column(String, nullable=False)
    note = Column(String, nullable=False, default="")


SEED_DB_PATH = PROJECT_DIR / "data" / "seed.db"


def init_db() -> None:
    """Create all tables if they do not already exist.

    On a fresh deployment (no giro_fantasy.db yet), seed from data/seed.db if
    it is present so the app starts with pre-loaded demo data.
    """
    db_file = _db_file_path()
    if not db_file.exists() and SEED_DB_PATH.exists():
        import shutil
        shutil.copy2(SEED_DB_PATH, db_file)
        # Re-bind engine so SQLAlchemy sees the copied file
        engine.dispose()

    Base.metadata.create_all(bind=engine)
    inspector = inspect(engine)
    if "stage_results" in inspector.get_table_names():
        column_names = {column["name"] for column in inspector.get_columns("stage_results")}
        if "position" not in column_names:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE stage_results ADD COLUMN position INTEGER DEFAULT 0"))
    if "riders" in inspector.get_table_names():
        rider_column_names = {column["name"] for column in inspector.get_columns("riders")}
        if "category_locked" not in rider_column_names:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE riders ADD COLUMN category_locked BOOLEAN DEFAULT 0"))


def _get_session() -> Session:
    return SessionLocal()


def _canonical_name(name: str) -> str:
    """Return a comparison-friendly name key (case and accent insensitive)."""
    normalized = unicodedata.normalize("NFKD", name or "")
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(no_accents.lower().split())


def add_player(name: str) -> int:
    """Insert a player if missing and return the player id."""
    session = _get_session()
    try:
        existing = session.query(Player).filter(Player.name == name).first()
        if existing:
            return existing.id

        player = Player(name=name)
        session.add(player)
        session.commit()
        session.refresh(player)
        return player.id
    finally:
        session.close()


def add_rider(name: str, team: str, category: str, price: float, youth: bool = False) -> int:
    """Insert a rider if missing and return the rider id."""
    session = _get_session()
    try:
        existing = session.query(Rider).filter(Rider.name == name).first()
        if existing:
            existing.team = team
            if not existing.category_locked:
                existing.category = category
                existing.price = price
                existing.youth = youth
            session.commit()
            return existing.id

        rider = Rider(name=name, team=team, category=category, price=price, youth=youth)
        session.add(rider)
        session.commit()
        session.refresh(rider)
        return rider.id
    finally:
        session.close()


def update_rider_overrides(rider_id: int, category: str, price: float) -> bool:
    """Update rider category and price overrides, returning True if rider was found."""
    session = _get_session()
    try:
        rider = session.query(Rider).filter(Rider.id == rider_id).first()
        if rider is None:
            return False

        rider.category = category
        rider.price = float(price)
        session.commit()
        return True
    finally:
        session.close()


def set_rider_lock(rider_id: int, category_locked: bool) -> bool:
    """Set lock state for rider category/price, returning True if rider exists."""
    session = _get_session()
    try:
        rider = session.query(Rider).filter(Rider.id == rider_id).first()
        if rider is None:
            return False

        rider.category_locked = bool(category_locked)
        session.commit()
        return True
    finally:
        session.close()


def set_all_riders_lock(category_locked: bool) -> int:
    """Set lock state for all riders and return number of affected rows."""
    session = _get_session()
    try:
        affected = session.query(Rider).update({Rider.category_locked: bool(category_locked)})
        session.commit()
        return int(affected or 0)
    finally:
        session.close()


def get_all_riders_with_lock() -> list[tuple[int, str, str, str, float, bool, bool]]:
    """Return all riders including lock state for admin tools."""
    session = _get_session()
    try:
        riders = session.query(Rider).order_by(Rider.team, Rider.name).all()
        return [
            (r.id, r.name, r.team, r.category, r.price, r.youth, bool(r.category_locked))
            for r in riders
        ]
    finally:
        session.close()


def get_all_riders() -> list[tuple[int, str, str, str, float, bool]]:
    """Return all riders in the tuple format expected by the app."""
    session = _get_session()
    try:
        riders = session.query(Rider).order_by(Rider.category, Rider.name).all()
        return [(r.id, r.name, r.team, r.category, r.price, r.youth) for r in riders]
    finally:
        session.close()


def get_riders_by_category(category: str) -> list[tuple[int, str, str, str, float, bool]]:
    """Return riders for a single category."""
    session = _get_session()
    try:
        riders = session.query(Rider).filter(Rider.category == category).order_by(Rider.name).all()
        return [(r.id, r.name, r.team, r.category, r.price, r.youth) for r in riders]
    finally:
        session.close()


def save_player_team(player_name: str, rider_names: list[str]) -> None:
    """Replace a player's team selection with the provided rider names."""
    session = _get_session()
    try:
        player = session.query(Player).filter(Player.name == player_name).first()
        if player is None:
            player = Player(name=player_name)
            session.add(player)
            session.flush()

        session.query(PlayerTeam).filter(PlayerTeam.player_id == player.id).delete()

        if rider_names:
            riders = session.query(Rider).filter(Rider.name.in_(rider_names)).all()
            for rider in riders:
                session.add(PlayerTeam(player_id=player.id, rider_id=rider.id))

        session.commit()
    finally:
        session.close()


def get_player_team(player_name: str) -> list[tuple[int, str, str, str, float, bool]]:
    """Get a player's selected riders in app tuple format."""
    session = _get_session()
    try:
        player = session.query(Player).filter(Player.name == player_name).first()
        if player is None:
            return []

        team_rows = (
            session.query(Rider)
            .join(PlayerTeam, PlayerTeam.rider_id == Rider.id)
            .filter(PlayerTeam.player_id == player.id)
            .all()
        )
        return [(r.id, r.name, r.team, r.category, r.price, r.youth) for r in team_rows]
    finally:
        session.close()


def count_transfers(player_name: str) -> int:
    """Return number of transfers made by a player."""
    session = _get_session()
    try:
        player = session.query(Player).filter(Player.name == player_name).first()
        if player is None:
            return 0
        return session.query(Transfer).filter(Transfer.player_id == player.id).count()
    finally:
        session.close()


def add_transfer(player_name: str, rider_out_name: str | None = None, rider_in_name: str | None = None) -> int:
    """Record a transfer and return transfer id."""
    session = _get_session()
    try:
        player = session.query(Player).filter(Player.name == player_name).first()
        if player is None:
            player = Player(name=player_name)
            session.add(player)
            session.flush()

        rider_out_id = None
        rider_in_id = None

        if rider_out_name:
            rider_out = session.query(Rider).filter(Rider.name == rider_out_name).first()
            rider_out_id = rider_out.id if rider_out else None

        if rider_in_name:
            rider_in = session.query(Rider).filter(Rider.name == rider_in_name).first()
            rider_in_id = rider_in.id if rider_in else None

        transfer = Transfer(
            player_id=player.id,
            rider_out_id=rider_out_id,
            rider_in_id=rider_in_id,
            transfer_date=datetime.now(UTC).isoformat(),
        )
        session.add(transfer)
        session.commit()
        session.refresh(transfer)
        return transfer.id
    finally:
        session.close()


def save_stage_results(stage_number: int, result_rows: list[dict]) -> tuple[int, int, list[str]]:
    """Replace saved stage results for a stage and return import stats."""
    session = _get_session()
    try:
        existing_count = session.query(StageResult).filter(StageResult.stage_number == stage_number).count()
        session.query(StageResult).filter(StageResult.stage_number == stage_number).delete()

        rider_names = [row["name"] for row in result_rows if row.get("name")]
        riders = session.query(Rider).filter(Rider.name.in_(rider_names)).all() if rider_names else []
        rider_ids_by_name = {rider.name: rider.id for rider in riders}
        all_riders = session.query(Rider).all()
        rider_ids_by_canonical: dict[str, int] = {}
        for rider in all_riders:
            rider_ids_by_canonical.setdefault(_canonical_name(rider.name), rider.id)

        inserted = 0
        unmatched: list[str] = []
        for row in result_rows:
            rider_name = row.get("name", "")
            rider_id = rider_ids_by_name.get(rider_name)
            if rider_id is None and rider_name:
                rider_id = rider_ids_by_canonical.get(_canonical_name(rider_name))
            if rider_id is None:
                if rider_name:
                    unmatched.append(rider_name)
                continue

            session.add(
                StageResult(
                    rider_id=rider_id,
                    stage_number=stage_number,
                    position=int(row.get("position") or 0),
                    result_time=row.get("time", ""),
                )
            )
            inserted += 1

        session.commit()
        return inserted, existing_count, sorted(set(unmatched))
    finally:
        session.close()


def get_stage_results(stage_number: int | None = None) -> list[tuple[int, int, str, str, int, str]]:
    """Return stage results joined with rider metadata for display."""
    session = _get_session()
    try:
        query = (
            session.query(StageResult, Rider)
            .join(Rider, Rider.id == StageResult.rider_id)
            .order_by(StageResult.stage_number, StageResult.position, Rider.name)
        )
        if stage_number is not None:
            query = query.filter(StageResult.stage_number == stage_number)

        rows = query.all()
        return [
            (
                stage_result.stage_number,
                stage_result.position,
                rider.name,
                rider.team,
                rider.id,
                stage_result.result_time or "",
            )
            for stage_result, rider in rows
        ]
    finally:
        session.close()


def save_stage_points(stage_number: int, result_rows: list[dict]) -> tuple[int, int, list[str]]:
    """Replace saved stage points for a stage and return import stats."""
    session = _get_session()
    try:
        existing_count = session.query(StagePoints).filter(StagePoints.stage_number == stage_number).count()
        session.query(StagePoints).filter(StagePoints.stage_number == stage_number).delete()

        rider_names = [row["name"] for row in result_rows if row.get("name")]
        riders = session.query(Rider).filter(Rider.name.in_(rider_names)).all() if rider_names else []
        rider_ids_by_name = {rider.name: rider.id for rider in riders}
        all_riders = session.query(Rider).all()
        rider_ids_by_canonical: dict[str, int] = {}
        for rider in all_riders:
            rider_ids_by_canonical.setdefault(_canonical_name(rider.name), rider.id)

        aggregated_points_by_rider_id: dict[int, float] = {}
        unmatched: list[str] = []
        for row in result_rows:
            rider_name = row.get("name", "")
            rider_id = rider_ids_by_name.get(rider_name)
            if rider_id is None and rider_name:
                rider_id = rider_ids_by_canonical.get(_canonical_name(rider_name))
            if rider_id is None:
                if rider_name:
                    unmatched.append(rider_name)
                continue

            try:
                points = float(row.get("points") or 0)
            except (TypeError, ValueError):
                points = 0.0

            aggregated_points_by_rider_id[rider_id] = (
                aggregated_points_by_rider_id.get(rider_id, 0.0) + points
            )

        for rider_id, points in aggregated_points_by_rider_id.items():
            session.add(
                StagePoints(
                    rider_id=rider_id,
                    stage_number=stage_number,
                    points=points,
                )
            )

        session.commit()
        return len(aggregated_points_by_rider_id), existing_count, sorted(set(unmatched))
    finally:
        session.close()


def get_stage_points(stage_number: int | None = None) -> list[tuple[int, str, str, int, float]]:
    """Return saved stage points joined with rider metadata for display/scoring."""
    session = _get_session()
    try:
        query = (
            session.query(StagePoints, Rider)
            .join(Rider, Rider.id == StagePoints.rider_id)
            .order_by(StagePoints.stage_number, Rider.name)
        )
        if stage_number is not None:
            query = query.filter(StagePoints.stage_number == stage_number)

        rows = query.all()
        aggregated: dict[tuple[int, int], tuple[str, str, float]] = {}
        for stage_points, rider in rows:
            key = (stage_points.stage_number, rider.id)
            existing = aggregated.get(key)
            if existing is None:
                aggregated[key] = (rider.name, rider.team, float(stage_points.points or 0))
            else:
                aggregated[key] = (
                    existing[0],
                    existing[1],
                    existing[2] + float(stage_points.points or 0),
                )

        return [
            (stage_number_key, rider_name, rider_team, rider_id, points)
            for (stage_number_key, rider_id), (rider_name, rider_team, points) in sorted(
                aggregated.items(),
                key=lambda item: (item[0][0], item[1][0]),
            )
        ]
    finally:
        session.close()


def save_classification_results(classification: str, stage_number: int, result_rows: list[dict]) -> tuple[int, int, list[str]]:
    """Replace saved classification results for a stage and return import stats."""
    session = _get_session()
    try:
        existing_count = (
            session.query(ClassificationResult)
            .filter(
                ClassificationResult.classification == classification,
                ClassificationResult.stage_number == stage_number,
            )
            .count()
        )
        (
            session.query(ClassificationResult)
            .filter(
                ClassificationResult.classification == classification,
                ClassificationResult.stage_number == stage_number,
            )
            .delete()
        )

        rider_names = [row["name"] for row in result_rows if row.get("name")]
        riders = session.query(Rider).filter(Rider.name.in_(rider_names)).all() if rider_names else []
        rider_ids_by_name = {rider.name: rider.id for rider in riders}
        all_riders = session.query(Rider).all()
        rider_ids_by_canonical: dict[str, int] = {}
        for rider in all_riders:
            rider_ids_by_canonical.setdefault(_canonical_name(rider.name), rider.id)

        inserted = 0
        unmatched: list[str] = []
        for row in result_rows:
            rider_name = row.get("name", "")
            rider_id = rider_ids_by_name.get(rider_name)
            if rider_id is None and rider_name:
                rider_id = rider_ids_by_canonical.get(_canonical_name(rider_name))
            if rider_id is None:
                if rider_name:
                    unmatched.append(rider_name)
                continue

            session.add(
                ClassificationResult(
                    rider_id=rider_id,
                    stage_number=stage_number,
                    classification=classification,
                    position=int(row.get("position") or 0),
                    result_value=row.get("value", ""),
                )
            )
            inserted += 1

        session.commit()
        return inserted, existing_count, sorted(set(unmatched))
    finally:
        session.close()


def get_classification_results(classification: str, stage_number: int | None = None) -> list[tuple[int, str, int, str, str, int, str]]:
    """Return classification results joined with rider metadata for display."""
    session = _get_session()
    try:
        query = (
            session.query(ClassificationResult, Rider)
            .join(Rider, Rider.id == ClassificationResult.rider_id)
            .filter(ClassificationResult.classification == classification)
            .order_by(ClassificationResult.stage_number, ClassificationResult.position, Rider.name)
        )
        if stage_number is not None:
            query = query.filter(ClassificationResult.stage_number == stage_number)

        rows = query.all()
        return [
            (
                classification_result.stage_number,
                classification_result.classification,
                classification_result.position,
                rider.name,
                rider.team,
                rider.id,
                classification_result.result_value or "",
            )
            for classification_result, rider in rows
        ]
    finally:
        session.close()


def clear_riders() -> int:
    """Delete all riders and rider-dependent records, returning removed rider count."""
    session = _get_session()
    try:
        removed = session.query(Rider).count()
        session.query(PlayerTeam).delete()
        session.query(StageResult).delete()
        session.query(StagePoints).delete()
        session.query(ClassificationResult).delete()
        session.query(Transfer).delete()
        session.query(RiderWithdrawal).delete()
        session.query(Rider).delete()
        session.commit()
        return removed
    finally:
        session.close()


def upsert_rider_withdrawal(rider_name: str, withdrawal_date: str, note: str = "") -> int:
    """Create or update a rider withdrawal record and return its id."""
    session = _get_session()
    try:
        rider = session.query(Rider).filter(Rider.name == rider_name).first()
        if rider is None:
            raise ValueError(f"Rider not found: {rider_name}")

        row = session.query(RiderWithdrawal).filter(RiderWithdrawal.rider_id == rider.id).first()
        if row is None:
            row = RiderWithdrawal(
                rider_id=rider.id,
                withdrawal_date=str(withdrawal_date),
                note=str(note or "").strip(),
            )
            session.add(row)
        else:
            row.withdrawal_date = str(withdrawal_date)
            row.note = str(note or "").strip()

        session.commit()
        session.refresh(row)
        return row.id
    finally:
        session.close()


def get_rider_withdrawals() -> list[tuple[int, str, int, str, str, str]]:
    """Return rider withdrawals joined with rider metadata for UI/scoring."""
    session = _get_session()
    try:
        rows = (
            session.query(RiderWithdrawal, Rider)
            .join(Rider, Rider.id == RiderWithdrawal.rider_id)
            .order_by(RiderWithdrawal.withdrawal_date, Rider.name)
            .all()
        )
        return [
            (
                withdrawal.id,
                withdrawal.withdrawal_date,
                rider.id,
                rider.name,
                rider.team,
                withdrawal.note or "",
            )
            for withdrawal, rider in rows
        ]
    finally:
        session.close()


def delete_rider_withdrawal(withdrawal_id: int) -> bool:
    """Delete a rider withdrawal by id and return True if deleted."""
    session = _get_session()
    try:
        row = session.query(RiderWithdrawal).filter(RiderWithdrawal.id == int(withdrawal_id)).first()
        if row is None:
            return False

        session.delete(row)
        session.commit()
        return True
    finally:
        session.close()
