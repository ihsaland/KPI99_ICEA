"""KPI99 branding for reports: colors and logo path."""
from pathlib import Path

# KPI99 color scheme (from KPI99/ICEA static styles)
KPI99_DARK = "#1e293b"       # body text, headings
KPI99_DARK_ALT = "#334155"   # secondary text
KPI99_MUTED = "#64748b"      # small text, footer
KPI99_ACCENT = "#3b82f6"     # primary blue (links, buttons, table header)
KPI99_ACCENT_DARK = "#1e3a8a"  # dark blue (card headings)
KPI99_GRID = "#e2e8f0"       # table grid, borders
KPI99_LIGHT_BG = "#f0f9ff"   # light blue background
KPI99_SUCCESS = "#10b981"    # green (utilization)
KPI99_WARN = "#ef4444"       # red (waste)
KPI99_WHITE = "#ffffff"


def _static_dir(static_dir: Path | None = None) -> Path | None:
    """Resolve static directory for assets."""
    if static_dir is not None and Path(static_dir).is_dir():
        return Path(static_dir).resolve()
    here = Path(__file__).resolve().parent.parent.parent / "static"
    if here.is_dir():
        return here
    cwd = Path.cwd() / "static"
    if cwd.is_dir():
        return cwd
    return None


def _project_root() -> Path:
    """Project root (parent of icea package)."""
    return Path(__file__).resolve().parent.parent.parent


def _asset_candidates(filename: str, static_dir: Path | None = None) -> list[Path]:
    """Candidate paths for an asset: static dir first, then project root, then cwd."""
    candidates = []
    base = _static_dir(static_dir)
    if base is not None:
        candidates.append(base / filename)
    root = _project_root()
    candidates.append(root / filename)
    candidates.append(root / "static" / filename)
    cwd = Path.cwd()
    if cwd != root:
        candidates.append(cwd / filename)
        candidates.append(cwd / "static" / filename)
    return candidates


def get_logo_path(static_dir: Path | None = None) -> Path | None:
    """Path to menu_logo.png; checks static dir, project root, and cwd."""
    for path in _asset_candidates("menu_logo.png", static_dir):
        if path.is_file():
            return path
    return None


def get_favicon_path(static_dir: Path | None = None) -> Path | None:
    """Path to favicon (png or ico); checks static dir, project root, and cwd. Included on all reports."""
    for name in ("favicon.png", "favicon.ico"):
        for path in _asset_candidates(name, static_dir):
            if path.is_file():
                return path
    return None
