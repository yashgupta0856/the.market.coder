from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from utils.mongo import get_collection
from web.services.access_control import user_has_active_access

import yfinance as yf

router = APIRouter(tags=["Watchlist"])
templates = Jinja2Templates(directory="web/templates")

# ─── In-memory price cache (TTL = 30 seconds) ───────────────────
_price_cache = {}      # { symbol: { data: {...}, ts: float } }
CACHE_TTL = 30         # seconds


# ─── HELPER: Fetch price from yfinance ───────────────────────────

def fetch_price(symbol: str) -> dict:
    """
    Fetch current market price for a symbol using yfinance.
    For Indian stocks, appends .NS if no suffix present.
    Returns dict with price, change, change_pct, and yf_symbol.
    """
    yf_symbol = symbol

    # Auto-append .NS for Indian stocks (no dot = assume NSE)
    if "." not in symbol and not symbol.startswith("^"):
        yf_symbol = symbol + ".NS"

    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.fast_info

        price = round(info.get("lastPrice", 0) or 0, 2)
        prev_close = round(info.get("previousClose", 0) or 0, 2)

        if price == 0:
            # Fallback: try without .NS
            if yf_symbol.endswith(".NS"):
                yf_symbol = symbol
                ticker = yf.Ticker(yf_symbol)
                info = ticker.fast_info
                price = round(info.get("lastPrice", 0) or 0, 2)
                prev_close = round(info.get("previousClose", 0) or 0, 2)

        change = round(price - prev_close, 2) if prev_close else 0
        change_pct = round((change / prev_close) * 100, 2) if prev_close else 0

        return {
            "price": price,
            "prev_close": prev_close,
            "change": change,
            "change_pct": change_pct,
            "yf_symbol": yf_symbol,
            "valid": price > 0
        }
    except Exception:
        return {"price": 0, "prev_close": 0, "change": 0, "change_pct": 0, "yf_symbol": yf_symbol, "valid": False}


# ─── WATCHLIST PAGE ──────────────────────────────────────────────

@router.get("/watchlist")
def watchlist_page(request: Request):
    email = request.session.get("user_email")
    if not email:
        return RedirectResponse("/login", status_code=302)
    if not user_has_active_access(email):
        return RedirectResponse("/renew", status_code=302)

    return templates.TemplateResponse("watchlist.html", {"request": request})


# ─── API: GET ALL WATCHLIST ITEMS ────────────────────────────────

@router.get("/api/watchlist")
def get_watchlist(request: Request):
    email = request.session.get("user_email")
    if not email:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    col = get_collection("watchlists")
    doc = col.find_one({"email": email}, {"_id": 0, "stocks": 1})
    stocks = doc.get("stocks", []) if doc else []

    return JSONResponse({"stocks": stocks})


# ─── API: FETCH LIVE PRICES FOR ALL WATCHLIST STOCKS ─────────────

@router.get("/api/watchlist/prices")
def get_watchlist_prices(request: Request):
    """Fetch live prices for all stocks in user's watchlist."""
    email = request.session.get("user_email")
    if not email:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    col = get_collection("watchlists")
    doc = col.find_one({"email": email}, {"_id": 0, "stocks": 1})
    stocks = doc.get("stocks", []) if doc else []

    prices = {}
    for s in stocks:
        sym = s.get("symbol", "")
        data = fetch_price(sym)
        prices[sym] = data

    return JSONResponse({"prices": prices})


# ─── API: ADD STOCK ──────────────────────────────────────────────

@router.post("/api/watchlist/add")
def add_to_watchlist(request: Request, symbol: str = Form(...), notes: str = Form("")):
    email = request.session.get("user_email")
    if not email:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    symbol = symbol.strip().upper()
    if not symbol:
        return JSONResponse({"error": "Symbol is required"}, status_code=400)

    # Validate symbol exists on yfinance
    price_data = fetch_price(symbol)
    if not price_data["valid"]:
        return JSONResponse({"error": f"'{symbol}' not found on market. Check the symbol name."}, status_code=400)

    col = get_collection("watchlists")

    # Check if symbol already exists
    existing = col.find_one({"email": email, "stocks.symbol": symbol})
    if existing:
        return JSONResponse({"error": f"{symbol} already in watchlist"}, status_code=409)

    stock_entry = {
        "symbol": symbol,
        "notes": notes.strip(),
        "added_at": datetime.now(timezone.utc).isoformat(),
        "price": price_data["price"],
        "prev_close": price_data["prev_close"],
        "yf_symbol": price_data["yf_symbol"]
    }

    col.update_one(
        {"email": email},
        {"$push": {"stocks": stock_entry}},
        upsert=True
    )

    return JSONResponse({"status": "added", "symbol": symbol, "price": price_data["price"]})


# ─── API: UPDATE STOCK NOTES ────────────────────────────────────

@router.post("/api/watchlist/update")
def update_watchlist_item(request: Request, symbol: str = Form(...), notes: str = Form("")):
    email = request.session.get("user_email")
    if not email:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    col = get_collection("watchlists")
    col.update_one(
        {"email": email, "stocks.symbol": symbol.strip().upper()},
        {"$set": {"stocks.$.notes": notes.strip()}}
    )

    return JSONResponse({"status": "updated", "symbol": symbol})


# ─── API: REMOVE STOCK ──────────────────────────────────────────

@router.post("/api/watchlist/remove")
def remove_from_watchlist(request: Request, symbol: str = Form(...)):
    email = request.session.get("user_email")
    if not email:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    col = get_collection("watchlists")
    col.update_one(
        {"email": email},
        {"$pull": {"stocks": {"symbol": symbol.strip().upper()}}}
    )

    return JSONResponse({"status": "removed", "symbol": symbol})
