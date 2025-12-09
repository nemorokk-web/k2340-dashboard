import gspread
import os
import re
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, render_template, request, redirect, url_for, session, flash

app = Flask(__name__)
app.secret_key = "NemoSecretKey_2340_FINAL"

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "credentials.json")

# 1. MAIN SHEET (Smash Forts)
MAIN_SHEET_ID = "1sdRXixziEyCb1njYMJKVlgGFesE0eYB-9_J7doEGxmI" 

# 2. NEW STATS SHEET (KvK History)
STATS_SHEET_ID = "1_MbQLNR0ZrONR1OaKeoNC1kahHfSHeu5pRtiKIuL8PU"

def connect_to_sheet(sheet_id):
    if not os.path.exists(CREDS_FILE):
        return None, f"❌ FILE ERROR: credentials.json not found"
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id)
        return sheet, None
    except Exception as e:
        return None, f"❌ CONNECTION ERROR: {str(e)}"

# --- SMART HELPERS ---

def safe_get(row, idx, default="0"):
    try: return row[idx] if len(row) > idx and row[idx] else default
    except: return default

def find_header_row(rows, target="Gov ID"):
    """Finds row index containing specific header (Case Insensitive)."""
    target = target.lower()
    for i, r in enumerate(rows):
        row_str = [str(cell).lower() for cell in r]
        # Check for partial match or exact match
        if any(target in cell for cell in row_str): return i
    return -1

def get_col_index(headers, possible_names):
    """Finds column index for a list of possible header names."""
    headers_lower = [str(h).lower().strip() for h in headers]
    for name in possible_names:
        if name.lower() in headers_lower:
            return headers_lower.index(name.lower())
    return -1

def fetch_kvk_history_data(gov_id):
    """Scans the new Statistics Sheet for KVK history."""
    history = []
    db, err = connect_to_sheet(STATS_SHEET_ID)
    if not db: return []

    gov_id = str(gov_id)
    worksheets = db.worksheets()

    for ws in worksheets:
        # Look for sheets with "KVK" and "Summary" but ignore "Combined" or "Copy"
        title = ws.title.lower()
        if "summary" in title and "kvk" in title and "combined" not in title and "copy" not in title:
            try:
                # Clean name: "2340 KVK1 Summary" -> "KVK1"
                kvk_name = ws.title.replace("2340", "").replace("Summary", "").strip()
                
                rows = ws.get_all_values()
                
                # Header row is usually row 2 or 3 in these sheets
                h_idx = find_header_row(rows, "Governor Id")
                if h_idx == -1: h_idx = find_header_row(rows, "Governor ID")
                if h_idx == -1: continue

                headers = rows[h_idx]
                
                # Map Columns
                i_id = get_col_index(headers, ["Governor Id", "Governor ID", "ID"])
                i_rank = get_col_index(headers, ["Total KvK Pts Rank", "Rank"])
                i_pts = get_col_index(headers, ["Total KvK Pts", "Total Points"])
                i_kills = get_col_index(headers, ["Kills"])
                i_deaths = get_col_index(headers, ["Deaths", "Deads"])
                i_t4 = get_col_index(headers, ["T4", "T4 Kills"])
                i_t5 = get_col_index(headers, ["T5", "T5 Kills"])

                if i_id == -1: continue

                for r in rows[h_idx+1:]:
                    if len(r) > i_id and str(r[i_id]).strip() == gov_id:
                        history.append({
                            "name": kvk_name,
                            "rank": safe_get(r, i_rank, "-"),
                            "points": safe_get(r, i_pts, "0"),
                            "kills": safe_get(r, i_kills, "0"),
                            "deaths": safe_get(r, i_deaths, "0"),
                            "t4": safe_get(r, i_t4, "0"),
                            "t5": safe_get(r, i_t5, "0")
                        })
                        break
            except: pass
            
    # Sort naturally (KVK1, KVK2, KVK3...)
    history.sort(key=lambda x: x['name'])
    return history

def fetch_all_user_data(gov_id):
    """The Brain: Scans Main Sheet + Stats Sheet."""
    data = {
        "overview": {"season": "Unknown", "req": 0, "points": 0, "forts": 0, "delta": 0, "rank": "Member"},
        "credits": {"total": 0, "redeemable": 0},
        "profile": [],
        "history": [],     # Season History (Main Sheet)
        "kvk_history": []  # KvK History (New Sheet)
    }
    
    # 1. Connect to MAIN Sheet
    db, err = connect_to_sheet(MAIN_SHEET_ID)
    if db:
        worksheets = db.worksheets()
        gov_id = str(gov_id)

        # A. CREDITS
        try:
            ws = next((w for w in worksheets if w.title == "Credits"), None)
            if ws:
                rows = ws.get_all_values()
                h_idx = find_header_row(rows, "Gov ID")
                if h_idx != -1:
                    headers = rows[h_idx]
                    id_idx = headers.index("Gov ID")
                    cred_idx = get_col_index(headers, ["Available to redeem", "Total Credits"])
                    if cred_idx != -1:
                        for r in rows[h_idx+1:]:
                            if len(r) > id_idx and str(r[id_idx]) == gov_id:
                                data["credits"]["redeemable"] = safe_get(r, cred_idx)
                                break
        except: pass

        # B. PROFILE (Linked Accounts)
        try:
            ws = next((w for w in worksheets if w.title == "Rolled UP"), None)
            if ws:
                rows = ws.get_all_values()
                h_idx = find_header_row(rows, "Gov ID")
                if h_idx != -1:
                    headers = rows[h_idx]
                    col_map = {
                        "Name": get_col_index(headers, ["Name"]),
                        "Type": get_col_index(headers, ["Gov Type"]),
                        "Power": get_col_index(headers, ["Power"]),
                        "Deads": get_col_index(headers, ["Deads"]),
                        "KP": get_col_index(headers, ["Kill Points"]),
                        "T5": get_col_index(headers, ["T5 Kills"])
                    }
                    id_idx = headers.index("Gov ID")
                    
                    buffer = []
                    found = False
                    for r in rows[h_idx+1:]:
                        if len(r) <= max(col_map.values()): continue
                        rid = str(r[id_idx])
                        rtype = str(r[col_map["Type"]]).lower()
                        
                        if rtype == "main":
                            if found: break 
                            if rid == gov_id: found = True; buffer.append(r)
                            else: buffer = [] 
                        elif rtype == "farm" and found:
                            buffer.append(r)
                    
                    if found:
                        for r in buffer:
                            data["profile"].append({
                                "name": r[col_map["Name"]],
                                "type": r[col_map["Type"]],
                                "power": r[col_map["Power"]],
                                "deads": r[col_map["Deads"]],
                                "kp": r[col_map["KP"]],
                                "t5": r[col_map["T5"]]
                            })
        except: pass

        # C. SEASON HISTORY & OVERVIEW
        season_sheets = []
        for ws in worksheets:
            if "Season" in ws.title and "Rolled UP" in ws.title:
                match = re.search(r"Season\s*(\d+)", ws.title)
                num = int(match.group(1)) if match else 0
                season_sheets.append((num, ws))
        season_sheets.sort(key=lambda x: x[0], reverse=True)

        if season_sheets:
            latest_s_num, latest_ws = season_sheets[0]
            data["overview"]["season"] = f"Season {latest_s_num}"

            for s_num, ws in season_sheets:
                try:
                    rows = ws.get_all_values()
                    h_idx = find_header_row(rows, "Gov ID")
                    if h_idx == -1: continue
                    headers = rows[h_idx]
                    
                    i_id = headers.index("Gov ID")
                    i_pts = get_col_index(headers, ["Points Earned"])
                    i_join = get_col_index(headers, ["Joined"])
                    i_req = get_col_index(headers, ["Point Requirement"])
                    i_delta = get_col_index(headers, ["Delta"])

                    in_group = False
                    for r in rows[h_idx+1:]:
                        if len(r) < 5: continue
                        rid = str(r[i_id])
                        if rid == gov_id: in_group = True
                        if rid == "TOTAL" and in_group:
                            # Calculate Status
                            delta_val = safe_get(r, i_delta).replace(",","").replace("-","")
                            status = "✅ Met"
                            try:
                                if float(safe_get(r, i_delta).replace(",","")) < 0: status = "❌ Missed"
                            except: pass

                            row_data = {
                                "season": f"S{s_num}",
                                "points": safe_get(r, i_pts),
                                "forts": safe_get(r, i_join),
                                "req": safe_get(r, i_req),
                                "delta": safe_get(r, i_delta),
                                "status": status
                            }
                            data["history"].append(row_data)
                            
                            # If Latest Season, Fill Overview
                            if s_num == latest_s_num:
                                data["overview"]["points"] = row_data["points"]
                                data["overview"]["forts"] = row_data["forts"]
                                data["overview"]["req"] = row_data["req"]
                                data["overview"]["delta"] = row_data["delta"]
                            
                            in_group = False
                            break
                except: pass

    # 2. FETCH KVK HISTORY (From New Sheet)
    data["kvk_history"] = fetch_kvk_history_data(gov_id)

    return data

# --- ROUTES ---

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        gov_id = request.form.get("gov_id", "").strip()
        password = request.form.get("password", "").strip()
        
        db, msg = connect_to_sheet(MAIN_SHEET_ID)
        if not db:
            flash(msg); return render_template("login.html")

        try:
            users = db.worksheet("WebUsers").get_all_records()
            user = next((r for r in users if str(r.get("GovID")) == gov_id), None)
            if user and str(user.get("Password")).strip() == password:
                session["user"] = gov_id
                session["name"] = "Member"
                return redirect(url_for("dashboard"))
            else:
                flash("❌ Invalid ID or Password")
        except: flash("❌ Login Error")

    return render_template("login.html")

@app.route("/dashboard")
def dashboard():
    if "user" not in session: return redirect(url_for("login"))
    data = fetch_all_user_data(session["user"])
    return render_template("dashboard.html", page="overview", data=data, user=session["user"])

@app.route("/stats")
def stats_page():
    if "user" not in session: return redirect(url_for("login"))
    data = fetch_all_user_data(session["user"])
    return render_template("dashboard.html", page="stats", data=data, user=session["user"])

@app.route("/credits")
def credits_page():
    if "user" not in session: return redirect(url_for("login"))
    data = fetch_all_user_data(session["user"])
    return render_template("dashboard.html", page="credits", data=data, user=session["user"])

@app.route("/history")
def history_page():
    if "user" not in session: return redirect(url_for("login"))
    data = fetch_all_user_data(session["user"])
    return render_template("dashboard.html", page="history", data=data, user=session["user"])

@app.route("/kvk_history")
def kvk_history_page():
    if "user" not in session: return redirect(url_for("login"))
    data = fetch_all_user_data(session["user"])
    return render_template("dashboard.html", page="kvk_history", data=data, user=session["user"])

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)