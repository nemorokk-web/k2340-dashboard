import json
import threading
import requests
import asyncio
import uuid
import io
import re
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import discord

@nightyScript(
    name="K2340 Master Suite",
    author="Nemo",
    description="V3.0: Credentials sent to DM.",
    usage="Dashboard for controls, Settings for config."
)
def K2340MasterSuite():
    
    BASE_DIR = Path(getScriptsPath()) / "json"
    CONFIG_FILE = BASE_DIR / "k2340_master_config.json"
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    DEFAULT_CONFIG = {
        "api_url": "",
        "api_key": "",
        "owner_id": "",
        "sync_interval": "60",
        "fort_interval": "60",
        "channel_id": "", 
        "target_bot_id": "1190709916034416640",
        "admins": [],
        "seasons": []
    }

    state = {
        "sync_running": False,
        "fort_running": False,
        "sync_task": None,
        "fort_task": None,
        "last_sync": "Never",
        "last_status": "Idle"
    }

    def _load():
        try:
            if not CONFIG_FILE.exists(): return DEFAULT_CONFIG.copy()
            with open(CONFIG_FILE, "r") as f: return json.load(f)
        except: return DEFAULT_CONFIG.copy()

    def _save(data):
        try:
            with open(CONFIG_FILE, "w") as f: json.dump(data, f, indent=4)
        except: pass

    def log_ui(msg):
        state["last_status"] = msg
        txt_log.content = f"[{datetime.now().strftime('%H:%M')}] {msg}"
        tab_dash.render()

    def call_api_sync():
        cfg = _load()
        url = cfg.get("api_url")
        key = cfg.get("api_key")
        if not url or not key: return False, "⚠️ Missing API URL or Key"
        try:
            r = requests.post(f"{url}?action=run_sync&key={key}", timeout=20)
            if r.status_code != 200: return False, f"HTTP {r.status_code}"
            d = r.json()
            return d.get("success", False), d.get("message", "No msg")
        except Exception as e: return False, str(e)

    def call_api_get(action, req_id, target_id=None, season=None, **kwargs):
        cfg = _load()
        url = cfg.get("api_url")
        if not url: return {"success": False, "message": "⚠️ No API URL"}
        u = f"{url}?action={action}&requesterId={req_id}"
        if target_id: u += f"&targetId={target_id}"
        if season: u += f"&season={season}"
        for k, v in kwargs.items(): u += f"&{k}={v}"
        try:
            r = requests.get(u, timeout=10)
            return r.json() if r.status_code == 200 else {"success": False, "message": f"HTTP {r.status_code}"}
        except Exception as e: return {"success": False, "message": str(e)}

    async def loop_sync():
        log_ui("Sync Loop Started")
        while state["sync_running"]:
            cfg = _load()
            inv = int(cfg.get("sync_interval", 60))
            if inv < 1: inv = 60
            
            s, m = await bot.loop.run_in_executor(None, call_api_sync)
            state["last_sync"] = datetime.now().strftime("%H:%M UTC")
            log_ui(f"{'✅' if s else '❌'} Sync: {m[:40]}...")
            
            for _ in range(inv * 60):
                if not state["sync_running"]: break
                await asyncio.sleep(1)
        log_ui("Sync Loop Stopped")

    async def loop_fort():
        log_ui("Fort Loop Started")
        while state["fort_running"]:
            try:
                data = _load()
                active = None
                today = datetime.utcnow()
                for s in data.get("seasons", []):
                    try:
                        s_dt = datetime.strptime(s["start"], "%d/%m/%Y")
                        e_dt = datetime.strptime(s["end"], "%d/%m/%Y").replace(hour=23, minute=59)
                        if s_dt <= today <= e_dt:
                            active = s
                            break
                    except: pass

                if active:
                    channel = bot.get_channel(int(data.get("channel_id", 0)))
                    if channel:
                        start_fmt = f"{active['start']} 00:00"
                        end_fmt = f"{active['end']} 23:59"
                        
                        cmds = []
                        if hasattr(channel, "slash_commands"):
                            res = channel.slash_commands()
                            if asyncio.iscoroutine(res): res = await res
                            if hasattr(res, "__aiter__"): 
                                async for c in res: cmds.append(c)
                            else: cmds = list(res) if res else []
                        
                        target_cmd = None
                        for c in cmds:
                            if c.name == "fort" and str(c.application_id) == str(data.get("target_bot_id")):
                                target_cmd = c
                                break
                        
                        if target_cmd:
                            dl = None
                            if hasattr(target_cmd, "children") and target_cmd.children:
                                for child in target_cmd.children:
                                    if child.name == "download": dl = child; break
                            try:
                                if dl: await dl(channel, summarize=False, start_date=start_fmt, end_date=end_fmt)
                                else: await target_cmd(channel, summarize=False, start_date=start_fmt, end_date=end_fmt)
                                log_ui("Slash Command Sent")
                            except Exception as ex: log_ui(f"Slash Err: {ex}")
                        else: log_ui("Cmd /fort not found")
                
                inv = int(data.get("fort_interval", 60))
                for _ in range(inv * 60):
                    if not state["fort_running"]: break
                    await asyncio.sleep(1)
            except Exception as e:
                log_ui(f"Fort Loop Err: {e}")
                await asyncio.sleep(60)

    async def repost_rally(content):
        data = _load()
        ch = bot.get_channel(int(data.get("channel_id", 0)))
        if not ch: return
        try:
            fb = io.BytesIO(content.encode("utf-8"))
            fobj = discord.File(fb, filename="rally_data.csv")
            now_utc = datetime.utcnow()
            now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            
            interval_min = int(data.get("fort_interval", 60))
            next_utc = now_utc + timedelta(minutes=interval_min)
            next_str = next_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            
            s_name, s_start, s_end, days_txt = "N/A", "N/A", "N/A", "N/A"
            for s in data.get("seasons", []):
                try:
                    s_dt = datetime.strptime(s["start"], "%d/%m/%Y")
                    e_dt = datetime.strptime(s["end"], "%d/%m/%Y").replace(hour=23, minute=59)
                    if s_dt <= now_utc <= e_dt:
                        s_name = s["name"]
                        s_start = s["start"]
                        s_end = s["end"]
                        delta = e_dt - now_utc
                        d_left = max(delta.days, 0)
                        days_txt = "<1" if d_left == 0 and delta.total_seconds() > 0 else str(d_left)
                        break
                except: continue
            
            msg = (
                ":open_file_folder: Rally Data Captured\n"
                f"date taken: {now_str}\n"
                f"next snapshot: {interval_min} min later ({next_str})\n"
                f"season: {s_name} ({s_start} → {s_end})\n"
                f"days left: {days_txt}"
            )
            await ch.send(content=msg, file=fobj)
        except: pass

    tab_dash = Tab(name="Dashboard", title="K2340 Dashboard", icon="layout", gap=8)
    tab_sett = Tab(name="Settings", title="Configuration", icon="settings", gap=8)

    c_stat = tab_dash.create_container(type="rows", gap=8).create_card(gap=4)
    c_stat.create_ui_element(UI.Text, content="🤖 System Status", variant="header")
    
    t_sync = c_stat.create_ui_element(UI.Text, content="Sheets Sync: 🔴 OFFLINE", variant="paragraph")
    t_fort = c_stat.create_ui_element(UI.Text, content="Fort Loop: 🔴 OFFLINE", variant="paragraph")
    txt_log = c_stat.create_ui_element(UI.Text, content="Waiting...", variant="paragraph")

    def toggle_sync():
        if state["sync_running"]:
            state["sync_running"] = False
            if state["sync_task"]: state["sync_task"].cancel()
            t_sync.content = "Sheets Sync: 🔴 OFFLINE"
        else:
            state["sync_running"] = True
            state["sync_task"] = bot.loop.create_task(loop_sync())
            t_sync.content = "Sheets Sync: 🟢 ONLINE"
        tab_dash.render()

    def toggle_fort():
        if state["fort_running"]:
            state["fort_running"] = False
            if state["fort_task"]: state["fort_task"].cancel()
            t_fort.content = "Fort Loop: 🔴 OFFLINE"
        else:
            state["fort_running"] = True
            state["fort_task"] = bot.loop.create_task(loop_fort())
            t_fort.content = "Fort Loop: 🟢 ONLINE"
        tab_dash.render()

    c_stat.create_ui_element(UI.Button, label="Toggle Sync Loop", color="primary", onClick=toggle_sync)
    c_stat.create_ui_element(UI.Button, label="Toggle Fort Loop", color="default", onClick=toggle_fort)

    c_seas = tab_dash.create_container(type="rows", gap=8).create_card(gap=4)
    c_seas.create_ui_element(UI.Text, content="📅 Seasons Overview", variant="header")
    t_seas = c_seas.create_ui_element(
        UI.Table,
        columns=[
            {"label": "Name", "type": "text"},
            {"label": "Start", "type": "text"},
            {"label": "End", "type": "text"},
            {"label": "Status", "type": "text"}
        ],
        rows=[]
    )

    def refresh_dash_seasons():
        d = _load()
        rows = []
        now = datetime.utcnow()
        for s in d.get("seasons", []):
            st = "⏳ Upcoming"
            try:
                if datetime.strptime(s["start"], "%d/%m/%Y") <= now <= datetime.strptime(s["end"], "%d/%m/%Y").replace(hour=23, minute=59): st = "✅ ACTIVE"
                elif now > datetime.strptime(s["end"], "%d/%m/%Y"): st = "❌ Ended"
            except: st = "⚠ Err"
            rows.append({"id": str(uuid.uuid4()), "cells": [{"text": s["name"]}, {"text": s["start"]}, {"text": s["end"]}, {"text": st}]})
        t_seas.rows = rows

    sett_top = tab_sett.create_container(type="columns", gap=8)
    
    c_api = sett_top.create_card(gap=4)
    c_api.create_ui_element(UI.Text, content="🔗 Main Config", variant="header")
    cfg = _load()
    i_url = c_api.create_ui_element(UI.Input, label="Google Script URL", value=cfg.get("api_url", ""))
    i_key = c_api.create_ui_element(UI.Input, label="API Key", value=cfg.get("api_key", ""))
    i_chn = c_api.create_ui_element(UI.Input, label="Rally Channel ID", value=str(cfg.get("channel_id", "")))
    i_bot = c_api.create_ui_element(UI.Input, label="Target Bot ID", value=str(cfg.get("target_bot_id", "")))
    i_own = c_api.create_ui_element(UI.Input, label="Owner ID", value=str(cfg.get("owner_id", "")))
    i_s_int = c_api.create_ui_element(UI.Input, label="Sync Interval (min)", value=str(cfg.get("sync_interval", "60")))
    i_f_int = c_api.create_ui_element(UI.Input, label="Fort Interval (min)", value=str(cfg.get("fort_interval", "60")))

    def save_all():
        d = _load()
        d["api_url"] = i_url.value.strip()
        d["api_key"] = i_key.value.strip()
        d["channel_id"] = i_chn.value.strip()
        d["target_bot_id"] = i_bot.value.strip()
        d["owner_id"] = i_own.value.strip()
        d["sync_interval"] = i_s_int.value.strip()
        d["fort_interval"] = i_f_int.value.strip()
        _save(d)
        log_ui("Settings Saved")
        tab_sett.render()
    c_api.create_ui_element(UI.Button, label="💾 Save Config", color="success", onClick=save_all)

    c_s_mgr = sett_top.create_card(gap=4)
    c_s_mgr.create_ui_element(UI.Text, content="📅 Season Manager", variant="header")
    i_s_name = c_s_mgr.create_ui_element(UI.Input, label="Name", placeholder="S7", value="")
    i_s_start = c_s_mgr.create_ui_element(UI.Input, label="Start (DD/MM/YYYY)", value="")
    i_s_end = c_s_mgr.create_ui_element(UI.Input, label="End (DD/MM/YYYY)", value="")

    def add_seas():
        if i_s_name.value and i_s_start.value and i_s_end.value:
            d = _load()
            d["seasons"].append({"id":str(uuid.uuid4()), "name": i_s_name.value, "start": i_s_start.value, "end": i_s_end.value})
            _save(d)
            i_s_name.value = ""
            refresh_dash_seasons()
            tab_dash.render()
            tab_sett.render()

    def rem_seas():
        if i_s_name.value:
            d = _load()
            d["seasons"] = [s for s in d["seasons"] if s["name"].lower() != i_s_name.value.lower()]
            _save(d)
            refresh_dash_seasons()
            tab_dash.render()
            tab_sett.render()

    c_s_mgr.create_ui_element(UI.Button, label="➕ Add Season", color="primary", onClick=add_seas)
    c_s_mgr.create_ui_element(UI.Button, label="➖ Remove (Name)", color="danger", onClick=rem_seas)

    sett_bot = tab_sett.create_container(type="rows", gap=8)
    c_adm = sett_bot.create_card(gap=4)
    c_adm.create_ui_element(UI.Text, content="🛡️ Admin Manager", variant="header")
    i_adm = c_adm.create_ui_element(UI.Input, label="Discord ID", value="")
    t_adm_list = c_adm.create_ui_element(UI.Text, content="Loading...", variant="paragraph")

    def ref_adm():
        d = _load()
        l = d.get("admins", [])
        t_adm_list.content = ", ".join(l) if l else "None"
    
    def add_adm():
        if i_adm.value:
            d = _load()
            if i_adm.value not in d["admins"]: d["admins"].append(i_adm.value)
            _save(d)
            ref_adm()
            i_adm.value = ""
            tab_sett.render()

    def rem_adm():
        if i_adm.value:
            d = _load()
            if i_adm.value in d["admins"]: d["admins"].remove(i_adm.value)
            _save(d)
            ref_adm()
            i_adm.value = ""
            tab_sett.render()

    c_adm.create_ui_element(UI.Button, label="➕ Add Admin", color="primary", onClick=add_adm)
    c_adm.create_ui_element(UI.Button, label="➖ Remove Admin", color="danger", onClick=rem_adm)

    refresh_dash_seasons()
    ref_adm()
    tab_dash.render()
    tab_sett.render()

    @bot.listen("on_message")
    async def master_listener(message):
        if str(message.author.id) == str(_load().get("target_bot_id")):
            RALLY_HEAD = "id,alliance_tag,governor_id,governor_name,governor_location_x"
            is_csv = False
            content = ""
            
            if message.attachments:
                for att in message.attachments:
                    if att.filename.endswith(".csv"):
                        try:
                            b = io.BytesIO()
                            await att.save(b)
                            b.seek(0)
                            content = b.read().decode("utf-8", errors="ignore")
                            if RALLY_HEAD in content: is_csv = True
                        except: pass
            
            if not is_csv and message.embeds:
                full = message.content or ""
                for e in message.embeds:
                    full += f"\n{e.description or ''}"
                    for f in e.fields: full += f"\n{f.value}"
                if RALLY_HEAD in full:
                    content = full.replace("``````", "").strip()
                    is_csv = True

            if is_csv and state["fort_running"]:
                await repost_rally(content)
                return

        msg = (message.content or "").strip()
        lower = msg.lower()
        parts = msg.split()
        if not parts: return

        cfg = _load()
        uid = str(message.author.id)
        is_allowed = uid == str(bot.user.id) or uid in cfg.get("admins", [])

        if lower == "!help":
            help_msg = (
                "**🛡️ K2340 BOT COMMANDS**\n"
                "```\n"
                "[ 🟢 PUBLIC COMMANDS ]\n"
                "!profile <Name|ID>           :: View Season Profile (Main+Farms)\n"
                "!forts                       :: Check your stats\n"
                "!forts <ID>                  :: Check another player's stats\n"
                "!forts S<Num> [ID]           :: Check specific Season (e.g. !forts S6)\n"
                "!password                    :: 🔑 Get your Web Credentials\n"
                "!shop                        :: View Shop Items\n"
                "!order <Item> [Qty]          :: Buy items (e.g. !order 3h x10)\n"
                "!credits                     :: Check your wallet\n\n"
                "[ 🔴 ADMIN COMMANDS ]\n"
                "!force sync                  :: 🔄 Force Google Sheet Sync\n"
                "!start sync                  :: ▶ Start Sync Loop\n"
                "!stop sync                   :: ⏹ Stop Sync Loop\n"
                "!start forts                 :: ▶ Start Rally Capture\n"
                "!stop forts                  :: ⏹ Stop Rally Capture\n"
                "!transfer <ID> <ID>          :: 💸 Transfer Credits\n"
                "!deduct <ID> <Amt>           :: 📉 Deduct Credits\n"
                "!add credits <Amt>           :: 📈 Add Credits\n"
                "!add admin <ID>              :: 🛡️ Add New Admin\n"
                "!#<ID> done                  :: ✅ Approve Order (R4)\n"
                "```"
            )
            await message.channel.send(help_msg)
            return

        if lower == "!password":
            w = await message.channel.send("⏳ **Fetching Credentials...**")
            d = await bot.loop.run_in_executor(None, lambda: call_api_get("get_password", uid, did=uid))
            
            if d.get("success"):
                p_val = d.get("password", "Unknown")
                p_id = d.get("id", "Unknown")
                try:
                    await message.author.send(f"🆔 **ID:** {p_id}\n🔑 **Password:** ||{p_val}||")
                    await w.edit(content="✅ **Credentials sent to your DM!**")
                except discord.Forbidden:
                    await w.edit(content="❌ **Could not DM you.** Please open your DMs and try again.")
                except Exception as e:
                    await w.edit(content=f"❌ **Error sending DM:** {e}")
            else:
                await w.edit(content=f"❌ {d.get('message')}")
            return

        if re.match(r"^!p(rofile)?(\s+|$)", lower):
            w = await message.channel.send("⏳ **Searching Profile...**")
            query = lower.replace("!profile", "").replace("!p", "").strip()
            if not query: query = uid
            
            d = await bot.loop.run_in_executor(None, lambda: call_api_get("profile", uid, query))

            if not d.get("success"):
                await w.edit(content=f"❌ {d.get('message', 'Not Found')}")
                return

            try:
                main_name = d.get('mainName', 'Unknown')
                farms = d.get('farms', [])
                farms_count = len(farms)
                
                farm_lines = []
                for i in range(0, len(farms), 2):
                    f1 = farms[i]
                    f2 = farms[i+1] if i+1 < len(farms) else ""
                    if f2:
                        farm_lines.append(f"{f1:<15} |   {f2}") 
                    else:
                        farm_lines.append(f"{f1}")
                
                farms_str = "\n".join(farm_lines) if farm_lines else "None"
                
                total_l = "{:,}".format(int(d.get('totalLaunched', 0)))
                total_j = "{:,}".format(int(d.get('totalJoined', 0)))
                total_p = "{:,}".format(int(d.get('totalPoints', 0)))
                seasons = d.get('seasons', 'None')
                
                box = (
                    f"<:1325094176865386526:1445177230975307869> **{main_name}**\n"
                    f"```\n"
                    f"🟢 Farms:   {farms_count}\n"
                    f"{farms_str}\n"
                    f"🚀 Total  Launched: {total_l} | Total  Joined: {total_j}\n"
                    f"👑 Total Points {total_p}\n"
                    f"📅 Seasons Played {seasons}\n"
                    f"```"
                )
                await w.edit(content=box)
            except Exception as e:
                await w.edit(content=f"❌ **Data Error:** {e}")
            return

        if re.match(r"^!\s*forts?", lower):
            args = lower.split()[1:] 
            action = "self"
            tid = None
            season_req = None

            if len(args) >= 3 and args[0] == "admin" and args[1] == "add":
                action, tid = "add_admin", args[2]
            else:
                for arg in args:
                    if re.match(r"^s\d+$", arg): 
                        s_num = int(arg.replace("s", ""))
                        if s_num < 6:
                            await message.channel.send("⚠️ **Archive Data:** Seasons before S6 are not Rolled UP.\n👉 Please use `!profile` to view historic data.")
                            return
                        season_req = str(s_num)
                    elif arg.isdigit():
                        action = "lookup"
                        tid = arg

            w = await message.channel.send("⏳ Fetching...")
            d = await bot.loop.run_in_executor(None, lambda: call_api_get(action, uid, tid, season_req))
            
            if not d.get("success"):
                await w.edit(content=f"❌ {d.get('message')}")
                return

            if "Admin added" in d.get("message", ""):
                await w.edit(content=d['message'])
                return

            try:
                e, r, dl = d.get('earned', 0), d.get('req', 1), d.get('delta', 0)
                try: prog = int((float(e) / float(r)) * 100) if float(r) > 0 else 0
                except: prog = 0
                
                ds = "+" if (float(dl) if dl else 0) > 0 else ""
                
                now = datetime.utcnow()
                sun = now.replace(hour=23, minute=59, second=0) + timedelta(days=(6 - now.weekday()))
                if sun < now: sun += timedelta(days=7)
                diff = sun - now
                
                footer_txt = f"⏳ Time Left:  {diff.days} Days, {diff.seconds // 3600} Hours"
                if season_req: footer_txt = "🔒 Historic Season View"

                body = (
                    f"<:1325094176865386526:1445177230975307869> **{d.get('name')}**\n```\n"
                    f"👑 Points:     {'{:,}'.format(int(e))} (Delta: {ds}{'{:,}'.format(int(dl))})\n"
                    f"🟢 Accounts:   {d.get('accountCount')}\n"
                    f"🎯 Req:        {'{:,}'.format(int(r))}\n"
                    f"🚀 Launched:   {'{:,}'.format(int(d.get('launched')))} | Joined: {'{:,}'.format(int(d.get('joined')))}\n"
                    f"📊 Progress:   {prog}%\n"
                    f"-----------------------------------\n"
                    f"📅 Season {d.get('season', 'X')}:   Ends Sunday 23:59 UTC\n"
                    f"{footer_txt}\n```"
                )
                await w.edit(content=body)
            except Exception as ex:
                await w.edit(content=f"❌ Parse Error: {ex}")
            return

        if not is_allowed: return

        if lower == "!force sync" or lower == "!run":
            w = await message.channel.send("⏳ **Triggering Manual Sheet Sync...**")
            s, m = await bot.loop.run_in_executor(None, call_api_sync)
            if s: await w.edit(content=f"✅ **Sync Successful!**\n```\n{m}\n```")
            else: await w.edit(content=f"❌ **Sync Failed!**\nError: {m}")

        elif lower.startswith("!start "):
            target = lower.split(" ")[1]
            if target == "forts" or target == "fort":
                if not state["fort_running"]:
                    state["fort_running"] = True
                    state["fort_task"] = bot.loop.create_task(loop_fort())
                    await message.channel.send("✅ **Fort Loop Started**")
                    log_ui("Fort Loop Started by Command")
                else: await message.channel.send("⚠️ Fort Loop already running.")
            elif target == "sync":
                if not state["sync_running"]:
                    state["sync_running"] = True
                    state["sync_task"] = bot.loop.create_task(loop_sync())
                    await message.channel.send("✅ **Sync Loop Started**")
                    log_ui("Sync Loop Started by Command")
                else: await message.channel.send("⚠️ Sync Loop already running.")

        elif lower.startswith("!stop "):
            target = lower.split(" ")[1]
            if target == "forts" or target == "fort":
                if state["fort_running"]:
                    state["fort_running"] = False
                    if state["fort_task"]: state["fort_task"].cancel()
                    await message.channel.send("🛑 **Fort Loop Stopped**")
                    log_ui("Fort Loop Stopped by Command")
                else: await message.channel.send("⚠️ Fort Loop is not running.")
            elif target == "sync":
                if state["sync_running"]:
                    state["sync_running"] = False
                    if state["sync_task"]: state["sync_task"].cancel()
                    await message.channel.send("🛑 **Sync Loop Stopped**")
                    log_ui("Sync Loop Stopped by Command")
                else: await message.channel.send("⚠️ Sync Loop is not running.")

    @bot.listen("on_message_edit")
    async def master_edit_listener(before, after):
        if str(after.author.id) == str(_load().get("target_bot_id")):
            RALLY_HEAD = "id,alliance_tag,governor_id,governor_name,governor_location_x"
            is_csv = False
            content = ""
            
            if after.attachments:
                for att in after.attachments:
                    if att.filename.endswith(".csv"):
                        try:
                            b = io.BytesIO()
                            await att.save(b)
                            b.seek(0)
                            content = b.read().decode("utf-8", errors="ignore")
                            if RALLY_HEAD in content: is_csv = True
                        except: pass
            
            if not is_csv and after.embeds:
                full = after.content or ""
                for e in after.embeds:
                    full += f"\n{e.description or ''}"
                    for f in e.fields: full += f"\n{f.value}"
                if RALLY_HEAD in full:
                    content = full.replace("``````", "").strip()
                    is_csv = True

            if is_csv and state["fort_running"]:
                await repost_rally(content)

    log_ui(state["last_status"])

K2340MasterSuite()
