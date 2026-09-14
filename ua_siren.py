from pathlib import Path
import requests

def check():
    raw = Path("/home/ubuntu/dnipro-air-bot/.env").read_text()
    token = raw.split("UKRAINEALARM_TOKEN=",1)[1].splitlines()[0].strip()
    r = requests.get("https://api.ukrainealarm.com/api/v3/alerts", headers={"Authorization": token}, timeout=30)
    r.raise_for_status()
    items = r.json()
    blob = " ".join(str(x.get("regionName") or "").lower() for x in items)
    return blob

def flags():
    blob = check()
    city = ("м. дніпро" in blob) or ("дніпровська територіальна" in blob) or ("дніпровська міська" in blob) or ("дніпровський район" in blob) or ("дніпропетровська область" in blob)
    tsar = city or ("царичан" in blob) or ("смт царич" in blob) or ("селище царич" in blob) or ("царичанська" in blob) or ("дніпровський район" in blob) or ("дніпропетровська область" in blob)
    return city, tsar

def pick_locals(texts):
    kept = []
    for t in texts:
        t = (t or "").strip()
        if not t: continue
        nt = "".join(ch.lower() if ch.isalnum() else " " for ch in t)
        words = set(nt.split())
        found = False
        i = 0
        while i < len(kept):
            k = kept[i]
            nk = "".join(ch.lower() if ch.isalnum() else " " for ch in k)
            wk = set(nk.split())
            same = nt in nk or nk in nt
            if words and wk:
                same = same or (len(words & wk) / min(len(words), len(wk)) >= 0.6)
            if same:
                if len(t) > len(k): kept[i] = t
                found = True
                break
            i += 1
        if not found:
            kept.append(t)
    return kept
