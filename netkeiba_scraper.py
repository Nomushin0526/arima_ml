"""
netkeiba スクレイパー v2（個人の分析用・データは各自で取得し共有しない）

コマンド
  python netkeiba_scraper.py results 2021 2026   # 芝1800m以上のレース結果を取得（既定）
  python netkeiba_scraper.py --min-dist 2000 results 2021 2026   # 芝2000m以上に絞る
  python netkeiba_scraper.py --surface all --min-dist 0 results 2021 2026   # 全レース
  python netkeiba_scraper.py --sleep 5 results 2021 2026   # 間隔をさらに長くする場合
  python netkeiba_scraper.py --max-pages 2000 results 2021 2026   # 1回の通信上限（既定3000）
  python netkeiba_scraper.py ped                 # 取得済み結果に出てくる馬の血統を取得
  python netkeiba_scraper.py ped --since 2018    # 2018年以降に走った馬だけ
  python netkeiba_scraper.py race 202306050811   # 1レースだけ（動作確認用）
  python netkeiba_scraper.py shutuba 202606050811  # これから走るレースの出馬表

出力（data/ 以下。pyarrow があれば .parquet、なければ .csv）
  data/{年}/race_info   1行=1レース  レース条件・ラップ・ペース・コーナー通過順
  data/{年}/result      1行=1頭      着順・枠・馬番・タイム・通過・上がり・確定単勝オッズ・人気・馬体重 など
  data/{年}/payout      1行=1組番    券種・組番・払戻・人気
  data/pedigree         1行=1頭      父・母・母父（とそのID）
  data/failed.txt       取得に失敗したURL

取得元
  - 結果はレース側の結果ページ（race.netkeiba.com/race/result.html）から取る。
    非会員でも 後3F（上がり）・コーナー通過順・ラップ・払戻が1ページにそろうため
  - そのページが取れないレースだけ、DB側（db.netkeiba.com/race/）で補う（通過・上がりは欠ける）
  - race_info の「取得元」列で、どちらから取ったかわかる

設計
  - 1リクエストごとに SLEEP_SEC 秒＋0〜JITTER_SEC 秒のランダムな時間待つ（平均約4秒）
    短くしないこと（規約第16条：設備への支障）
  - 取得したHTMLは cache/ に保存し、2回目以降はネットに出ない（中断しても再実行で続きから）
  - 未確定のレースや未来の日付のページはキャッシュしない
  - 2008〜2026年の結果で約6.5万ページ ≒ 約72時間（平均4秒）。夜間に1週間ほどかけて回す想定
  - 2008〜2010年は主に「過去成績の特徴量」の材料。学習データ自体は2011年以降から使う

必要なもの
  pip install requests beautifulsoup4 lxml pandas pyarrow
"""

import argparse
import hashlib
import random
import re
import sys
import time
import unicodedata
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

SLEEP_SEC = 3.0    # 最低待ち時間（秒）
JITTER_SEC = 2.0   # これに 0〜JITTER_SEC 秒をランダムに足す（機械的な一定間隔を避ける）
CACHE_DIR = Path("cache")
DATA_DIR = Path("data")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}
PLACE = {"01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
         "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉"}

session = requests.Session()
session.headers.update(HEADERS)
_last_request = 0.0
STATS = {"requests": 0, "cache_hits": 0}  # 実際にネットに出た回数 / キャッシュで済んだ回数
MAX_PAGES = 3000   # 1回の実行で通信してよい上限（キャッシュ読みは数えない）。--max-pages で変更
BLOCK_CODES = {400, 403, 429, 503}  # アクセス制限の可能性がある応答。来たら即停止


class StopRun(BaseException):
    """実行を安全に止めるための例外（上限到達・アクセス制限の疑い）。
    レース単位の `except Exception` で握りつぶされないよう BaseException を継承する"""


# ============================================================ 通信・キャッシュ
def _decode(content: bytes) -> str:
    head = content[:3000].decode("ascii", errors="ignore").lower()
    m = re.search(r'charset=["\']?([\w-]+)', head)
    enc = m.group(1) if m else "euc-jp"
    try:
        return content.decode(enc, errors="replace")
    except LookupError:
        return content.decode("euc-jp", errors="replace")


def fetch(url: str, valid=None) -> str | None:
    """HTMLを取得する。valid(html)がTrueのときだけキャッシュに保存する。"""
    global _last_request
    CACHE_DIR.mkdir(exist_ok=True)
    path = CACHE_DIR / (hashlib.md5(url.encode()).hexdigest() + ".html")
    if path.exists():
        STATS["cache_hits"] += 1
        return path.read_text(encoding="utf-8")

    if MAX_PAGES and STATS["requests"] >= MAX_PAGES:
        raise StopRun(f"今回の上限（{MAX_PAGES}ページ）に達しました。"
                      "次回、同じコマンドを実行すれば続きから進みます。")

    for attempt in range(3):
        wait = SLEEP_SEC + random.uniform(0, JITTER_SEC) - (time.time() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.time()
        STATS["requests"] += 1
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 404:
                return None
            if r.status_code in BLOCK_CODES:
                raise StopRun(f"HTTP {r.status_code} が返ってきました（{url}）。"
                              "アクセス制限の可能性があるため停止します。"
                              "最低でも1日あけ、ブラウザで普通にnetkeibaが見られることを確認してから再開してください。")
            r.raise_for_status()
            html = _decode(r.content)
            if valid is None or valid(html):
                path.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException as e:
            print(f"    通信エラー({attempt + 1}/3): {e}")
            time.sleep(SLEEP_SEC * 5 * (attempt + 1))  # 失敗時は長めに待つ

    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / "failed.txt", "a", encoding="utf-8") as f:
        f.write(url + "\n")
    return None


def save_table(df: pd.DataFrame, path_wo_ext: Path):
    path_wo_ext.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path_wo_ext.with_suffix(".parquet"), index=False)
    except (ImportError, ValueError):
        df.to_csv(path_wo_ext.with_suffix(".csv"), index=False, encoding="utf-8-sig")


def load_table(path_wo_ext: Path) -> pd.DataFrame | None:
    p, c = path_wo_ext.with_suffix(".parquet"), path_wo_ext.with_suffix(".csv")
    if p.exists():
        return pd.read_parquet(p)
    if c.exists():
        return pd.read_csv(c, dtype=str)
    return None


def _id_from_href(a, kind: str):
    if a is None or not a.get("href"):
        return None
    m = re.search(rf"/{kind}/(?:result/recent/)?(\w+)", a["href"])
    return m.group(1) if m else None


# ============================================================ 開催日・レースID
def get_kaisai_dates(year: int, month: int) -> list[str]:
    """その月のJRA開催日（YYYYMMDD）"""
    url = f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}"
    is_past = date(year, month, 1) < date.today().replace(day=1)
    html = fetch(url, valid=lambda h: is_past) or ""
    return sorted(set(re.findall(r"kaisai_date=(\d{8})", html)))


def _race_list_courses(html: str) -> dict[str, tuple[str, int]]:
    """レース一覧のHTMLから {race_id: (芝/ダ/障, 距離)} を読む。読めないレースは含めない"""
    out = {}
    pat = re.compile(r"(?:race_id=|/race/)(\d{12})")
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=pat):
        rid = pat.search(a["href"]).group(1)
        if rid in out:
            continue
        # リンクの中、なければ一番近い li / tr / dd の中から「芝2000m」などを探す
        for node in [a] + [a.find_parent(t) for t in ("li", "tr", "dd")]:
            if node is None:
                continue
            m = re.search(r"(芝|ダ|障)\D{0,3}?(\d{3,4})\s*m", _nfkc(node.get_text(" ")))
            if m:
                out[rid] = (m.group(1), int(m.group(2)))
                break
    return out


def get_race_list(kaisai_date: str) -> dict[str, tuple[str, int] | None]:
    """開催日のJRAレース {race_id: (芝/ダ/障, 距離) or None}（場コード01〜10のみ）"""
    is_past = kaisai_date < date.today().strftime("%Y%m%d")
    pat = re.compile(r"(?:race_id=|/race/)(\d{12})")

    url = f"https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={kaisai_date}"
    html = fetch(url, valid=lambda h: is_past and bool(pat.search(h))) or ""
    ids = set(pat.findall(html))
    if not ids:  # 古い日付などで取れないときは db 側の一覧を使う
        url = f"https://db.netkeiba.com/race/list/{kaisai_date}/"
        html = fetch(url, valid=lambda h: is_past and bool(pat.search(h))) or ""
        ids = set(pat.findall(html))
    courses = _race_list_courses(html)
    return {i: courses.get(i) for i in sorted(ids) if i[4:6] in PLACE}


def get_race_ids(kaisai_date: str) -> list[str]:
    """開催日のJRAレースID一覧（12桁・場コード01〜10のみ）"""
    return list(get_race_list(kaisai_date))


# 取得対象の絞り込み（--surface / --min-dist で変更）
FILTER = {"surface": "芝", "min_dist": 1800}


def want_race(course: tuple[str, int] | None) -> bool:
    """絞り込み条件に合うか。一覧から距離が読めなかったレースは安全側で取得する"""
    if course is None:
        return True
    surface, dist = course
    if FILTER["surface"] and surface != FILTER["surface"]:
        return False
    return dist >= (FILTER["min_dist"] or 0)


# ============================================================ 結果ページ（共通）
def _nfkc(s) -> str:
    """全角数字・記号を半角にそろえる（"３歳以上" → "3歳以上"）"""
    return unicodedata.normalize("NFKC", s or "")


def _pace_from_lap(laps: list[float], distance: int | None) -> dict:
    """ラップから前半3F（600m地点の通過タイム）と後半3F（最後の600m）を計算する。
    200mで割り切れない距離は最初の区間が100m等になるので、600m地点を区間内で補間する。"""
    if len(laps) < 4:
        return {}
    first = distance - 200 * (len(laps) - 1) if distance else 200
    if not 0 < first <= 200:
        first = 200
    seg = [first] + [200] * (len(laps) - 1)
    pos, cum = 0, 0.0
    for v, L in zip(laps, seg):
        if pos + L >= 600:
            cum += v * (600 - pos) / L
            break
        cum += v
        pos += L
    return {"前半3F": round(cum, 1), "後半3F": round(sum(laps[-3:]), 1), "最初の区間m": first}


def _to_seconds(s):
    m = re.match(r"(?:(\d+):)?(\d+(?:\.\d+)?)$", str(s).strip())
    if not m:
        return None
    return int(m.group(1) or 0) * 60 + float(m.group(2))


def _parse_race_info(soup, race_id: str) -> dict:
    info = {"race_id": race_id, "場": PLACE.get(race_id[4:6]), "回": int(race_id[6:8]),
            "日目": int(race_id[8:10]), "R": int(race_id[10:12])}

    title = soup.title.get_text() if soup.title else ""
    if m := re.search(r"\((G[123I]+|J\.G[123I]+|L|OP)\)", title):
        info["グレード"] = m.group(1).replace("III", "3").replace("II", "2").replace("GI", "G1")

    intro = soup.select_one("div.data_intro")
    if not intro:
        return info
    h1 = intro.find("h1")
    info["レース名"] = h1.get_text(strip=True) if h1 else ""

    span = intro.select_one("p span")
    s = span.get_text(" ", strip=True) if span else ""
    info["条件文"] = s
    if m := re.search(r"(芝|ダ|障)([^\d/]*?)(\d{3,4})\s*m", s):
        info["コース"], extra, info["距離"] = m.group(1), m.group(2), int(m.group(3))
        info["回り"] = "右" if "右" in extra else "左" if "左" in extra else "直線" if "直" in extra else ""
        info["内外"] = "外" if "外" in extra else "内" if "内" in extra else ""
    if m := re.search(r"天候\s*:\s*(\S+)", s):
        info["天候"] = m.group(1)
    if m := re.search(r"(?:芝|ダート)\s*:\s*(\S+)", s):
        info["馬場"] = m.group(1)
    if m := re.search(r"発走\s*:\s*(\d+:\d+)", s):
        info["発走"] = m.group(1)

    small = intro.select_one("p.smalltxt")
    st = small.get_text(" ", strip=True) if small else ""
    info["詳細"] = st
    if m := re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", st):
        info["日付"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    if m := re.search(r"(新馬|未勝利|1勝クラス|2勝クラス|3勝クラス|500万下|1000万下|1600万下|オープン)", st):
        info["クラス"] = m.group(1)
    if m := re.search(r"(2歳|3歳以上|4歳以上|3歳)", st):
        info["年齢条件"] = m.group(1)
    return info


def _parse_lap_and_corner(soup) -> dict:
    out = {}
    for t in soup.select("table.result_table_02"):
        for tr in t.find_all("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            key, val = th.get_text(strip=True), td.get_text(" ", strip=True)
            if key == "ラップ":
                out["ラップ"] = re.sub(r"\s*-\s*", "-", val)
            elif key == "ペース":
                out["ペース"] = re.sub(r"\s*-\s*", "-", val)
                if m := re.search(r"\((\d+\.\d)-(\d+\.\d)\)", val):
                    out["前半3F"], out["後半3F"] = float(m.group(1)), float(m.group(2))
            elif "コーナー" in key:
                out[key] = val  # 1コーナー〜4コーナーの隊列（例: "(*3,5)-1,2-..."）
    return out


# レース側ページの列名 → DB側と同じ列名にそろえる
RACE_PAGE_COLS = {"枠": "枠番", "単勝オッズ": "単勝", "後3F": "上り", "コーナー通過順": "通過",
                  "厩舎": "調教師", "馬体重(増減)": "馬体重"}


def _parse_result_table(table, race_id: str) -> pd.DataFrame:
    df = pd.read_html(StringIO(str(table)))[0]
    df.columns = [_nfkc(str(c)).replace(" ", "") for c in df.columns]
    df = df.rename(columns=RACE_PAGE_COLS)
    # 有料会員限定の列（"**"表示）は捨てる
    df = df.drop(columns=[c for c in df.columns
                          if c in ("タイム指数", "調教タイム", "厩舎コメント", "備考")
                          or df[c].astype(str).eq("**").all()], errors="ignore")

    rows = table.select("tr.HorseList")
    if len(rows) != len(df):
        rows = [tr for tr in table.find_all("tr") if tr.find("td")]
    df["horse_id"] = [_id_from_href(tr.select_one('a[href*="/horse/"]'), "horse") for tr in rows][: len(df)]
    df["jockey_id"] = [_id_from_href(tr.select_one('a[href*="/jockey/"]'), "jockey") for tr in rows][: len(df)]
    df["trainer_id"] = [_id_from_href(tr.select_one('a[href*="/trainer/"]'), "trainer") for tr in rows][: len(df)]
    df.insert(0, "race_id", race_id)

    # 派生列
    rank = df["着順"].astype(str)
    df["着順_数値"] = pd.to_numeric(rank.str.extract(r"^(\d+)")[0], errors="coerce")
    df["異常区分"] = rank.str.extract(r"(取|除|中|失|降)")[0]
    if "性齢" in df.columns:
        df["性"] = df["性齢"].astype(str).str[0]
        df["年齢"] = pd.to_numeric(df["性齢"].astype(str).str[1:], errors="coerce")
    if "タイム" in df.columns:
        df["タイム秒"] = df["タイム"].map(_to_seconds)
    if "馬体重" in df.columns:
        # "506(+2)" / "454"（増減0は省略されることがある）/ "計不"
        w = df["馬体重"].astype(str).str.extract(r"^\s*(\d{3})(?:\.0)?\s*(?:\(([^)]*)\))?")
        df["体重"] = pd.to_numeric(w[0], errors="coerce")
        diff = w[1].fillna("0").str.replace("+", "", regex=False)
        df["体重増減"] = pd.to_numeric(diff, errors="coerce").where(df["体重"].notna())
    df["頭数"] = len(df)
    return df


KENSHU_SIZE = {"単勝": 1, "複勝": 1, "枠連": 2, "馬連": 2, "ワイド": 2, "馬単": 2, "三連複": 3, "三連単": 3}


def _norm_kenshu(s: str) -> str:
    return _nfkc(s).strip().replace("3連", "三連")


def _parse_payout(soup, race_id: str) -> pd.DataFrame:
    """DB側ページの払戻"""
    rows = []
    for t in soup.select("table.pay_table_01"):
        for tr in t.find_all("tr"):
            th, tds = tr.find("th"), tr.find_all("td")
            if not th or len(tds) < 3:
                continue
            split = lambda td: [x.strip() for x in td.get_text("\n").split("\n") if x.strip()]
            nums, pays, pops = split(tds[0]), split(tds[1]), split(tds[2])
            for i, n in enumerate(nums):
                rows.append({
                    "race_id": race_id, "券種": _norm_kenshu(th.get_text(strip=True)),
                    "組番": re.sub(r"\s*(?:-|→)\s*", "-", n),
                    "払戻": int(pays[i].replace(",", "")) if i < len(pays) and pays[i].replace(",", "").isdigit() else None,
                    "人気": pops[i] if i < len(pops) else None,
                })
    return pd.DataFrame(rows)


# ============================================================ 結果ページ（レース側）
def _parse_race_info_race_page(soup, race_id: str) -> dict:
    info = {"race_id": race_id, "場": PLACE.get(race_id[4:6]), "回": int(race_id[6:8]),
            "日目": int(race_id[8:10]), "R": int(race_id[10:12])}
    title = _nfkc(soup.title.get_text() if soup.title else "")

    name = soup.select_one(".RaceName")
    info["レース名"] = _nfkc(name.get_text(" ", strip=True)) if name else ""
    if m := re.search(r"\((G[123]|J\.?G[123]|L|OP)\)", title + info["レース名"]):
        info["グレード"] = m.group(1)
    else:
        icon = soup.select_one('[class*="Icon_GradeType"]')
        if icon and (m := re.search(r"Icon_GradeType(\d+)", " ".join(icon.get("class", [])))):
            info["グレード"] = {"1": "G1", "2": "G2", "3": "G3"}.get(m.group(1))

    d1 = soup.select_one(".RaceData01")
    s = _nfkc(d1.get_text(" ", strip=True)) if d1 else ""
    info["条件文"] = s
    if m := re.search(r"(芝|ダ|障)\s*(\d{3,4})\s*m", s):
        info["コース"], info["距離"] = m.group(1), int(m.group(2))
        extra = (re.search(r"m\s*\(([^)]*)\)", s) or [None, ""])[1]
        info["回り"] = "右" if "右" in extra else "左" if "左" in extra else "直線" if "直" in extra else ""
        info["内外"] = "外" if "外" in extra else "内" if "内" in extra else ""
    if m := re.search(r"天候\s*:\s*(\S+)", s):
        info["天候"] = m.group(1)
    if m := re.search(r"馬場\s*:\s*(\S+)", s):
        info["馬場"] = m.group(1)
    if m := re.search(r"(\d{1,2}:\d{2})\s*発走", s):
        info["発走"] = m.group(1)

    if d1:
        # 天候はアイコンの属性に入っている: <span class="Icon_Weather Weather01" title="晴">
        icon = d1.select_one(".Icon_Weather")
        if icon and not info.get("天候"):
            w = _nfkc(icon.get("title") or icon.get("aria-label") or "")
            if not w and (m := re.search(r"Weather(\d+)", " ".join(icon.get("class", [])))):
                w = {"01": "晴", "02": "曇", "03": "雨", "04": "小雨", "05": "雪", "06": "小雪"}.get(m.group(1), "")
            if w:
                info["天候"] = w
        # 馬場はラベルなしで Item03 に入っている: <span class="Item03"> 良</span>
        item = d1.select_one(".Item03")
        if item and not info.get("馬場"):
            b = _nfkc(item.get_text(strip=True)).replace("馬場", "").strip(" :/")
            if b in ("良", "稍重", "稍", "重", "不良", "不"):
                info["馬場"] = {"稍": "稍重", "不": "不良"}.get(b, b)
        # "芝2500m 右 A" → 回り=右、コース区分=A（内外がある場合は "右 外 B" など）
        if m := re.search(r"\d{3,4}\s*m\s*(右|左|直線)?\s*(内|外)?[^/]*?\b([A-E])\b", s):
            if m.group(1) and not info.get("回り"):
                info["回り"] = m.group(1)
            if m.group(2) and not info.get("内外"):
                info["内外"] = m.group(2)
            info["コース区分"] = m.group(3)

    # まだ見つからない項目は、レース情報欄のHTML全体とページ全体から探す
    box = soup.select_one(".RaceList_NameBox") or soup.select_one(".RaceData01")
    raw = _nfkc(str(box)) if box else ""
    whole = _nfkc(soup.get_text(" "))
    if not info.get("回り"):
        if m := re.search(r"\d{3,4}\s*m[^/]{0,20}?(右|左|直線)", re.sub(r"<[^>]+>", " ", raw)):
            info["回り"] = m.group(1)
        if m := re.search(r"\d{3,4}\s*m[^/]{0,20}?(内|外)", re.sub(r"<[^>]+>", " ", raw)):
            info["内外"] = m.group(1)
    if not info.get("天候"):
        m = re.search(r"天候\s*:?\s*(晴|曇|小雨|雨|小雪|雪)", whole)
        if m:
            info["天候"] = m.group(1)
    if not info.get("馬場"):
        m = re.search(r"(?:馬場|芝|ダート|ダ)\s*:\s*(良|稍重|稍|重|不良|不)", whole)
        if m:
            info["馬場"] = {"稍": "稍重", "不": "不良"}.get(m.group(1), m.group(1))
    info["_debug_RaceData01"] = s  # テストで原因を確認するため（保存前に消す）

    d2 = soup.select_one(".RaceData02")
    st = _nfkc(d2.get_text(" ", strip=True)) if d2 else ""
    info["詳細"] = st
    if m := re.search(r"(新馬|未勝利|1勝クラス|2勝クラス|3勝クラス|500万下|1000万下|1600万下|オープン)", st):
        info["クラス"] = m.group(1)
    if m := re.search(r"(2歳|3歳以上|4歳以上|3歳)", st):
        info["年齢条件"] = m.group(1)
    if m := re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title):
        info["日付"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return info


def _parse_lap_and_corner_race_page(soup) -> dict:
    out = {}
    box = soup.select_one(".Race_HaronTime")
    if box:
        rows = []
        for tr in box.find_all("tr"):
            vals = [float(x) for x in re.findall(r"\d+\.\d", " ".join(td.get_text(" ") for td in tr.find_all("td")))]
            if vals:
                rows.append(vals)
        # ハロンタイム（各区間）と通過タイム（累計）の2行がある。各区間の行は値が小さい
        lap = next((r for r in rows if max(r) < 20), None)
        if lap:
            out["ラップ"] = "-".join(f"{v:.1f}" for v in lap)
    for th in soup.find_all("th"):
        key = _nfkc(th.get_text(strip=True))
        if re.fullmatch(r"[1-4]コーナー", key) and (td := th.find_next_sibling("td")):
            out[key] = _nfkc(td.get_text("", strip=True))
    return out


def _parse_payout_race_page(soup, race_id: str) -> pd.DataFrame:
    rows = []
    for tr in soup.select("table.Payout_Detail_Table tr"):
        th, res, pay = tr.find("th"), tr.select_one("td.Result"), tr.select_one("td.Payout")
        nin = tr.select_one("td.Ninki")
        if not (th and res and pay):
            continue
        kind = _norm_kenshu(th.get_text(strip=True))
        k = KENSHU_SIZE.get(kind)
        pay_text = pay.get_text(" ")
        pays = [int(x.replace(",", "")) for x in (re.findall(r"([\d,]+)\s*円", pay_text) or re.findall(r"[\d,]+", pay_text))]
        nums = re.findall(r"\d+", res.get_text(" "))
        pops = re.findall(r"(\d+)\s*人気", nin.get_text(" ")) if nin else []
        if not k or not pays or not nums:
            continue
        if len(nums) == k * len(pays):
            combos = ["-".join(nums[i * k:(i + 1) * k]) for i in range(len(pays))]
        else:  # 想定外の並び。組番をまとめて1行に残しておく
            combos = ["-".join(nums)]
        for i, c in enumerate(combos):
            rows.append({"race_id": race_id, "券種": kind, "組番": c,
                         "払戻": pays[i] if i < len(pays) else None,
                         "人気": pops[i] if i < len(pops) else None})
    return pd.DataFrame(rows)


def _from_race_page(race_id: str):
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    ok = lambda h: ("All_Result_Table" in h or "RaceTable01" in h) and "Payout_Detail_Table" in h
    html = fetch(url, valid=ok)
    if not html or not ok(html):
        return None
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table#All_Result_Table") or soup.select_one("table.RaceTable01")
    if table is None:
        return None
    result = _parse_result_table(table, race_id)
    if result.empty or result["着順_数値"].notna().sum() == 0:
        return None
    info = _parse_race_info_race_page(soup, race_id)
    info.update(_parse_lap_and_corner_race_page(soup))
    info["取得元"] = "race"
    return info, result, _parse_payout_race_page(soup, race_id)


def _from_db_page(race_id: str):
    url = f"https://db.netkeiba.com/race/{race_id}/"
    html = fetch(url, valid=lambda h: "race_table_01" in h)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.race_table_01")
    if table is None:
        return None
    info = _parse_race_info(soup, race_id)
    info.update(_parse_lap_and_corner(soup))
    info["取得元"] = "db"
    return info, _parse_result_table(table, race_id), _parse_payout(soup, race_id)


def get_race_result(race_id: str, kaisai_date: str | None = None):
    """確定済みレースの (race_info dict, result DataFrame, payout DataFrame)。未確定なら None
    レース側ページを優先し、取れないときだけDB側ページで補う。"""
    got = _from_race_page(race_id) or _from_db_page(race_id)
    if not got:
        return None
    info, result, pay = got
    if not info.get("日付") and kaisai_date:
        info["日付"] = f"{kaisai_date[:4]}-{kaisai_date[4:6]}-{kaisai_date[6:]}"
    if info.get("ラップ"):  # 前半/後半3Fはラップと距離から計算し直す（取得元によらず同じ定義に）
        laps = [float(x) for x in info["ラップ"].split("-") if x]
        info.update(_pace_from_lap(laps, info.get("距離")))
    info["頭数"] = len(result)
    return info, result, pay


# ============================================================ 血統
def get_pedigree(horse_id: str) -> dict | None:
    url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
    html = fetch(url, valid=lambda h: "blood_table" in h)
    if not html:
        return None
    t = BeautifulSoup(html, "lxml").select_one("table.blood_table")
    if t is None:
        return None

    def name_id(td):
        a = td.find("a", href=re.compile(r"/horse/"))
        name = a.get_text(strip=True) if a else td.get_text(" ", strip=True).split(" ")[0]
        return name, _id_from_href(a, "horse")

    g1 = t.find_all("td", rowspan="16")  # [父, 母]
    g2 = t.find_all("td", rowspan="8")   # [父父, 父母, 母父, 母母]
    if len(g1) < 2 or len(g2) < 4:
        return None
    (f, f_id), (m, m_id), (mf, mf_id) = name_id(g1[0]), name_id(g1[1]), name_id(g2[2])
    return {"horse_id": horse_id, "父": f, "父_id": f_id, "母": m, "母_id": m_id,
            "母父": mf, "母父_id": mf_id}


# ============================================================ 出馬表（本番用）
def get_shutuba(race_id: str) -> pd.DataFrame:
    """これから走るレースの出馬表。オッズはJSで読み込まれるため含まない"""
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    soup = BeautifulSoup(fetch(url) or "", "lxml")  # 当日まで変わるのでキャッシュしない
    rows = []
    for tr in soup.select("table.Shutuba_Table tr.HorseList"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        cell = lambda cls, i: (tr.find("td", class_=re.compile(cls)) or tds[i]).get_text(strip=True)
        h, j, tr_ = (tr.select_one(f"td.{c} a") for c in ("HorseInfo", "Jockey", "Trainer"))
        rows.append({
            "race_id": race_id, "枠": cell(r"^Waku", 0), "馬番": cell(r"^Umaban", 1),
            "馬名": h.get_text(strip=True) if h else "", "horse_id": _id_from_href(h, "horse"),
            "性齢": cell("Barei", 4), "斤量": tds[5].get_text(strip=True),
            "騎手": j.get_text(strip=True) if j else "", "jockey_id": _id_from_href(j, "jockey"),
            "厩舎": tr_.get_text(strip=True) if tr_ else "", "trainer_id": _id_from_href(tr_, "trainer"),
            "馬体重": cell("Weight", 8),
        })
    # 出馬表ページはキャッシュに残さない
    (CACHE_DIR / (hashlib.md5(url.encode()).hexdigest() + ".html")).unlink(missing_ok=True)
    return pd.DataFrame(rows)


# ============================================================ コマンド
def _fmt(sec: float) -> str:
    h, m = divmod(int(sec) // 60, 60)
    return f"{h}時間{m:02d}分" if h else f"{m}分{int(sec) % 60:02d}秒"


def cmd_results(start_year: int, end_year: int):
    today = date.today()
    t_all = time.time()
    for year in range(start_year, end_year + 1):
        infos, results, pays = [], [], []
        t0 = time.time()
        req0 = STATS["requests"]
        for month in range(1, 13):
            if date(year, month, 1) > today:
                break
            for d in get_kaisai_dates(year, month):
                if d >= today.strftime("%Y%m%d"):
                    continue
                race_list = get_race_list(d)
                ids = [rid for rid, c in race_list.items() if want_race(c)]
                skipped = len(race_list) - len(ids)
                for rid in ids:
                    try:
                        got = get_race_result(rid, d)
                    except Exception as e:
                        print(f"    {rid} 解析失敗: {e}")
                        continue
                    if got:
                        got[0].pop("_debug_RaceData01", None)
                        infos.append(got[0]); results.append(got[1]); pays.append(got[2])
                n_req = STATS["requests"] - req0
                per = (time.time() - t0) / n_req if n_req else 0
                print(f"{d}: {len(ids)}R（対象外{skipped}R）  累計{len(infos)}R  経過{_fmt(time.time() - t0)}"
                      f"  通信{n_req}回（平均{per:.1f}秒/回）", flush=True)

        if not infos:
            print(f"{year}: データなし")
            continue
        out = DATA_DIR / str(year)
        save_table(pd.DataFrame(infos), out / "race_info")
        save_table(pd.concat(results, ignore_index=True), out / "result")
        save_table(pd.concat([p for p in pays if len(p)], ignore_index=True), out / "payout")
        print(f"== {year}: {len(infos)}レースを保存 ({out})  この年{_fmt(time.time() - t0)}"
              f"  全体{_fmt(time.time() - t_all)}", flush=True)


def cmd_ped(since: int | None):
    horse_ids = set()
    for ydir in sorted(DATA_DIR.glob("[12][0-9][0-9][0-9]")):
        if since and int(ydir.name) < since:
            continue
        df = load_table(ydir / "result")
        if df is not None:
            horse_ids |= set(df["horse_id"].dropna().astype(str))

    done = load_table(DATA_DIR / "pedigree")
    rows = done.to_dict("records") if done is not None else []
    have = {str(r["horse_id"]) for r in rows}
    todo = sorted(horse_ids - have)
    print(f"対象{len(horse_ids)}頭 / 取得済み{len(have)}頭 / 残り{len(todo)}頭"
          f"（約{len(todo) * (SLEEP_SEC + JITTER_SEC / 2) / 3600:.1f}時間）")

    for i, hid in enumerate(todo, 1):
        p = get_pedigree(hid)
        if p:
            rows.append(p)
        if i % 200 == 0 or i == len(todo):  # 200頭ごとに保存（中断しても続きから）
            save_table(pd.DataFrame(rows), DATA_DIR / "pedigree")
            print(f"  {i}/{len(todo)}頭")


def cmd_race(race_id: str):
    got = get_race_result(race_id)
    if not got:
        print("結果が見つかりません（未確定 or race_id誤り）")
        return
    info, result, pay = got
    for k, v in info.items():
        print(f"  {k}: {v}")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(result.head(18))
        print(pay)


def cmd_shutuba(race_id: str):
    df = get_shutuba(race_id)
    save_table(df, DATA_DIR / "live" / f"shutuba_{race_id}_{datetime.now():%Y%m%d%H%M}")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="netkeiba スクレイパー v2")
    ap.add_argument("--sleep", type=float, help=f"最低待ち時間（秒、既定{SLEEP_SEC}、3未満は不可）")
    ap.add_argument("--max-pages", type=int, help=f"1回の実行で通信する上限ページ数（既定{MAX_PAGES}、0で無制限）")
    ap.add_argument("--surface", choices=["芝", "ダ", "all"], help="取得する馬場（既定: 芝。all で全部）")
    ap.add_argument("--min-dist", type=int, help=f"取得する最短距離m（既定: {FILTER['min_dist']}。0で制限なし）")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("results"); p.add_argument("start", type=int); p.add_argument("end", type=int, nargs="?")
    p = sub.add_parser("ped"); p.add_argument("--since", type=int)
    p = sub.add_parser("race"); p.add_argument("race_id")
    p = sub.add_parser("shutuba"); p.add_argument("race_id")
    a = ap.parse_args()
    if a.sleep is not None:
        SLEEP_SEC = max(3.0, a.sleep)
    if a.max_pages is not None:
        MAX_PAGES = a.max_pages
    if a.surface:
        FILTER["surface"] = None if a.surface == "all" else a.surface
    if a.min_dist is not None:
        FILTER["min_dist"] = a.min_dist
    if a.cmd == "results":
        print(f"取得対象: {FILTER['surface'] or '芝・ダ・障'} / {FILTER['min_dist'] or 0}m以上", flush=True)

    try:
        if a.cmd == "results":
            cmd_results(a.start, a.end or a.start)
        elif a.cmd == "ped":
            cmd_ped(a.since)
        elif a.cmd == "race":
            cmd_race(a.race_id)
        elif a.cmd == "shutuba":
            cmd_shutuba(a.race_id)
    except StopRun as e:
        print(f"\n【停止】{e}")
        print(f"今回の通信 {STATS['requests']}回 / キャッシュ利用 {STATS['cache_hits']}回")
        sys.exit(2)
    except KeyboardInterrupt:
        print("\n中断しました。もう一度同じコマンドを実行すれば、キャッシュを使って続きから進みます。")
        sys.exit(1)
