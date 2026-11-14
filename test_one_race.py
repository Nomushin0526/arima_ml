"""
netkeiba_scraper.py の動作確認（1レースだけ・アクセスは5回程度）

使い方（netkeiba_scraper.py と同じフォルダで）
  python test_one_race.py                  # 2023年有馬記念でテスト
  python test_one_race.py 202405050811     # 別のレースでテスト

確認すること
  1. 開催カレンダーから開催日が取れるか
  2. 開催日からレースID一覧が取れるか
  3. 結果ページ（レース情報・ラップ・着順・払戻）が取れるか
  4. 1着馬の血統が取れるか
  5. 1ページあたりの実測時間と、本番にかかる時間の見積もり
全部 OK なら本番（python netkeiba_scraper.py results ...）を回してOK。
NG が出たら、この画面の出力をそのまま貼ってください。

Google Colab で試す場合（2つのファイルを左のフォルダ欄にアップロードしてから）
  !python test_one_race.py
  ※Colabの接続は数時間で切れ、cache/ も消えるので、本番は自分のPCで回すこと
"""

import sys
import time

import pandas as pd

import netkeiba_scraper as ns

# 12桁の数字が渡されたときだけ使う（Colab/Jupyterの内部引数 "-f ..." を誤って拾わないため）
_args = [a for a in sys.argv[1:] if a.isdigit() and len(a) == 12]
RACE_ID = _args[0] if _args else "202306050811"  # 2023年 有馬記念
results = []
timings = []  # (処理名, 秒, 通信回数)


def check(name, ok, detail=""):
    results.append(ok)
    print(f"  [{'OK' if ok else 'NG'}] {name}  {detail}")


def timed(label, fn, *args):
    """fnを実行し、かかった時間と通信回数を記録する"""
    req0, t0 = ns.STATS["requests"], time.time()
    out = fn(*args)
    sec, n = time.time() - t0, ns.STATS["requests"] - req0
    timings.append((label, sec, n))
    print(f"    ⏱ {label}: {sec:.1f}秒（通信{n}回）")
    return out


def print_timing():
    print("\n--- 時間計測 ---")
    total = sum(s for _, s, _ in timings)
    n_req = sum(n for _, _, n in timings)
    print(f"  合計 {total:.1f}秒 / 通信{n_req}回 / キャッシュ利用{ns.STATS['cache_hits']}回")
    if n_req == 0:
        print("  すべてキャッシュから読んだため計測できません。cache/ を消して再実行してください。")
        return
    measured = total / n_req
    # 最初の1回は待ち時間なしで出るため、実測平均は低めに出る。設定上の平均待ち時間を下限にする
    per = max(measured, ns.SLEEP_SEC + ns.JITTER_SEC / 2 + 0.3)
    print(f"  1回あたり 実測平均{measured:.1f}秒 → 見積もりには{per:.1f}秒を使用"
          f"（設定: {ns.SLEEP_SEC:g}〜{ns.SLEEP_SEC + ns.JITTER_SEC:g}秒待ち＋通信時間）")
    # 本番の見積もり：結果ページ 約3,450/年 ＋ 開催日・レース一覧 約300/年
    print("  本番の見積もり（この平均値で計算）:")
    for label, years in [("2025〜2026年", 1.9), ("2020〜2026年", 6.9), ("2013〜2026年", 13.9), ("2008〜2026年", 18.9)]:
        pages = int(years * 3750)
        print(f"    {label}: 約{pages:,}ページ → 約{pages * per / 3600:.0f}時間")
    print("  ※通信5回程度の平均なので目安です。本番ログの「平均◯秒/回」で確かめてください。")


def main():
    print(f"テスト対象: {RACE_ID}  （待ち時間込みで30秒ほどかかります）\n")

    # 1. 結果ページ
    print("1. 結果ページ")
    got = timed("結果ページ", ns.get_race_result, RACE_ID)
    check("結果ページを取得", got is not None, "" if got else "→ race_idが違うか、ページ構造が変わった可能性")
    if not got:
        return
    info, result, pay = got

    check("取得元がレース側ページ", info.get("取得元") == "race",
          f"取得元={info.get('取得元')!r}（'db' だと通過・上がりが欠ける）")
    for key in ["レース名", "日付", "コース", "距離", "回り", "馬場", "天候", "クラス"]:
        check(f"レース情報: {key}", bool(info.get(key)), repr(info.get(key)))
    if not all(info.get(k) for k in ["回り", "馬場", "天候"]):
        html = ns.fetch(f"https://race.netkeiba.com/race/result.html?race_id={RACE_ID}") or ""
        from bs4 import BeautifulSoup
        box = BeautifulSoup(html, "lxml").select_one(".RaceData01")
        print("    ↓原因調査用（この2行も貼ってください）")
        print(f"    RaceData01の文字: {info.get('_debug_RaceData01')!r}")
        print(f"    RaceData01のHTML: {str(box)[:600]!r}")
    check("ラップ", bool(info.get("ラップ")), str(info.get("ラップ", ""))[:40])
    check("前半/後半3F", "前半3F" in info,
          f"{info.get('前半3F')} / {info.get('後半3F')}（最初の区間{info.get('最初の区間m')}m）")
    corners = [k for k in info if "コーナー" in k]
    check("コーナー通過順", bool(corners), ", ".join(corners))

    check("出走頭数", len(result) >= 5, f"{len(result)}頭")
    for col in ["着順_数値", "枠番", "馬番", "horse_id", "jockey_id", "trainer_id",
                "タイム秒", "通過", "上り", "単勝", "人気", "体重"]:
        if col not in result.columns:
            check(f"結果の列: {col}", False, "→ 列がない")
            continue
        filled = result[col].notna().mean()
        check(f"結果の列: {col}", filled >= 0.8, f"埋まり率 {filled:.0%}")

    kinds = set(pay["券種"]) if len(pay) else set()
    for k in ["単勝", "三連複", "三連単"]:
        row = pay[pay["券種"] == k] if len(pay) else pd.DataFrame()
        check(f"払戻: {k}", len(row) > 0,
              f"{row.iloc[0]['組番']} → {row.iloc[0]['払戻']}円" if len(row) else f"取れた券種: {sorted(kinds)}")

    # 2. 開催カレンダーとレース一覧（このレースの開催日で確認）
    print("\n2. 開催カレンダー・レース一覧")
    day = (info.get("日付") or "").replace("-", "")
    if day:
        dates = timed("開催カレンダー", ns.get_kaisai_dates, int(day[:4]), int(day[4:6]))
        check("開催日一覧", day in dates, f"{len(dates)}日（{day}を含む: {day in dates}）")
        race_list = timed("レース一覧", ns.get_race_list, day)
        ids = list(race_list)
        check("レースID一覧", RACE_ID in ids, f"{len(ids)}レース（対象を含む: {RACE_ID in ids}）")
        read = {k: v for k, v in race_list.items() if v}
        check("一覧から距離を読めた", len(read) >= len(ids) * 0.9,
              f"{len(read)}/{len(ids)}レース（読めないと絞り込みが効かず全部取得になる）")
        print(f"    この開催日の内訳: " + ", ".join(f"{s}{d}m" for s, d in list(read.values())[:12]) + " …")
        n_want = sum(ns.want_race(c) for c in race_list.values())
        print(f"    絞り込み（{ns.FILTER['surface']}・{ns.FILTER['min_dist']}m以上）で取得するのは {n_want}/{len(ids)}レース")
    else:
        check("開催日一覧", False, "→ 日付が取れていないため確認できない")

    # 3. 血統（1着馬）
    print("\n3. 血統")
    winner = result.sort_values("着順_数値").iloc[0]
    ped = timed("血統ページ", ns.get_pedigree, str(winner["horse_id"]))
    check("血統ページ", ped is not None,
          f"{winner['馬名']}: 父={ped['父']} 母={ped['母']} 母父={ped['母父']}" if ped else "")

    # 結果の表示
    print("\n--- 取得結果（上位5頭） ---")
    cols = [c for c in ["着順", "枠番", "馬番", "馬名", "騎手", "タイム", "通過", "上り", "単勝", "人気", "馬体重", "trainer_id"]
            if c in result.columns]
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(result[cols].head(5).to_string(index=False))
        print("\n--- 払戻 ---")
        print(pay.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except ns.StopRun as e:
        print(f"\n【停止】{e}")
        results.append(False)
    except Exception as e:
        import traceback
        traceback.print_exc()
        results.append(False)
    print_timing()
    ng = results.count(False)
    print(f"\n==== {len(results) - ng}/{len(results)} 項目 OK ====")
    print("本番を回して大丈夫です。" if ng == 0 and results else "NG の行と上の出力を貼ってください。")
