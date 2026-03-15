import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import time
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com/'
}

os.makedirs("data_theme", exist_ok=True)

# =============================================
# 1. 전체 테마 목록 수집 (1~7페이지)
# =============================================
theme_list = []

for page in range(1, 8):
    url  = f"https://finance.naver.com/sise/theme.naver?field=change_rate&ordering=desc&page={page}"
    res  = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    for a in soup.select('a[href*="sise_group_detail"]'):
        name = a.text.strip()
        href = a['href']
        if 'no=' in href:
            code = href.split('no=')[-1].split('&')[0]
            if name and code:
                theme_list.append({'테마명': name, '테마코드': code})

    print(f"페이지 {page} 수집 완료 → 누적 테마 수: {len(theme_list)}개")
    time.sleep(0.3)

df_themes = pd.DataFrame(theme_list).drop_duplicates(subset=['테마코드'])
print(f"\n전체 테마 수: {len(df_themes)}개")
df_themes.to_csv("data_theme/theme_list.csv", index=False, encoding="utf-8-sig")
print("✅ 테마 목록 저장: data_theme/theme_list.csv")

# =============================================
# 2. 테마별 종목 수집
# =============================================
all_theme_stocks = []
total = len(df_themes)

for i, row in df_themes.iterrows():
    theme_name = row['테마명']
    theme_code = row['테마코드']

    try:
        url    = f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={theme_code}"
        res    = requests.get(url, headers=headers)
        dfs    = pd.read_html(StringIO(res.text))

        if len(dfs) < 3:
            print(f"[{i+1}/{total}] {theme_name} → 테이블 없음 스킵")
            continue

        df_raw = dfs[2].copy()

        # NaN 행 제거
        df_raw = df_raw[df_raw.iloc[:, 0].notna()]
        df_raw = df_raw[df_raw.iloc[:, 2].notna()]
        df_raw = df_raw[df_raw.iloc[:, 0].astype(str) != 'nan']

        # 종목명: 첫번째 컬럼 (* 제거)
        df_raw['종목명'] = df_raw.iloc[:, 0].astype(str).str.replace(r' \*$', '', regex=True).str.strip()

        # 등락률: 6번째 컬럼
        df_raw['등락률'] = df_raw.iloc[:, 5].astype(str).str.replace('%', '').str.replace('+', '').str.strip()
        df_raw['등락률'] = pd.to_numeric(df_raw['등락률'], errors='coerce')

        # 테마 정보 추가
        df_raw['테마명']  = theme_name
        df_raw['테마코드'] = theme_code

        df_result = df_raw[['테마코드', '테마명', '종목명', '등락률']].copy()
        df_result = df_result[df_result['종목명'].str.len() > 0]
        all_theme_stocks.append(df_result)

        print(f"[{i+1}/{total}] {theme_name} → {len(df_result)}개 종목")

    except Exception as e:
        print(f"[{i+1}/{total}] {theme_name} 실패: {e}")

    time.sleep(0.3)

# =============================================
# 3. 전체 저장
# =============================================
if all_theme_stocks:
    df_all = pd.concat(all_theme_stocks, ignore_index=True)
    df_all.to_csv("data_theme/theme_stocks.csv", index=False, encoding="utf-8-sig")
    print(f"\n✅ 테마별 종목 저장 완료: data_theme/theme_stocks.csv")
    print(f"총 {len(df_all)}개 행 (종목-테마 조합)")
    print(df_all.head(20).to_string())
else:
    print("❌ 수집된 데이터 없음")