"""
데이터 전처리 모듈
타임스탬프 생성, 시작 노드 추가, 데이터 정리 함수들
"""

import pandas as pd
from datetime import timedelta


def prepare_timestamps(df_event):
    """
    각 타석의 투구에 timestamp 부여 (In 노드 없이)
    
    Args:
        df_event: 필터링된 DataFrame
    
    Returns:
        DataFrame: timestamp가 추가된 DataFrame
    """

    # 1️⃣ pitch_type이 'nan'(문자열) 또는 None인 행 제거
    df_event = df_event[
        ~df_event['pitch_type'].astype(str).str.lower().isin(['nan', 'none', '', 'null'])
    ].copy()
    
    # 2️⃣ 각 케이스별로 정렬
    df_event = df_event.sort_values(by=['case_id', 'pitch_order']).reset_index(drop=True)
    
    # 3️⃣ timestamp 생성
    df_event['time:timestamp'] = df_event.apply(
        lambda row: row['game_date'] + timedelta(seconds=row['pitch_order']),
        axis=1
    )
    
    return df_event



def add_start_node(df_event):
    """
    각 타석의 시작에 "In" 노드 추가
    
    Args:
        df_event: 필터링된 DataFrame
    
    Returns:
        DataFrame: "In" 노드가 추가된 DataFrame
    """
    df_with_start = df_event.copy()
    
    # 각 케이스별로 정렬
    df_with_start = df_with_start.sort_values(by=['case_id', 'pitch_order']).reset_index(drop=True)
    
    # time:timestamp가 없으면 먼저 생성
    if 'time:timestamp' not in df_with_start.columns:
        df_with_start = prepare_timestamps(df_with_start)
    
    # 각 케이스의 시작에 "In" 노드 추가
    start_rows = []
    for case_id in df_with_start['case_id'].unique():
        case_df = df_with_start[df_with_start['case_id'] == case_id]
        if len(case_df) > 0:
            # 첫 번째 행을 복사하여 "In" 노드 생성
            first_row = case_df.iloc[0].copy()
            first_row['pitch_type'] = 'In'
            first_row['pitch_order'] = -1  # 시작 노드는 -1로 설정
            # 첫 번째 투구보다 1초 전 시간 설정
            if 'time:timestamp' in first_row and pd.notna(first_row['time:timestamp']):
                first_row['time:timestamp'] = first_row['time:timestamp'] - timedelta(seconds=1)
            elif 'game_date' in first_row:
                first_row['time:timestamp'] = first_row['game_date'] - timedelta(seconds=1)
            start_rows.append(first_row)
    
    # 시작 노드 DataFrame 생성
    if start_rows:
        start_df = pd.DataFrame(start_rows)
        # 원본 데이터와 합치기
        df_with_start = pd.concat([start_df, df_with_start], ignore_index=True)
        # 케이스와 순서로 정렬
        df_with_start = df_with_start.sort_values(by=['case_id', 'pitch_order']).reset_index(drop=True)
    
    return df_with_start


def attach_case_result_to_pitch_type(df_event):
    """
    각 투구의 pitch_type에 해당 타석의 최종 결과(out / reach / other)를 붙임
    예: SL → SL_out, SI → SI_reach
    """
    # case_id별 마지막 이벤트 기반으로 결과 분류
    case_results = df_event.groupby('case_id')['events'].apply(
        lambda x: x.dropna().iloc[-1] if len(x.dropna()) > 0 else None
    )

    def classify_result(event):
        if event in ['strikeout', 'out', 'field_out', 'force_out', 'double_play', 'triple_play',
                     'strikeout_double_play', 'sac_fly', 'sac_bunt']:
            return 'out'
        elif event in ['single', 'double', 'triple', 'home_run', 'walk', 'hit_by_pitch',
                       'catcher_interf', 'field_error', 'fielders_choice']:
            return 'reach'
        else:
            return 'other'

    case_results = case_results.apply(classify_result)

    # 원본 DataFrame에 결과 병합
    df_event = df_event.merge(case_results.rename('case_result'), on='case_id', how='left')

    # pitch_type + 결과 결합
    df_event['pitch_type'] = df_event['pitch_type'] + '_' + df_event['case_result']

    return df_event


def clean_dataframe(df_event):
    """
    DataFrame에서 None 값 완전히 제거
    
    Args:
        df_event: 원본 DataFrame (이미 FF가 제외된 상태)
    
    Returns:
        DataFrame: 정리된 DataFrame
    """
    # pm4py 포맷으로 컬럼 이름 맞추기
   # 1. description 그룹화
    df_event = map_description_to_category(df_event)

    # 2. pitch_type + description_group 조합으로 concept:name 생성
    df_event['concept:name'] = df_event['pitch_type'].astype(str) + " - " + df_event['description_group']

    # 3. pm4py용 컬럼 이름 통일
    df_clean = df_event.rename(columns={
        'case_id': 'case:concept:name'
    })
    
    # 필요한 컬럼만 선택
    df_clean = df_clean[['case:concept:name', 'concept:name', 'time:timestamp']].copy()
    
    # None, 빈 문자열, NaN 모두 제거
    df_clean = df_clean.dropna(subset=['concept:name'])
    df_clean = df_clean[df_clean['concept:name'].notna()]
    df_clean = df_clean[df_clean['concept:name'] != '']
    df_clean = df_clean[df_clean['concept:name'].astype(str) != 'nan']
    df_clean['concept:name'] = df_clean['concept:name'].astype(str)
    
    return df_clean

def map_description_to_category(df):
    """description을 5개 범주로 그룹화"""
    mapping = {
        'hit_into_play': '공을 침',
        'ball': '볼',
        'blocked_ball': '볼',
        'called_strike': '스트라이크',
        'swinging_strike': '스트라이크',
        'swinging_strike_blocked': '스트라이크',
        'foul_tip': '스트라이크',
        'foul_bunt': '스트라이크',
        'missed_bunt': '스트라이크',
        'foul': '파울',
        'hit_by_pitch': '데드볼'
    }
    df['description_group'] = df['description'].map(mapping).fillna('result')
    return df


## 훈성 타석 순서 첨가
def add_end_node(df_event):
    """
    각 타석(case_id)의 마지막 pitch 이벤트를 프로세스 종료 이벤트로 추가
    (예: single, strikeout, walk 등)
    """
    df_with_end = df_event.copy()

    end_rows = []
    for case_id, case_df in df_with_end.groupby('case_id'):
        case_df = case_df.sort_values(by='pitch_order')
        # 마지막 pitch
        last_row = case_df.iloc[-1].copy()
        if pd.notna(last_row.get('events')) and last_row['events'] not in [None, '', 'None', 'nan']:
            # 새로운 "종료 노드" 생성
            end_row = last_row.copy()
            end_row['pitch_type'] = str(last_row['events']).capitalize()  # e.g. 'Single', 'Strikeout'
            end_row['description'] = 'Result'
            end_row['pitch_order'] = last_row['pitch_order'] + 1
            # timestamp는 마지막 pitch보다 +1초
            if 'time:timestamp' in end_row:
                end_row['time:timestamp'] = end_row['time:timestamp'] + timedelta(seconds=1)
            end_rows.append(end_row)
    
    # 종료 노드 추가
    if end_rows:
        end_df = pd.DataFrame(end_rows)
        df_with_end = pd.concat([df_with_end, end_df], ignore_index=True)
        df_with_end = df_with_end.sort_values(by=['case_id', 'pitch_order']).reset_index(drop=True)
    
    return df_with_end
