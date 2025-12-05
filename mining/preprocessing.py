"""
데이터 전처리 모듈
"""

import pandas as pd
from datetime import timedelta

# Helper Functions
def deleteNullPitchType(df_event):
    """
    pitch_type이 'nan'(문자열) 또는 None인 행 제거
    """
    p_index = set(df_event[df_event['pitch_type'].isna()]['processID'])
    condition = ~df_event['processID'].isin(p_index)
    
    df_event = df_event[condition]
    df_event = df_event.sort_values(by=['processID'], ascending=True).reset_index(drop=True)
    return df_event

def checkNullPitchType(df_event):
    """
    pitch_type이 'nan'(문자열) 또는 None인 행 확인
    """
    p_index = set(df_event[df_event['pitch_type'].isna()]['processID'])
    df_check_null = df_event[df_event['processID'].isin(p_index)]

    return df_check_null

def descriptionToGroups(df_event):
    """
        description을 3개 범주로 그룹화
        
        [그룹화 목적]
        - Process Activity를 정의 할 때, "구종+그룹화된 결과"로 사용해보려함
     
    """
    mapping = {
        'called_strike': 'S', # 스트라이크
        'swinging_strike': 'S',
        'swinging_strike_blocked': 'S',
        'foul_tip': 'S',
        'foul_bunt': 'S',
        'missed_bunt': 'S',
        'blocked_ball': 'B',# 볼&데드볼
        'ball': 'B',
        'hit_by_pitch': 'B',
        'foul': 'I', # 인플레이(In-play)&파울
        'hit_into_play': 'I'
    }
    df_event['grouped_description'] = df_event['description'].map(mapping).fillna('result')
    return df_event


def attach_case_result_to_pitch_type(df_event): 
    """
    각 투구의 pitch_type에 해당 타석의 최종 결과(out / reach / other)를 붙임
    예: SL → SL_out, SI → SI_reach
    """
    # case_id별 마지막 이벤트 기반으로 결과 분류
    case_results = df_event.groupby('processID')['events'].apply(
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
    df_event = df_event.merge(case_results.rename('case_result'), on='processID', how='left')

    # pitch_type + 결과 결합
    df_event['pitch_type'] = df_event['pitch_type'] + '_' + df_event['case_result']

    return df_event

# 시작 노드 끝 노드 설정하는 걸로 변경하기 (Labeling으로 도식화)
def add_node_and_preprocess(df_event, start_name, end_name, case_type=None):
 
    # [1] 행 제거(PitchType is Null)
    acept_data = deleteNullPitchType(df_event)
    
    # [2] Description을 3개 범주로 그룹화
    acept_data = descriptionToGroups(acept_data)

    # [2.1] 출루 유무에 따라 concept:name을 설정할 경우
    if case_type == 'reach' or case_type == 'out':
        acept_data = attach_case_result_to_pitch_type(acept_data)
        acept_data['concept:name'] = acept_data['pitch_type'].astype(str)
        if case_type == 'reach+discription' or case_type == 'out+discription':
            acept_data['concept:name'] = acept_data['pitch_type'].astype(str)+ "_" + acept_data['grouped_description'].astype(str) 
    
    # [3] pm4py용 EventLog 포맷으로 변경 (Timestamp, CaseID, Activity)
    acept_data['time:timestamp'] = acept_data['game_date'] + pd.to_timedelta(acept_data['pitchOrder'], unit='s')
    acept_data['case:concept:name'] = acept_data['processID']
    if case_type == None:
        acept_data['concept:name'] = acept_data['pitch_type'].astype(str)
    elif case_type == 'discription':
        acept_data['concept:name'] = acept_data['pitch_type'].astype(str)+ "_" + acept_data['grouped_description'].astype(str) 

        
    
    # [4] 시작/종료 노드 추가
    start_name = start_name
    end_name = end_name
    add_node_list = []

    for process in acept_data['processID'].unique():
        case_df = acept_data[acept_data['processID'] == process]
        if len(case_df) > 0:
            # 첫 번째 행을 복사하여 "In" 노드 생성
            first_row = case_df.iloc[-1].copy()
            first_row['time:timestamp'] = first_row['time:timestamp'] - timedelta(seconds=1)
            first_row['case:concept:name'] = first_row['processID']
            first_row['concept:name'] = start_name
            first_row['pitch_type'] = start_name
            first_row['pitchOrder'] = -1  # 시작 노드는 -1로 설정
        
            last_row = case_df.iloc[0].copy()
            last_row['time:timestamp'] = last_row['time:timestamp'] + timedelta(seconds=1)
            last_row['case:concept:name'] = last_row['processID']
            last_row['concept:name'] = end_name
            last_row['pitch_type'] = end_name
            last_row['pitchOrder'] = len(case_df)
            
            add_node_list.extend([first_row, last_row])

    # [4] 결과 저장
    node_df = pd.DataFrame(add_node_list)
    acept_data = pd.concat([acept_data, node_df], ignore_index=True)
    acept_data = acept_data.sort_values(by=['processID', 'pitchOrder'], ascending=[True, True]).reset_index(drop=True)

    return acept_data


def assign_group_index_two_pointer(df):
    """
    투 포인터 알고리즘:
    - left: 시작 포인터 (고정)
    - right: 순차 탐색 포인터
    - left의 case_id == right의 case_id → 같은 그룹 인덱스 부여
    - left의 case_id != right의 case_id → 그룹 인덱스 +1, left를 right로 업데이트
    """
    n = len(df)
    group_indices = [0] * n
    
    if n == 0:
        return group_indices
    
    left = 0  # 시작 포인터
    group_index = 0  # 그룹 인덱스 (case_id가 달라질 때마다 증가)
    start_case_id = df.iloc[left]['case_id']  # 시작 case_id
    
    # 첫 번째 행은 항상 그룹 0
    group_indices[left] = group_index
    
    # right 포인터로 순차 탐색
    for right in range(1, n):
        current_case_id = df.iloc[right]['case_id']
        
        # 시작 case_id와 같으면 같은 그룹 인덱스 부여
        if current_case_id == start_case_id:
            group_indices[right] = group_index
        else:
            # case_id가 달라졌으므로 그룹 인덱스 증가
            group_index += 1
            # 시작값을 달라진 값으로 업데이트
            start_case_id = current_case_id
            left = right  # left 포인터를 right 위치로 업데이트
            group_indices[right] = group_index
    
    return group_indices


def define_at_bat_cases(df):
    """
    각 타석을 케이스로 정의 (game_date + batter)
    경기일자가 같고 같은 게임의 같은 타자 = 하나의 케이스 (하나의 프로세스)
    
    Args:
        df: 투구 데이터 DataFrame
    
    Returns:
        DataFrame: case_id가 추가된 DataFrame
    """
    # 투구 순서 부여 + 케이스별로 정렬 + 그룹 인덱스 라벨링
    df_event = df.copy()
    df_event = df_event.reset_index()
    df_event['case_id'] = (df_event['game_date'].astype(str) + "_" + df_event['batter'].astype(str))
    df_event['processID'] = assign_group_index_two_pointer(df_event)
    df_event = df_event.sort_values(by=['case_id','index'], ascending=[True, False]).reset_index(drop=True)

    # 그룹 인덱스 할당
    df_event['pitchOrder'] = df_event.groupby('processID').cumcount()
    df_event = df_event.sort_values(by=['processID','index'], ascending=[True, True]).reset_index(drop=True)
    del df_event['index']
    del df_event['case_id']

    return df_event


def one_way_filter(df, colName = 'events', posCondition = ['strikeout']):

    # 1. 조건에 걸리는 행만 추출
    condition = df[colName].isin(posCondition)
    df_filtered = df[condition]

    # 2. 조건에 걸리는 행의 processID 추출
    processID_list = df_filtered['processID'].unique()

    # 3. 조건에 걸리는 행의 processID로 케이스 필터링
    df_filtered = df[df['processID'].isin(processID_list)]

    return df_filtered

