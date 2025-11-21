"""
케이스 정의 모듈
타석을 프로세스 케이스로 정의하고 필터링하는 함수들
"""

#from xxlimited import Str
import pandas as pd
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


