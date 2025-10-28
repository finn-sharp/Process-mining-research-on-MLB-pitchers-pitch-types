"""
케이스 정의 모듈
타석을 프로세스 케이스로 정의하고 필터링하는 함수들
"""

from xxlimited import Str
import pandas as pd


def define_at_bat_cases(df):
    """
    각 타석을 케이스로 정의 (game_date + batter)
    같은 게임의 같은 타자 = 하나의 케이스 (하나의 프로세스)
    
    Args:
        df: 투구 데이터 DataFrame
    
    Returns:
        DataFrame: case_id가 추가된 DataFrame
    """
    df_event = df.copy()
    
    # [1. 정렬] - game_date + batter별로 정렬 (이미 순서대로 되어있지만 확실히)
    df_event = df_event.sort_values(by=['game_date', 'batter']).reset_index(drop=True)
    
    # [2. 데이터 포인터 생성] - case_id 생성: 같은 게임의 같은 타자 = 하나의 케이스
    df_event['case_id'] = (
        df_event['game_date'].astype(str) + "_" + 
        df_event['batter'].astype(str)
    )
    
    # [3. 투구 순서 부여] - 케이스별로 정렬 + 투구 순서 부여 (데이터 순서대로)
    df_event = df_event.sort_values(by=['case_id']).reset_index(drop=True)
    df_event['pitch_order'] = df_event.groupby('case_id').cumcount()
    
    return df_event


def filter_cases(df_event, case, out_events=None, reach_events=None):
    """
    출루/아웃으로 끝나는 타석만 필터링
    
    Args:
        df_event: 케이스가 정의된 DataFrame
        case: 분류할 케이스 타입 ('out' 또는 'reach')
        out_events: 아웃 이벤트 리스트
        reach_events: 출루 이벤트 리스트
    
    Returns:
        DataFrame: 아웃으로 끝나는 타석만 포함된 DataFrame
    """

    # [ Target :  아웃 이벤트 리스트 설정 ]
    if out_events is None:
        out_events = ['strikeout', 'out', 'field_out', 'force_out', 'double_play', 'triple_play', 
                      'strikeout_double_play', 'sac_fly', 'sac_bunt']
    
    # [ Target : 출루 이벤트 리스트 설정 ]
    if reach_events is None:
        reach_events = ['single', 'double', 'triple', 'home_run', 'walk', 'hit_by_pitch', 
                        'catcher_interf', 'field_error', 'fielders_choice']
    
    def classify_case_result(case_df):
        """타석의 마지막 이벤트로 결과 분류"""
        if len(case_df) == 0:
            return 'unknown'
        
        # 마지막 유효한 이벤트 찾기 (NaN이 아닌 것 중에서)
        events_series = case_df['events'].dropna()
        if len(events_series) == 0:
            return 'ongoing'
        
        last_event = events_series.iloc[-1]
        
        if last_event in out_events:
            return 'out'
        elif last_event in reach_events:
            return 'reach'
        else:
            return 'other'
    
    # [ 필터링 결과 분류 ] - 각 case_id별로 결과 분류
    case_results = df_event.groupby('case_id', group_keys=False).apply(
        lambda x: pd.Series([classify_case_result(x)], index=[x.name])
    )
    
    # [ 필터링 ] - 아웃으로 끝나는 케이스만 필터링
    out_case_ids = case_results[case_results == case].index.tolist()
    df_filtered = df_event[df_event['case_id'].isin(out_case_ids)].copy()
    
    return df_filtered, case_results.value_counts()


