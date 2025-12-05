"""
파이프라인 모듈
CSV > EDA 까지 한 flow로 가는 코드
"""
import pandas as pd

# custom
from .utils import load_data_from_bigquery

from .preprocessing import define_at_bat_cases
from .preprocessing import add_node_and_preprocess
from .preprocessing import one_way_filter

from .probability import BasedTraces
from .exploratory import ProcessEDA



def preprocessing_df(df, start_name='start', end_name='end', case_type=None):

    # case 정의
    df_grouped = define_at_bat_cases(df)

    # 결측치 indexing (pitch_type)
    missing_index = set(df_grouped[df_grouped['pitch_type'].isna()]['processID'])
    valid_index = ~df_grouped['processID'].isin(missing_index)
    
    # 결측치 제거
    df_valid = df_grouped[valid_index]
    
    # 시작, 종료 노드 추가
    df_added = add_node_and_preprocess(df_valid, start_name, end_name, case_type=case_type)

    return df_added


def one_step_EDA_from_bigquery(path="key.json", limit=None, start_name='start', end_name='end', case_type=None):
    """
    전체 분석 파이프라인 실행
    
    Args:
        key_path: BigQuery 키 파일 경로
        limit: 데이터 제한 (None이면 전체)
        min_prob: 전이 확률 최소 임계값
        case_type: 분석할 케이스 타입 ('out' 또는 'reach')
    
    Returns:
        dict: 분석 결과
    """
    # Data Load
    df = load_data_from_bigquery(key_path="key.json", limit=limit)

    # Data Preprocess
    df_preprocess = preprocessing_df(df, start_name=start_name, end_name=end_name, case_type=case_type)

    # Data Filtering
    df_filtered = one_way_filter(df_preprocess, 'events', ['strikeout'])

    # Event Log 데이터를 Probability로 계산
    calc_eventlog = BasedTraces(df_filtered) 
    final_result = calc_eventlog()

    # Probability Based EDA : 기술통계량 및 시각화
    eda = ProcessEDA(final_result)

    return eda
    
def one_step_EDA_from_csv(path:str, limit=None, start_name='start', end_name='end', case_type=None):
    """
    전체 분석 파이프라인 실행
    
    Args:
        key_path: BigQuery 키 파일 경로
        limit: 데이터 제한 (None이면 전체)
        min_prob: 전이 확률 최소 임계값
        case_type: 분석할 케이스 타입 ('out' 또는 'reach')
    
    Returns:
        dict: 분석 결과
    """
    # Data Load
    df = pd.read_csv(path)

    # Data Preprocess
    df_preprocess = preprocessing_df(df, start_name=start_name, end_name=end_name, case_type=case_type)

    # Data Filtering
    df_filtered = one_way_filter(df_preprocess, 'events', ['strikeout'])

    # Event Log 데이터를 Probability로 계산
    calc_eventlog = BasedTraces(df_filtered) 
    final_result = calc_eventlog()

    # Probability Based EDA : 기술통계량 및 시각화
    eda = ProcessEDA(final_result)

    
    return eda


