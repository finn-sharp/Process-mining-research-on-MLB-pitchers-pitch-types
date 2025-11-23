"""
이벤트 로그 생성 모듈
DataFrame을 PM4Py 이벤트 로그로 변환하는 함수들
"""

from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.obj import EventLog, Trace


def clean_event_log(log):
    """
    이벤트 로그에서 None 값을 완전히 제거
    
    Args:
        log: PM4Py 이벤트 로그
    
    Returns:
        EventLog: 정리된 이벤트 로그
    """
    cleaned_log = EventLog()
    
    for idx, case in enumerate(log):
        cleaned_events = []
        for event in case:
            # Event 객체도 dict처럼 get() 메서드 사용 가능
            try:
                concept_name = event.get('concept:name', None) if hasattr(event, 'get') else getattr(event, 'concept:name', None)
            except:
                continue
            
            # None 체크 및 유효성 검사
            if concept_name is not None and concept_name != '' and str(concept_name).lower() != 'nan':
                cleaned_events.append(event)
        
        if len(cleaned_events) > 0:
            # Trace 객체 생성 (attributes는 생성자에서 처리)
            cleaned_case = Trace(cleaned_events)
            # attributes는 읽기 전용이므로, 원본 케이스를 참조하거나 새로 생성
            if hasattr(case, 'attributes'):
                # attributes를 직접 복사하는 대신, 원본 케이스 유지
                pass
            cleaned_log.append(cleaned_case)
    
    return cleaned_log


def createEventLogFromDataFrame(df_clean):
    """
    DataFrame을 PM4Py 이벤트 로그로 변환
    
    Args:
        df_clean: 정리된 DataFrame
    
    Returns:
        EventLog: PM4Py 이벤트 로그
    """

    # Timestamp 변환 & 이벤트 로그 변환
    df_clean = dataframe_utils.convert_timestamp_columns_in_df(df_clean)
    event_log = log_converter.apply(df_clean)
    
    # None 값 제거
    event_log = clean_event_log(event_log)
    
    return event_log

