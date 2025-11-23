"""
메인 파이프라인 모듈
전체 분석 파이프라인을 실행하는 함수
"""

from .data_loader import load_data_from_bigquery
from .case_definer import define_at_bat_cases, one_way_filter
from .preprocessor import prepareEventLog, addNodeAndPreprocess
from .event_log import createEventLogFromDataFrame
from .process_mining import create_process_model
from .transition_analyzer import calculate_transition_probabilities
from .visualizer import visualize_transition_graph_pyvis


def analyze_pitching_patterns(key_path="key.json", limit=None, min_prob=0.05, case_type='out', filter=None, start_name='In', end_name='Out'):
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
    # 데이터 로드
    df = load_data_from_bigquery(key_path, limit)
    
    # 타석 케이스 정의
    df_event = define_at_bat_cases(df)
    
    #######
    #######
    #######
    ####### pm4py에서 제공하는 패턴으로 변경해야할 부분
    #######
    #######
    from .preprocessor import attach_case_result_to_pitch_type
    df_event = attach_case_result_to_pitch_type(df_event)

    if filter is None :
        pass
    else : 
        # 케이스 타입에 따라 데이터 포인트 필터링
        df_filtered = one_way_filter(df_event, **filter)
        output_file = f"transition_graph_{case_type}.html"
    
    # 시작/종료 노드 추가 및 pm4py 필수 컬럼 생성
    df_node_added = addNodeAndPreprocess(df_filtered, start_name=start_name, end_name=end_name)
    
    # pm4py 포멧 변경
    df_clean = prepareEventLog(df_node_added)
    
    # 이벤트 로그 생성
    event_log = createEventLogFromDataFrame(df_clean)
    
    # 전이 확률 계산
    transition_probs, transition_counts = calculate_transition_probabilities(event_log)
    
    # 전이 확률 그래프 시각화
    visualize_transition_graph_pyvis(transition_probs, transition_counts, min_prob, output_file, case_type)
    
    # 프로세스 모델 생성 (필요시에만)
    try:
        net, im, fm = create_process_model(event_log)
    except:
        net, im, fm = None, None, None
    
    return {
        'df': df,
        'df_filtered': df_filtered,
        'event_log': event_log,
        'net': net,
        'im': im,
        'fm': fm,
        'transition_probs': transition_probs,
        'transition_counts': transition_counts,
        'case_type': case_type,
        # 'result_counts': result_counts,
        # 'num_cases': num_cases,
        # 'num_pitches': num_pitches
    }

