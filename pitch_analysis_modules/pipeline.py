"""
메인 파이프라인 모듈
전체 분석 파이프라인을 실행하는 함수
"""

from .data_loader import load_data_from_bigquery
from .case_definer import define_at_bat_cases, filter_cases
from .preprocessor import prepare_timestamps, clean_dataframe, add_end_node
from .event_log import create_event_log
from .process_mining import create_process_model
from .transition_analyzer import calculate_transition_probabilities
from .visualizer import visualize_transition_graph_pyvis


def analyze_pitching_patterns(key_path="key.json", limit=None, min_prob=0.05, case_type='out'):
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
    
    # ➕ 여기에 추가
    from .preprocessor import attach_case_result_to_pitch_type
    df_event = attach_case_result_to_pitch_type(df_event)

    # 케이스 타입에 따라 데이터 포인트 필터링
    df_filtered, result_counts = filter_cases(df_event, case_type)
    output_file = f"transition_graph_{case_type}.html"
    num_cases = result_counts.get(case_type, 0)
    num_pitches = len(df_filtered)
    print(f"\n=== {case_type.capitalize()} 케이스 분석 ===")
    print(f"케이스 수: {num_cases:,}개")
    print(f"투구 수: {num_pitches:,}개")
    print(f"결과 분포:\n{result_counts}")
    
    # Timestamp 준비
    df_with_timestamps = prepare_timestamps(df_filtered)
    
    # 🎯 타석 종료 노드 추가 (타석 순서때문에 첨가한 코드 )------
    df_with_timestamps = add_end_node(df_with_timestamps)
    # --------------------------------------------------------

    # 데이터 정리
    df_clean = clean_dataframe(df_with_timestamps)
    
    # 이벤트 로그 생성
    event_log = create_event_log(df_clean)
    
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
        'result_counts': result_counts,
        'num_cases': num_cases,
        'num_pitches': num_pitches
    }

