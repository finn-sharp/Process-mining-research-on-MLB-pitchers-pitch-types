from pitch_analysis_modules import (
    load_data_from_bigquery,
    define_at_bat_cases,
    one_way_filter,
    addNodeAndPreprocess,
    prepareEventLog,
    createEventLogFromDataFrame,
    create_process_model,
    calculate_transition_probabilities,
    visualize_transition_graph_pyvis,
    analyze_pitching_patterns,
    compare_transition_probabilities,
    print_comparison_summary
)


# ==================== 방법 1: 전체 파이프라인 한 번에 실행 ====================

def example_full_pipeline():
    """전체 분석을 한 번에 실행"""
    results = analyze_pitching_patterns(
        key_path="key.json",
        limit=None,  # 전체 데이터 사용 (테스트용으로는 limit=100 등 사용 가능)
        min_prob=0.05  # 5% 이상의 전이 확률만 표시
    )
    
    return results


# ==================== 방법 2: 단계별 실행 ====================

def example_step_by_step():
    """단계별로 실행하여 중간 결과 확인"""
    
    # 1. 데이터 로드
    print("1. 데이터 로드 중...")
    df = load_data_from_bigquery(key_path="key.json", limit=1000)
    print(f"   로드된 데이터: {len(df)}행")
    
    # 2. 프로세스 라벨링
    print("\n2. 프로세스 별 라벨링 작업 중...")
    df_event = define_at_bat_cases(df)
    print(f"   총 타석 수: {df_event['processID'].nunique()}")
    
    # 3. 데이터 필터링
    print("\n3. 사용자 정의 필터 작업 중...")
    filter = {}
    filter['colName'] = 'events'
    filter['posCondition'] = ['strikeout']
    
    df_filtered, result_counts = one_way_filter(df_event, **filter)
    print(f"   케이스: {len(df_filtered['case_id'].unique())}개")
    print(f"   결과 분포:\n{result_counts}")
    
    # 4. 노드 추가 및 pm4py 필수 컬럼 생성
    start_name = 'In'
    end_name = 'Out'
    print("\n4. 시작 및 종료 노드 추가 및 pm4py 필수 컬럼 생성 중...")
    df_with_start = addNodeAndPreprocess(df_filtered, start_name, end_name)
    print(f"   시작 및 종료 노드 추가 및 pm4py 필수 컬럼 생성 완료")
    
    # 5. pm4py 포멧 변경
    print("\n5. pm4py 포멧 변경 중...")
    df_clean = prepareEventLog(df_with_start)
    print(f"   변경 후 pm4py 포멧: {len(df_clean)}행")
    
    # 6. 이벤트 로그 생성
    print("\n6. 이벤트 로그 생성 중...")
    event_log = createEventLogFromDataFrame(df_clean)
    print(f"   이벤트 로그 케이스 수: {len(event_log)}")
    
    # 7. 프로세스 모델 생성
    print("\n7. 프로세스 모델 생성 중...")
    net, im, fm = create_process_model(event_log)
    print("   모델 생성 완료!")
    
    # 8. 전이 확률 계산
    print("\n8. 전이 확률 계산 중...")
    transition_probs, transition_counts = calculate_transition_probabilities(event_log)
    
    # 9. 전이 확률 그래프 시각화
    print("\n9. 전이 확률 그래프 시각화 중...")
    # Pyvis를 사용한 인터랙티브 시각화
    visualize_transition_graph_pyvis(transition_probs, transition_counts, min_prob=0.05)
    
    return {
        'df': df,
        'df_filtered': df_filtered,
        'event_log': event_log,
        'net': net,
        'im': im,
        'fm': fm,
        'transition_probs': transition_probs,
        'transition_counts': transition_counts
    }


# ==================== 방법 3: 특정 기능만 사용 ====================

def example_custom_analysis():
    """특정 기능만 사용하는 예시"""
    
    # 데이터 로드
    df = load_data_from_bigquery(key_path="key.json", limit=500)
    
    # 타석 케이스 정의
    df_event = define_at_bat_cases(df)
    
    # 아웃 케이스만 필터링
    filter = {}
    filter['colName'] = 'events'
    filter['posCondition'] = ['strikeout']
    
    df_filtered, _ = one_way_filter(df_event, **filter)    
    
    # 시작 노드 추가
    start_name = 'In'
    end_name = 'Out'
    df_with_start = addNodeAndPreprocess(df_filtered, start_name, end_name)
    
    # 데이터 정리
    df_clean = prepareEventLog(df_with_start)
    
    # 이벤트 로그 생성
    event_log = createEventLogFromDataFrame(df_clean)
    
    # 전이 확률만 계산 (시각화 없이)
    transition_probs, transition_counts = calculate_transition_probabilities(event_log)
    
    # 전이 확률만 출력
    print("\n=== 전이 확률 요약 ===")
    for from_activity in sorted(transition_probs.keys()):
        print(f"\n{from_activity}에서:")
        sorted_transitions = sorted(transition_probs[from_activity].items(), 
                                   key=lambda x: x[1], reverse=True)
        for to_activity, prob in sorted_transitions:
            count = transition_counts[from_activity][to_activity]
            print(f"  → {to_activity}: {prob:.3f} ({count}회)")
    
    return transition_probs, transition_counts


if __name__ == "__main__":
    # 원하는 방법 선택해서 실행
    filter_out = {}
    filter_out['colName'] = 'events'
    filter_out['posCondition'] = ['strikeout']

    print("=== One-Way Filted Case Process Mining ===")
    results_out = analyze_pitching_patterns(
        key_path="key.json",
        limit=None,  # 전체 데이터 사용
        min_prob=0.05,
        case_type='out',
        filter=filter_out
        )
    
    filter_reach = {}
    filter_reach['colName'] = 'events'
    filter_reach['posCondition'] = ['walk', 'single', 'double', 'triple', 'home_run']

    print("\n=== 출루 케이스 분석 ===")
    results_reach = analyze_pitching_patterns(
        key_path="key.json",
        limit=None,  # 전체 데이터 사용
        min_prob=0.05,
        case_type='reach',
        filter=filter_reach
    )
    
    # 전이 확률 비교 및 Loss 계산
    print("\n=== 전이 확률 비교 분석 ===")
    comparison = compare_transition_probabilities(
        results_out['transition_probs'],
        results_reach['transition_probs'],
        results_out['transition_counts'],
        results_reach['transition_counts']
    )
    
    # print_comparison_summary(
    #     comparison,
    #     num_out_cases=results_out['num_cases'],
    #     num_reach_cases=results_reach['num_cases']
    # )
    
    # 또는
    # print("=== 단계별 실행 ===")
    # results = example_step_by_step()
    
    # 또는
    # print("=== 커스텀 분석 ===")
    # transition_probs, transition_counts = example_custom_analysis()

