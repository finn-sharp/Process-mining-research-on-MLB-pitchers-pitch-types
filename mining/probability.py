"""
확률 분석 모듈
"""
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.obj import EventLog, Trace

from pm4py.algo.filtering.log.variants.variants_filter import get_variants
from collections import defaultdict
import pandas as pd


def prepare_eventLog(df_clean):
    """
    DataFrame에서 None 값 완전히 제거
    
    Args:
        df_event: 원본 DataFrame 

    Returns:
        DataFrame: 정리된 DataFrame
    """
    # 필요한 컬럼만 선택
    df_clean = df_clean[['case:concept:name', 'concept:name', 'time:timestamp']].copy()
    
    # None, 빈 문자열, NaN 모두 제거
    df_clean = df_clean.dropna(subset=['concept:name'])
    df_clean = df_clean[df_clean['concept:name'].notna()]
    df_clean = df_clean[df_clean['concept:name'] != '']
    df_clean = df_clean[df_clean['concept:name'].astype(str) != 'nan']
    df_clean['concept:name'] = df_clean['concept:name'].astype(str)
    
    return df_clean


def clean_event_log(log):
    """
    이벤트 로그에서 None 값을 완전히 제거
    
    Args:
        log: PM4Py 이벤트 로그
    
    Returns:
        EventLog: 정리된 이벤트 로그
    """
    cleaned_log = EventLog()
    
    for _, case in enumerate(log):
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


def create_eventlog_from_dataFrame(df_clean):
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


class BasedTraces:
    
    def __init__(self, dataframe):
        self.dataframe = dataframe
        
        self.event_log = self.preprocessing()
        self.grouped_event_log = self.grouped_preprocessing()

    def preprocessing(self):
        prepared_df = prepare_eventLog(self.dataframe)
        eventlog_df = create_eventlog_from_dataFrame(prepared_df)        
        return eventlog_df

    def grouped_preprocessing(self):
        case_lengths = self.dataframe.groupby('processID').size().reset_index(name='case_length')
        merged_df = pd.merge(left=self.dataframe, 
                            right=case_lengths, 
                            how='outer', 
                            on='processID')

        grouped_preprocessed_data = []
        for i, df in merged_df.groupby('case_length'):
            prepared_df = prepare_eventLog(df)
            eventlog_df = create_eventlog_from_dataFrame(prepared_df)                    
            grouped_preprocessed_data.append((f"length_{i}", eventlog_df))
        return grouped_preprocessed_data

    def achieve_rawdata(self):
        """
                Description : Eventlog의 Vriants Pattern과 Freq, Length을 저장하기 위한 함수
        """
        raw_data = defaultdict(list)
        variants_dict = get_variants(self.event_log)
        
        for activities, traces in variants_dict.items():
            activity_length = len(activities) - 2
        
            raw_data['all'].append((activities, len(traces), activity_length))
            raw_data[f'length_{activity_length}'].append((activities, len(traces)))
            
        return raw_data
        
    
    def calc_translation(self):
        """
             Description : Eventlog에서 Length와 Layer 구분없이 빈도와 전이확률을 계산
        """
        counts = defaultdict(lambda: defaultdict(int))

        for case in self.event_log:
            activities = [event['concept:name'] for event in case]

            for i in range(len(activities) - 1):
                from_activity = activities[i]
                to_activity = activities[i + 1]        
                counts[from_activity][to_activity] += 1

        probs = {}
        for from_activity, to_dict in counts.items(): 
            total = sum(to_dict.values()) 
            probs[from_activity] = {to_activity: count / total for to_activity, count in to_dict.items()}
        
        return counts, probs
    

    def calc_transition_same_length(self):
        """
             Description : Eventlog에서 Length가 같은 varient pattern에 대하여 빈도와 전이확률 계산
        """
        variants_dict = get_variants(self.event_log)
        counts = defaultdict(lambda : defaultdict(lambda:defaultdict(int)))
        probs = defaultdict(lambda : defaultdict(int))
        
        for activities, traces in variants_dict.items():
            activity_legnth = len(activities) - 2
            
            for i in range(len(activities) - 1):
                from_activity = activities[i]
                to_activity = activities[i + 1]        
                counts[f'length_{activity_legnth}'][from_activity][to_activity] += 1
            
        for length in counts.keys():                
            for from_activity, to_dict in counts[length].items(): 
                total = sum(to_dict.values()) 
                probs[length][from_activity] = {to_activity: count / total for to_activity, count in to_dict.items()}
                
        return counts, probs
        
        
    def calc_transition_same_layer(self):
        """
             Description : Eventlog에서 Layer 별로 빈도와 전이확률 계산
        """
        counts = defaultdict(lambda: defaultdict(int))
        probs = {}

        for case in self.event_log:
            activities = [event['concept:name'] for event in case]                
            
            layered_activities = list(activities)
            for i in range(1, len(layered_activities) - 1):
                 layered_activities[i] = layered_activities[i] + "_" + str(i)
                 
            for i in range(len(layered_activities)-1):
                from_activity = layered_activities[i]    
                to_activity = layered_activities[i+1]
                counts[from_activity][to_activity] += 1

        for from_activity, to_dict in counts.items(): 
            total = sum(to_dict.values()) 
            probs[from_activity] = {to_activity: count / total for to_activity, count in to_dict.items()}
            
        return counts, probs
        
        
    def calc_transition_same_layer_and_length(self):
        """
                 Description : Eventlog에서 Layer와 Length가 같은 varient patterns들의 빈도와 전이확률 계산
        """
        variants_dict = get_variants(self.event_log)
        
        counts = defaultdict(lambda : defaultdict(lambda:defaultdict(int)))
        probs = defaultdict(lambda : defaultdict(int))
        
        for activities, traces in variants_dict.items():
            activity_length = len(activities) -2
        
            layered_activities = list(activities)
            for i in range(1, len(layered_activities) - 1):
                 layered_activities[i] = layered_activities[i] + "_" + str(i)
                 
            for i in range(len(layered_activities)-1):
                from_activity = layered_activities[i]    
                to_activity = layered_activities[i+1]
                counts[f"length_{activity_length}"][from_activity][to_activity] += 1
        
        for length in counts.keys():
            for from_activity, to_dict in counts[length].items(): 
                total = sum(to_dict.values()) 
                probs[length][from_activity] = {to_activity: count / total for to_activity, count in to_dict.items()}
    
        return counts, probs
            

        
    def __call__(self):
        
        result = {}
        result['event_log'] = self.event_log

        result['data'] = self.achieve_rawdata()         
        result['counts'], result['probs'] = self.calc_translation()
        
        result['length'] = {}
        result['length']['event_log'] = self.grouped_event_log
        result['length']['counts'], result['length']['probs'] = self.calc_transition_same_length()
        
        result['layer'] = {}
        result['layer_length'] = {}
        result['layer']['counts'], result['layer']['probs'] = self.calc_transition_same_layer()
        result['layer_length']['counts'], result['layer_length']['probs'] = self.calc_transition_same_layer_and_length()
        
        return result
