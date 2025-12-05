"""
프로세스 마이닝 결과에 대한 EDA 모듈
"""

from .visualizer import sankey_visualizer
from .visualizer import interactive_graph
from .utils import extract_stage_number
import pandas as pd

class ProcessEDA:
    """
    이벤트 로그 기반 탐색적 데이터 분석(EDA) 모듈
    
    Args:
        calculation (dict): EventLogToProbability 클래스의 계산 결과 딕셔너리.
        (case_id, activity, timestamp 등의 표준 컬럼 포함 가정)
    """

    def __init__(self, calculation: dict):
        """클래스 초기화 및 계산 결과 저장"""
        self.calc = calculation
        
        self.Descriptive = self._Descriptive(self.calc)
        self.Transition = self._Transition(self.calc)

    class _Descriptive:
        def __init__(self, calculation):
            self.calc = calculation
            self.data = self.calc.get('data', {})

        @property
        def maximum_frequencey(self):
            """
            전체 케이스를 대상으로 가장 빈번한/드문 활동 및 길이를 분석합니다. (계산 결과를 출력)
            """
            variants_count = self.data.get('all', [])
            if not variants_count:
                print("Error: 'all' variant count data not available.")
                return

            variants_count_by_freq = sorted(variants_count, key=lambda x: x[1], reverse=False)
            variants_count_by_len = sorted(variants_count, key=lambda x: x[2], reverse=False)

            print(f"Top 1 Frequency Variant : {' → '.join(variants_count_by_freq[-1][0])}, 빈도: {variants_count_by_freq[-1][1]}회, 길이: {variants_count_by_freq[-1][2]}")
            print(f"Minimum Frequency Variant : {' → '.join(variants_count_by_freq[0][0])}, 빈도: {variants_count_by_freq[0][1]}회, 길이: {variants_count_by_freq[0][2]}")
            
            print("---")
            print(f"Maximum Length Variant : {' → '.join(variants_count_by_len[-1][0])}, 길이: {variants_count_by_len[-1][2]}, 빈도: {variants_count_by_len[-1][1]}회")
            print(f"Minimum Length Variant : {' → '.join(variants_count_by_len[0][0])}, 길이: {variants_count_by_len[0][2]}, 빈도: {variants_count_by_len[0][1]}회")


        @property
        def maximum_frequencey_per_length(self):
            """
                길이(Length) 그룹마다 가장 빈번하게 발생하는 Variant Pattern 분석 결과를 출력합니다.
            """
            list_length = [k for k in self.data.keys() if k != 'all']
            list_length.sort(key=lambda x: int(x.split('_')[-1]) if x.split('_')[-1].isdigit() else 0, reverse=False)

            for length in list_length:
                variants_count = self.data[length]
                if not variants_count: continue
                    
                variants_count.sort(key=lambda x: x[1], reverse=False)

                print(f"--- Length : {length} ---")
                print(f"Maximum Frequency Variant : {' → '.join(variants_count[-1][0])}, 빈도: {variants_count[-1][1]}회")
                print(f"Minimum Frequency Variant : {' → '.join(variants_count[0][0])}, 빈도: {variants_count[0][1]}회")
                print('='*50)           

    class _Transition:
        def __init__(self, calculation: dict):
            self.calc = calculation 
            
            self.Probability = self._Probability(self) 
            self.Frequency = self._Frequency(self) 

        def _grouped_transition_faired_set(self, grouped_transition_data):
            grouped_df_set={}
            for length, transition_data in grouped_transition_data.items():
                # Value Name은 Count 또는 Probability에 따라 동적으로 설정될 수 있으나, 
                # 현재는 하위 클래스에서 직접 처리하므로 기본값 유지
                grouped_df_set[length] = self._transition_faired_set(transition_data)
            return grouped_df_set

        def _transition_faired_set(self, transition_data):                    
            """전이 데이터를 Sankey 시각화에 적합한 DataFrame 형태로 변환합니다."""
            temp = {}
            for key1, data1 in transition_data.items():
                for key2, value in data1.items():
                    temp[(key1, key2)] = value
            
            df = pd.DataFrame([{'Source': k[0], 'Target': k[1], 'Variable': v} for k, v in temp.items()])
            
            df['Source_Num'] = df['Source'].apply(extract_stage_number) 
            df['Target_Num'] = df['Target'].apply(extract_stage_number)
            
            df_sorted = df.sort_values(by=['Source_Num', 'Target_Num'], ascending=True)
            df_preprocessing = df_sorted.drop(columns=['Source_Num', 'Target_Num'])

            return df_preprocessing

        class _Probability:
            def __init__(self, parent):
                self.all_probs = parent._transition_faired_set(parent.calc['probs'])
                self.len_probs = parent._grouped_transition_faired_set(parent.calc['length']['probs'])
                self.layer_probs = parent._transition_faired_set(parent.calc['layer']['probs'])
                self.len_layer_probs = parent._grouped_transition_faired_set(parent.calc['layer_length']['probs'])
                
            def visualizer(self, layered=True, grouped=True):
                
                if layered is True:                    
                    if grouped is True:
                        for length, df_vis in self.len_layer_probs.items():
                            sankey_visualizer(df_vis, length)
                    else :
                        length = 'Whole Data'
                        df_vis = self.layer_probs
                        sankey_visualizer(df_vis, length)

                else:
                    if grouped is True:
                        for length, df_vis in self.len_probs.items():
                            interactive_graph(df_vis)
                    else :
                        length = 'Whole Data'
                        df_vis = self.all_probs
                        interactive_graph(df_vis)


        class _Frequency:
            def __init__(self, parent):
                self.all_cnts = parent._transition_faired_set(parent.calc.get('counts', {}))
                self.len_cnts = parent._grouped_transition_faired_set(parent.calc['length']['counts'])

                self.event_log = parent.calc.get('event_log', {})
                self.grouped_event_log = parent.calc['length'].get('event_log', {})
                # self.layer_cnts = parent.calc['layer'].get('counts', {}),
                # self.len_layer_cnts = parent._grouped_transition_faired_set(parent.calc['layer_length'].get('counts', {})
                
            def visualizer(self, layered=True, grouped=True):

                from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
                from pm4py.visualization.dfg import visualizer as dfg_visualizer
                


                if layered is not None : 
                    print("Frequency Layered 기능은 불필요하여 개발하지 않았습니다")
                
                if grouped is True:
                    for length, event_log in self.grouped_event_log:
                        dfg = dfg_discovery.apply(event_log)
                        parameters = {dfg_visualizer.Variants.FREQUENCY.value.Parameters.FORMAT: "png"}
                        print(length)
                        gviz = dfg_visualizer.apply(dfg, 
                                                    log=event_log, 
                                                    variant=dfg_visualizer.Variants.FREQUENCY, 
                                                    parameters=parameters)

                        dfg_visualizer.view(gviz)
    
                else :
                    length = 'Whole Data'

                    
                    dfg = dfg_discovery.apply(self.event_log)
                    parameters = {dfg_visualizer.Variants.FREQUENCY.value.Parameters.FORMAT: "png"}
                    gviz = dfg_visualizer.apply(dfg, 
                                                log=self.event_log, 
                                                variant=dfg_visualizer.Variants.FREQUENCY, 
                                                parameters=parameters)

                    dfg_visualizer.view(gviz)

                    
                