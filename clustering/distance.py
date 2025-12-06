import numpy as np
from mining.probability import prepare_eventLog
from mining.probability import create_eventlog_from_dataFrame

from sklearn.cluster import AgglomerativeClustering
from pm4py.algo.filtering.log.variants.variants_filter import get_variants
from rapidfuzz.distance import Levenshtein


class ClusteredTraces:
    
    def __init__(self, dataframe):
        self.dataframe = dataframe
        
        self.event_log = self.preprocessing()
        self.sequences = self.achieve_trace_infomation()[1]
        self.matrix = self.calculate_distance_matrix()
        self.n_clusters = 0

    def preprocessing(self):
        prepared_df = prepare_eventLog(self.dataframe)
        eventlog_df = create_eventlog_from_dataFrame(prepared_df)        
        return eventlog_df

    def achieve_trace_infomation(self):
        """
                Description : Eventlog의 Vriants Pattern과 Freq, Length을 저장하기 위한 함수
        """
        variants = get_variants(self.event_log)

        traces = [variants[v][0] for v in variants]
        trace_sequences = [ [event['concept:name'] for event in trace] for trace in traces ]
        trace_labels = [f"T{str(i).zfill(2)}" for i in range(len(trace_sequences))]
        
        return (traces, trace_sequences, trace_labels)


    def calculate_distance_matrix(self):
        n = len(self.sequences)
        distance_matrix = np.zeros((n, n))

        for i in range(n):
            for j in range(i + 1, n):
                dist = Levenshtein.distance(
                    " ".join(self.sequences[i]), 
                    " ".join(self.sequences[j])
                )
                distance_matrix[i, j] = dist
                distance_matrix[j, i] = dist

        return distance_matrix

    @property
    def clusetering_agglomerative(self):
        # Clustering Model 
        clustering_model = AgglomerativeClustering( n_clusters=self.n_clusters, metric='precomputed', linkage='complete')
        clusters = clustering_model.fit_predict(self.matrix)
        return clusters

    def __call__(self, n_clusters=None):
        if n_clusters is None:
            print("Error : 군집의 개수를 정해주세요!,  'n_clusters' argument is empty")
        self.n_clusters = n_clusters
        
        result = {}
        result['traces'], result['sequences'], result['labels'] = self.achieve_trace_infomation()
        result['distances'] = self.matrix
        result['clusters'] = self.clusetering_agglomerative
        result['n_clusters'] = self.n_clusters

        return result