import matplotlib.pyplot as plt
import numpy as np
from sklearn.manifold import MDS as multidimensional_scaling
from scipy.cluster.hierarchy import dendrogram, linkage


def MDS_coords(matrix):
    mds = multidimensional_scaling(
        n_components=2,
        dissimilarity='precomputed',
        random_state=42
    )
    mds_coords = mds.fit_transform(matrix)
    return mds_coords


def MDS(clustered_result):
    """
        2-Axis Coordination위에 다차원척도법으로 시각화하는 함수
    """
    matrix = clustered_result['distances']
    trace_labels = clustered_result['labels']
    clusters = clustered_result['clusters']
    mds_coords = MDS_coords(matrix)

    plt.figure(figsize=(10, 8))

    scatter = plt.scatter(
        mds_coords[:, 0],
        mds_coords[:, 1],
        c=clusters,
        cmap='tab10',
        s=100,
        edgecolor='black'
    )

    # 각 점 위에 T00 등 라벨 추가
    for i, label in enumerate(trace_labels):
        plt.text(
            mds_coords[i, 0] + 0.02,
            mds_coords[i, 1] + 0.02,
            label,
            fontsize=10,
            fontweight='bold'
        )

    plt.title("MDS 2D Visualization of Trace Clustering", fontsize=14)
    plt.xlabel("MDS Dim 1")
    plt.ylabel("MDS Dim 2")
    plt.grid(alpha=0.4)
    plt.colorbar(scatter, label="Cluster Label")
    plt.show()


def Dendrogram(clustered_result):
    matrix = clustered_result['distances']
    trace_labels = clustered_result['labels']
    trace_sequences = clustered_result['sequences']
    clusters = clustered_result['clusters']
    n_clusters = clustered_result['n_clusters']

    linked = linkage(matrix, method='complete')

    plt.figure(figsize=(14, 7))

    dendrogram(
        linked,
        orientation='top',
        labels=trace_labels,
        distance_sort='descending',
        show_leaf_counts=True,
        leaf_font_size=9,
        color_threshold=0
    )

    plt.title("Hierarchical Clustering Dendrogram (Complete Linkage)", fontsize=14)
    plt.xlabel("Trace Label")
    plt.ylabel("Distance")
    plt.grid(axis='y', linestyle='--', alpha=0.4)
    plt.show()

    cluster_dict = {i: [] for i in range(n_clusters)}

    for idx, c in enumerate(clusters):
        cluster_dict[c].append((trace_labels[idx], trace_sequences[idx]))

    print("\n===== Cluster Composition =====")
    for c in cluster_dict:
        print(f"\n### Cluster {c}")
        for label, seq in cluster_dict[c]:
            print(f" {label} : " + " → ".join(seq))




