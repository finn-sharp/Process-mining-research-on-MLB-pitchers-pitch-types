# MLB 투수 구종 패턴 분석 프로젝트 요약

## **1. 연구 개요 및 목표**

- **주제:** Levenshtein 거리를 이용한 MLB 투수의 구종 시퀀스 패턴 분석 및 계층적 군집화.
- **목표:** 프로세스 마이닝 기법을 활용하여 투수의 구종 선택 전략을 **구조적으로 정량화**하고, 유사한 전략 패턴을 **군집화**하여 핵심 전략과 예외 전략을 식별합니다.
<br>

## **2. 프로젝트 참여자 (Contributors)**

| **사진 이름 (Photo Filename)** | **역할 (Role)** | **담당자 (Name)** |
| --- | --- | --- |
| **[파일 이름 명시]** | **프로젝트 리드 및 기획** (Project Lead & Planning), **시각화 및 결과 해석** (Visualization & Interpretation) | (담당자 이름 명시) |
| **[파일 이름 명시]** | **데이터 엔지니어링 및 전처리** (Data Engineering & Preprocessing) | (담당자 이름 명시) |
| **[파일 이름 명시]** | **군집 분석 및 알고리즘 구현** (Clustering Analysis & Algorithm Implementation) | (담당자 이름 명시) |
<br>

### 2.1 프로젝트 초기 설정 및 환경 세팅 가이드

이 프로젝트는 **프로세스 마이닝(Process Mining)** 분석을 위해 특정 Python 패키지와 모듈 구조를 필요로 합니다. 아래 가이드를 따라 환경을 설정하면 즉시 실험을 시작할 수 있습니다.
<br>

## **3. 프로젝트 디렉토리 구조 (Tree Structure)**

프로젝트의 논리적 구성 요소를 명확히 파악할 수 있도록 트리 형태로 디렉토리 구조를 시각화했습니다.

```
.
├── cap/                      # [1] 가상 환경 폴더 (Project Virtual Environment)
├── clustering/               # [2] 군집 분석 모듈
│   ├── __init__.py
│   ├── distance.py           # - Levenshtein 거리 계산, ClusteredTraces 클래스
│   └── visualizer.py         # - MDS, Dendrogram 시각화 기능
├── lib/                      # [3] 라이브러리 및 공통 데이터 저장소 (Common Libs/Data)
├── mining/                   # [4] 프로세스 마이닝 분석 모듈 (Core Logic)
│   ├── __init__.py
│   ├── utils.py              # - load_data_from_bigquery 등 유틸리티 함수
│   ├── preprocessing.py      # - 전처리, 필터링, 노드 추가 함수
│   ├── probability.py        # - BasedTraces 등 확률 기반 계산 모듈
│   └── exploratory.py        # - ProcessEDA 등 탐색적 분석 모듈
├── .git/
├── .gitignore                # Git 추적 제외 파일 명시 (key.json, cap 등)
├── key.json                  # BigQuery 접근 인증 파일
├── README.md
├── requirements.txt          # 패키지 의존성 목록
└── inference.ipynb           # [5] 메인 실험 및 분석 노트북
```
<br>

 #### 3.1 핵심 디렉토리 상세 설명

| **번호** | **디렉토리** | **역할 및 포함 내용** |
| --- | --- | --- |
| **[1]** | **`cap`** (`venv`) | **가상 환경:** 프로젝트의 Python 인터프리터와 설치된 의존성 패키지(`pm4py`, `pandas`, `Levenshtein` 등)를 격리 보관합니다. |
| **[2]** | **`clustering`** | **군집화 모듈:** 구종 시퀀스 간의 **구조적 유사성(Levenshtein Distance)**을 계산하고, 이를 기반으로 **계층적 군집화 및 MDS 시각화**를 수행하는 모듈이 포함됩니다. |
| **[3]** | **`lib`** | **공용 자원:** 프로젝트 전반에 걸쳐 사용되는 재사용 가능한 라이브러리 파일이나 공통 데이터셋을 저장합니다. |
| **[4]** | **`mining`** | **프로세스 마이닝 핵심 로직:** 데이터 로드, **프로세스 이벤트 로그 변환**, EDA, **전이 확률 계산** 등 프로세스 마이닝을 위한 기본 분석 기능이 정의되어 있습니다. |
| **[5]** | **`inference.ipynb`** | **실험 코드:** 데이터를 로드하고 전처리한 후, `mining` 모듈로 EDA를 수행하고 `clustering` 모듈로 패턴 분류를 수행하는 **전체 연구 흐름이 순서대로 기록**된 메인 노트북입니다. |
<br>

## **4. 초기 환경 설정 (Quick Start)**

### **4.1. 가상 환경 설정 및 활성화**

프로젝트의 의존성 충돌을 방지하기 위해 가상 환경을 사용합니다.

**4.1.1 프로젝트 루트 디렉토리로 이동합니다. (Process-Mining-Analysis-on-MLB)**
```Bash
cd Process-Mining-Analysis-on-MLB
```

**4.1.2. 가상 환경 생성 (예: 'venv'라는 이름으로 생성)**
```Bash
python3 -m venv venv
```

**4.1.3. 가상 환경 활성화**
- **macOS/Linux**
```Bash
source venv/bin/activate
```

- **Windows (PowerShell)**

```Bash
.\venv\Scripts\Activate
```
<br>

### **4.2. 의존성 패키지 설치**

프로젝트에 필요한 모든 라이브러리를 `requirements.txt` 파일을 통해 설치합니다.


```Bash
pip install -r requirements.txt
```
<br>

### **4.3. 데이터 접근 설정**

Google Cloud BigQuery를 사용하므로, 프로젝트 루트 디렉토리에 **`key.json`** 파일을 배치하여 데이터베이스 접근 권한을 설정해야 합니다.

