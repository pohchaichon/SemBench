# System Performance Analysis

**Analysis Date:** 2025-11-01 20:11:00
**Total Scenarios:** 5
**Total Systems:** 4
**Total Queries:** 55

## Analysis Summary

- **Across All Scenarios**
  - **Execution Time**:
    - 1st: **palimpzest** (17.0/53 = 32.1%)
    - 2nd: **thalamusdb** (13.0/53 = 24.5%)
    - 3rd: **bigquery** (12.0/53 = 22.6%)
  - **Money Cost**:
    - 1st: **thalamusdb** (28.0/53 = 52.8%)
    - 2nd: **lotus** (12.0/53 = 22.6%)
    - 3rd: **bigquery** (7.0/53 = 13.2%)
  - **Quality**:
    - 1st: **palimpzest** (18.3/53 = 34.6%)
    - 2nd: **lotus** (15.2/53 = 28.6%)
    - 3rd: **bigquery** (11.7/53 = 22.0%)

- **By Operator Type**
  - **SEM_FILTER**:
    - **Execution Time**:
      - 1st: **palimpzest** (10.0/33 = 30.3%)
      - 2nd: **thalamusdb** (10.0/33 = 30.3%)
      - 3rd: **lotus** (7.0/33 = 21.2%)
    - **Money Cost**:
      - 1st: **thalamusdb** (22.0/33 = 66.7%)
      - 2nd: **palimpzest** (5.0/33 = 15.2%)
      - 3rd: **lotus** (4.0/33 = 12.1%)
    - **Quality**:
      - 1st: **lotus** (11.3/33 = 34.3%)
      - 2nd: **palimpzest** (11.0/33 = 33.3%)
      - 3rd: **thalamusdb** (5.8/33 = 17.7%)
  - **SEM_JOIN**:
    - **Execution Time**:
      - 1st: **bigquery** (5.0/11 = 45.5%)
      - 2nd: **thalamusdb** (3.0/11 = 27.3%)
      - 3rd: **palimpzest** (2.0/11 = 18.2%)
    - **Money Cost**:
      - 1st: **thalamusdb** (6.0/11 = 54.5%)
      - 2nd: **lotus** (4.0/11 = 36.4%)
      - 3rd: **bigquery** (1.0/11 = 9.1%)
    - **Quality**:
      - 1st: **palimpzest** (5.5/11 = 50.0%)
      - 2nd: **bigquery** (2.0/11 = 18.2%)
      - 3rd: **thalamusdb** (2.0/11 = 18.2%)
  - **SEM_MAP**:
    - **Execution Time**:
      - 1st: **palimpzest** (3.0/5 = 60.0%)
      - 2nd: **bigquery** (2.0/5 = 40.0%)
    - **Money Cost**:
      - 1st: **lotus** (2.0/5 = 40.0%)
      - 2nd: **bigquery** (2.0/5 = 40.0%)
      - 3rd: **palimpzest** (1.0/5 = 20.0%)
    - **Quality**:
      - 1st: **palimpzest** (2.3/5 = 46.7%)
      - 2nd: **bigquery** (2.3/5 = 46.7%)
      - 3rd: **lotus** (0.3/5 = 6.7%)
  - **SEM_SCORE**:
    - **Execution Time**:
      - 1st: **bigquery** (1.0/3 = 33.3%)
      - 2nd: **lotus** (1.0/3 = 33.3%)
      - 3rd: **palimpzest** (1.0/3 = 33.3%)
    - **Money Cost**:
      - 1st: **lotus** (3.0/3 = 100.0%)
    - **Quality**:
      - 1st: **lotus** (1.0/3 = 33.3%)
      - 2nd: **bigquery** (1.0/3 = 33.3%)
      - 3rd: **palimpzest** (1.0/3 = 33.3%)
  - **SEM_CLASSIFY**:
    - **Execution Time**:
      - 1st: **lotus** (3.0/7 = 42.9%)
      - 2nd: **thalamusdb** (2.0/7 = 28.6%)
      - 3rd: **bigquery** (1.0/7 = 14.3%)
    - **Money Cost**:
      - 1st: **bigquery** (3.0/7 = 42.9%)
      - 2nd: **lotus** (2.0/7 = 28.6%)
      - 3rd: **thalamusdb** (2.0/7 = 28.6%)
    - **Quality**:
      - 1st: **lotus** (3.0/7 = 42.9%)
      - 2nd: **bigquery** (2.5/7 = 35.7%)
      - 3rd: **palimpzest** (1.5/7 = 21.4%)

---

## Notes

- **Win counts** can be fractional when systems tie on a query (e.g., if 2 systems tie, each gets 0.5 wins)
- **Execution time**: Lower is better (faster systems win)
- **Money cost**: Lower is better (cheaper systems win)
- **Quality**: Higher is better (uses f1_score, transformed relative_error, or spearman_correlation)
- **Total comparisons**: 55 queries across 5 scenarios

## 1. Winners Across All Scenarios

### Execution Time

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 52/53 | 17.0/53 | 32.1% | 164.36 | 14.07 | 8711.0 |
| 2 | **thalamusdb** | 39/53 | 13.0/53 | 24.5% | 184.67 | 6.90 | 7202.1 |
| 3 | **bigquery** | 52/53 | 12.0/53 | 22.6% | 36.63 | 24.96 | 1904.9 |
| 4 | **lotus** | 41/53 | 11.0/53 | 20.8% | 111.67 | 37.03 | 4690.3 |

### Money Cost

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 39/53 | 28.0/53 | 52.8% | 0.09 | 0.02 | 3.3 |
| 2 | **lotus** | 41/53 | 12.0/53 | 22.6% | 0.27 | 0.06 | 11.5 |
| 3 | **bigquery** | 52/53 | 7.0/53 | 13.2% | 0.96 | 0.12 | 50.1 |
| 4 | **palimpzest** | 52/53 | 6.0/53 | 11.3% | 0.72 | 0.10 | 37.9 |

### Quality

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 52/53 | 18.3/53 | 34.6% | 0.725 | 0.781 | 34.79 |
| 2 | **lotus** | 41/53 | 15.2/53 | 28.6% | 0.796 | 0.830 | 31.83 |
| 3 | **bigquery** | 52/53 | 11.7/53 | 22.0% | 0.651 | 0.695 | 29.32 |
| 4 | **thalamusdb** | 39/53 | 7.8/53 | 14.8% | 0.701 | 0.750 | 20.33 |

## 2. Winners by Operator Type

### SEM_FILTER

**Execution Time** (Total: 33 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 32/33 | 10.0/33 | 30.3% | 95.35 | 13.57 | 3146.5 |
| 2 | **thalamusdb** | 30/33 | 10.0/33 | 30.3% | 144.75 | 9.02 | 4342.5 |
| 3 | **lotus** | 23/33 | 7.0/33 | 21.2% | 95.80 | 36.99 | 2203.4 |
| 4 | **bigquery** | 32/33 | 6.0/33 | 18.2% | 29.43 | 18.66 | 941.6 |

**Money Cost** (Total: 33 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 30/33 | 22.0/33 | 66.7% | 0.08 | 0.03 | 2.5 |
| 2 | **palimpzest** | 32/33 | 5.0/33 | 15.2% | 0.27 | 0.10 | 8.9 |
| 3 | **lotus** | 23/33 | 4.0/33 | 12.1% | 0.10 | 0.06 | 2.2 |
| 4 | **bigquery** | 32/33 | 2.0/33 | 6.1% | 0.71 | 0.11 | 22.8 |

**Quality** (Total: 33 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 23/33 | 11.3/33 | 34.3% | 0.836 | 0.910 | 18.39 |
| 2 | **palimpzest** | 32/33 | 11.0/33 | 33.3% | 0.717 | 0.783 | 20.79 |
| 3 | **thalamusdb** | 30/33 | 5.8/33 | 17.7% | 0.691 | 0.743 | 16.58 |
| 4 | **bigquery** | 32/33 | 4.8/33 | 14.6% | 0.600 | 0.636 | 16.81 |


### SEM_JOIN

**Execution Time** (Total: 11 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 10/11 | 5.0/11 | 45.5% | 81.08 | 67.35 | 810.8 |
| 2 | **thalamusdb** | 8/11 | 3.0/11 | 27.3% | 356.60 | 38.71 | 2852.8 |
| 3 | **palimpzest** | 10/11 | 2.0/11 | 18.2% | 535.86 | 135.08 | 5894.5 |
| 4 | **lotus** | 10/11 | 1.0/11 | 9.1% | 273.44 | 209.61 | 2734.4 |

**Money Cost** (Total: 11 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 8/11 | 6.0/11 | 54.5% | 0.11 | 0.04 | 0.8 |
| 2 | **lotus** | 10/11 | 4.0/11 | 36.4% | 0.89 | 0.89 | 8.9 |
| 3 | **bigquery** | 10/11 | 1.0/11 | 9.1% | 3.05 | 1.00 | 30.5 |
| 4 | **palimpzest** | 10/11 | 0.0/11 | 0.0% | 2.63 | 1.02 | 29.0 |

**Quality** (Total: 11 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 10/11 | 5.5/11 | 50.0% | 0.663 | 0.735 | 7.29 |
| 2 | **bigquery** | 10/11 | 2.0/11 | 18.2% | 0.624 | 0.632 | 4.37 |
| 3 | **thalamusdb** | 8/11 | 2.0/11 | 18.2% | 0.732 | 0.710 | 2.93 |
| 4 | **lotus** | 10/11 | 1.5/11 | 13.6% | 0.687 | 0.667 | 6.18 |


### SEM_MAP

**Execution Time** (Total: 5 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 5/5 | 3.0/5 | 60.0% | 342.42 | 15.87 | 1712.1 |
| 2 | **bigquery** | 5/5 | 2.0/5 | 40.0% | 23.14 | 20.11 | 115.7 |
| 3 | **lotus** | 5/5 | 0.0/5 | 0.0% | 48.42 | 11.83 | 290.5 |

**Money Cost** (Total: 5 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 5/5 | 2.0/5 | 40.0% | 0.12 | 0.11 | 0.6 |
| 2 | **lotus** | 5/5 | 2.0/5 | 40.0% | 0.17 | 0.02 | 1.0 |
| 3 | **palimpzest** | 5/5 | 1.0/5 | 20.0% | 0.28 | 0.12 | 1.4 |

**Quality** (Total: 5 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 5/5 | 2.3/5 | 46.7% | 0.827 | 0.846 | 3.31 |
| 2 | **palimpzest** | 5/5 | 2.3/5 | 46.7% | 0.810 | 0.980 | 4.05 |
| 3 | **lotus** | 5/5 | 0.3/5 | 6.7% | 0.849 | 0.965 | 4.25 |


### SEM_SCORE

**Execution Time** (Total: 3 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 3/3 | 1.0/3 | 33.3% | 56.06 | 39.06 | 168.2 |
| 2 | **lotus** | 3/3 | 1.0/3 | 33.3% | 71.98 | 37.07 | 215.9 |
| 3 | **palimpzest** | 2/3 | 1.0/3 | 33.3% | 24.98 | 24.98 | 50.0 |

**Money Cost** (Total: 3 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 3/3 | 3.0/3 | 100.0% | 0.05 | 0.02 | 0.2 |
| 2 | **bigquery** | 3/3 | 0.0/3 | 0.0% | 1.58 | 0.13 | 4.7 |
| 3 | **palimpzest** | 2/3 | 0.0/3 | 0.0% | 0.21 | 0.21 | 0.4 |

**Quality** (Total: 3 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 3/3 | 1.0/3 | 33.3% | 0.571 | 0.500 | 1.71 |
| 2 | **lotus** | 3/3 | 1.0/3 | 33.3% | 0.606 | 0.667 | 1.82 |
| 3 | **palimpzest** | 2/3 | 1.0/3 | 33.3% | 0.600 | 0.600 | 1.20 |


### SEM_CLASSIFY

**Execution Time** (Total: 7 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 5/7 | 3.0/7 | 42.9% | 26.69 | 6.33 | 133.5 |
| 2 | **thalamusdb** | 3/7 | 2.0/7 | 28.6% | 27.59 | 6.90 | 82.8 |
| 3 | **bigquery** | 7/7 | 1.0/7 | 14.3% | 28.89 | 25.30 | 202.2 |
| 4 | **palimpzest** | 7/7 | 1.0/7 | 14.3% | 100.89 | 13.57 | 706.2 |

**Money Cost** (Total: 7 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 7/7 | 3.0/7 | 42.9% | 0.57 | 0.16 | 4.0 |
| 2 | **lotus** | 5/7 | 2.0/7 | 28.6% | 0.04 | 0.02 | 0.2 |
| 3 | **thalamusdb** | 3/7 | 2.0/7 | 28.6% | 0.09 | 0.03 | 0.3 |
| 4 | **palimpzest** | 7/7 | 0.0/7 | 0.0% | 0.73 | 0.07 | 5.1 |

**Quality** (Total: 7 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 5/7 | 3.0/7 | 42.9% | 0.838 | 0.906 | 3.35 |
| 2 | **bigquery** | 7/7 | 2.5/7 | 35.7% | 0.714 | 0.756 | 5.00 |
| 3 | **palimpzest** | 7/7 | 1.5/7 | 21.4% | 0.734 | 0.861 | 5.14 |
| 4 | **thalamusdb** | 3/7 | 0.0/7 | 0.0% | 0.486 | 0.486 | 0.97 |


## 3. Winners by Scenario

### ANIMALS

**Execution Time** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 10/10 | 4.0/10 | 40.0% | 13.39 | 9.74 | 133.9 |
| 2 | **bigquery** | 10/10 | 3.0/10 | 30.0% | 14.07 | 13.26 | 140.7 |
| 3 | **palimpzest** | 10/10 | 3.0/10 | 30.0% | 13.95 | 14.03 | 139.5 |
| 4 | **lotus** | 4/10 | 0.0/10 | 0.0% | 143.29 | 106.88 | 573.2 |

**Money Cost** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 10/10 | 8.0/10 | 80.0% | 0.07 | 0.05 | 0.7 |
| 2 | **bigquery** | 10/10 | 1.0/10 | 10.0% | 0.11 | 0.11 | 1.1 |
| 3 | **palimpzest** | 10/10 | 1.0/10 | 10.0% | 0.10 | 0.13 | 1.0 |
| 4 | **lotus** | 4/10 | 0.0/10 | 0.0% | 0.14 | 0.11 | 0.6 |

**Quality** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 10/10 | 3.2/10 | 31.7% | 0.656 | 0.750 | 4.59 |
| 2 | **lotus** | 4/10 | 2.5/10 | 25.0% | 0.946 | 1.000 | 3.79 |
| 3 | **bigquery** | 10/10 | 2.2/10 | 21.7% | 0.714 | 0.750 | 5.00 |
| 4 | **palimpzest** | 10/10 | 2.2/10 | 21.7% | 0.671 | 0.750 | 4.02 |


### ECOMM

**Execution Time** (Total: 13 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **bigquery** | 12/13 | 6.0/13 | 46.2% | 53.25 | 38.68 | 639.0 |
| 2 | **palimpzest** | 12/13 | 4.0/13 | 30.8% | 310.89 | 46.71 | 4041.5 |
| 3 | **lotus** | 12/13 | 3.0/13 | 23.1% | 128.51 | 125.32 | 1542.2 |
| 4 | **thalamusdb** | 5/13 | 0.0/13 | 0.0% | 467.98 | 92.41 | 2339.9 |

**Money Cost** (Total: 13 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 12/13 | 10.0/13 | 76.9% | 0.14 | 0.03 | 1.7 |
| 2 | **thalamusdb** | 5/13 | 2.0/13 | 15.4% | 0.21 | 0.30 | 1.0 |
| 3 | **bigquery** | 12/13 | 1.0/13 | 7.7% | 2.45 | 0.36 | 29.4 |
| 4 | **palimpzest** | 12/13 | 0.0/13 | 0.0% | 0.42 | 0.24 | 5.4 |

**Quality** (Total: 13 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 12/13 | 6.3/13 | 48.7% | 0.767 | 0.863 | 9.20 |
| 2 | **bigquery** | 12/13 | 3.5/13 | 26.9% | 0.681 | 0.708 | 8.18 |
| 3 | **lotus** | 12/13 | 2.8/13 | 21.8% | 0.801 | 0.800 | 8.81 |
| 4 | **thalamusdb** | 5/13 | 0.3/13 | 2.6% | 0.724 | 0.667 | 2.17 |


### MEDICAL

**Execution Time** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 6/10 | 3.0/10 | 30.0% | 110.86 | 99.33 | 665.2 |
| 2 | **palimpzest** | 10/10 | 3.0/10 | 30.0% | 79.34 | 21.11 | 793.4 |
| 3 | **thalamusdb** | 9/10 | 3.0/10 | 30.0% | 449.98 | 39.39 | 4049.8 |
| 4 | **bigquery** | 10/10 | 1.0/10 | 10.0% | 42.06 | 29.27 | 420.6 |

**Money Cost** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 10/10 | 4.0/10 | 40.0% | 0.54 | 0.10 | 5.4 |
| 2 | **thalamusdb** | 9/10 | 4.0/10 | 40.0% | 0.16 | 0.09 | 1.5 |
| 3 | **bigquery** | 10/10 | 2.0/10 | 20.0% | 1.27 | 0.23 | 12.7 |
| 4 | **lotus** | 6/10 | 0.0/10 | 0.0% | 0.24 | 0.13 | 1.4 |

**Quality** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **lotus** | 6/10 | 5.0/10 | 50.0% | 0.642 | 0.585 | 3.85 |
| 2 | **palimpzest** | 10/10 | 3.0/10 | 30.0% | 0.585 | 0.557 | 5.85 |
| 3 | **bigquery** | 10/10 | 2.0/10 | 20.0% | 0.614 | 0.530 | 5.52 |
| 4 | **thalamusdb** | 9/10 | 0.0/10 | 0.0% | 0.560 | 0.549 | 3.36 |


### MMQA

**Execution Time** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 10/10 | 5.0/10 | 50.0% | 233.68 | 3.87 | 2336.8 |
| 2 | **lotus** | 9/10 | 2.0/10 | 20.0% | 36.58 | 3.69 | 365.8 |
| 3 | **thalamusdb** | 7/10 | 2.0/10 | 20.0% | 4.53 | 4.38 | 31.7 |
| 4 | **bigquery** | 10/10 | 1.0/10 | 10.0% | 30.34 | 14.07 | 303.4 |

**Money Cost** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 7/10 | 7.0/10 | 70.0% | 0.00 | 0.00 | 0.0 |
| 2 | **bigquery** | 10/10 | 2.0/10 | 20.0% | 0.14 | 0.01 | 1.4 |
| 3 | **palimpzest** | 10/10 | 1.0/10 | 10.0% | 1.78 | 0.02 | 17.8 |
| 4 | **lotus** | 9/10 | 0.0/10 | 0.0% | 0.19 | 0.01 | 1.9 |

**Quality** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **palimpzest** | 10/10 | 4.7/10 | 46.7% | 0.851 | 1.000 | 8.51 |
| 2 | **lotus** | 9/10 | 3.2/10 | 31.7% | 0.944 | 1.000 | 8.49 |
| 3 | **bigquery** | 10/10 | 1.3/10 | 13.3% | 0.458 | 0.615 | 3.21 |
| 4 | **thalamusdb** | 7/10 | 0.8/10 | 8.3% | 0.722 | 0.750 | 3.61 |


### MOVIE

**Execution Time** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 8/10 | 4.0/10 | 40.0% | 80.86 | 3.75 | 646.9 |
| 2 | **lotus** | 10/10 | 3.0/10 | 30.0% | 154.41 | 21.52 | 1544.1 |
| 3 | **palimpzest** | 10/10 | 2.0/10 | 20.0% | 139.98 | 5.26 | 1399.8 |
| 4 | **bigquery** | 10/10 | 1.0/10 | 10.0% | 40.13 | 19.71 | 401.3 |

**Money Cost** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 8/10 | 7.0/10 | 70.0% | 0.02 | 0.00 | 0.2 |
| 2 | **lotus** | 10/10 | 2.0/10 | 20.0% | 0.59 | 0.05 | 5.9 |
| 3 | **bigquery** | 10/10 | 1.0/10 | 10.0% | 0.55 | 0.03 | 5.5 |
| 4 | **palimpzest** | 10/10 | 0.0/10 | 0.0% | 0.82 | 0.02 | 8.2 |

**Quality** (Total: 10 queries):

| Rank | System | Coverage | Wins | Win Rate | Average | Median | Sum |
|------|--------|----------|------|----------|---------|--------|-----|
| 1 | **thalamusdb** | 8/10 | 3.5/10 | 35.0% | 0.825 | 0.841 | 6.60 |
| 2 | **bigquery** | 10/10 | 2.7/10 | 26.7% | 0.741 | 0.725 | 7.41 |
| 3 | **palimpzest** | 10/10 | 2.2/10 | 21.7% | 0.721 | 0.757 | 7.21 |
| 4 | **lotus** | 10/10 | 1.7/10 | 16.7% | 0.689 | 0.667 | 6.89 |

