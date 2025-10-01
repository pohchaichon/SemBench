# System Performance Analysis with Tolerance

**Analysis Date:** 2025-10-01 05:53:34
**Total Scenarios:** 5
**Total Systems:** 4

## Overview

This analysis examines how system performance rankings change when we allow for tolerance in measurements.
Sometimes systems may have very similar performance (e.g., 1.01s vs 1.04s latency), and strict comparisons
might not reflect practical differences.

## Tolerance Semantics

### Execution Time & Money Cost (Relative Tolerance)

Uses **relative percentage tolerance** - tolerance is a fraction of the best value.

**Formula**: A system wins if `value ≤ best_value × (1 + tolerance)`

**Examples**:
- **Best execution time**: 2.0 seconds
- **Tolerance 0% (0.0)**: Only systems with exactly 2.0s win
- **Tolerance 10% (0.1)**: Systems with ≤ 2.2s win (2.0 × 1.1)
- **Tolerance 50% (0.5)**: Systems with ≤ 3.0s win (2.0 × 1.5)
- **Tolerance 100% (1.0)**: Systems with ≤ 4.0s win (2.0 × 2.0)

**Why relative?** Performance differences are often more meaningful as percentages.
A 0.1s difference matters more when the best time is 0.5s vs. 10s.

### Quality Metrics (Absolute Tolerance)

Uses **absolute distance tolerance** - tolerance is a fixed amount.

**Formula**: A system wins if `value ≥ best_value - tolerance`

**Examples**:
- **Best quality score**: 0.85 (f1_score, transformed relative_error, or spearman_correlation)
- **Tolerance 0% (0.0)**: Only systems with exactly 0.85 win
- **Tolerance 10% (0.1)**: Systems with ≥ 0.75 win (0.85 - 0.1)
- **Tolerance 20% (0.2)**: Systems with ≥ 0.65 win (0.85 - 0.2)
- **Tolerance 100% (1.0)**: Systems with ≥ -0.15 win (effectively all systems)

**Why absolute?** Quality metrics are bounded [0,1], so absolute differences
are more interpretable than relative percentages.

## Tolerance Analysis Results

![Tolerance Analysis](tolerance_analysis_plots_first_system.png)

**Plot Legend:**
- **Solid lines with markers**: Actual wins at each tolerance level
- **Dashed horizontal lines**: Upper bound (maximum possible wins for each system)
- **Dotted vertical lines**: Convergence tolerance (minimum tolerance where system reaches upper bound)

The horizontal dashed lines represent the theoretical maximum wins each system can achieve,
which equals the total number of queries that system successfully executed. The vertical dotted
lines show the minimum tolerance level where each system reaches its upper bound for that metric.
At infinite tolerance, each system's win count would converge to this upper bound.

### Execution Time Tolerance Analysis

**Tolerance Level: 0% (Strict)**

| Rank | System | Wins | Win Rate |
|------|--------|------|----------|
| 1 | **palimpzest** | 17.0/53 | 32.1% |
| 2 | **thalamusdb** | 13.0/53 | 24.5% |
| 3 | **bigquery** | 12.0/53 | 22.6% |
| 4 | **lotus** | 11.0/53 | 20.8% |

**Tolerance Level: 200%**

| Rank | System | Wins | Win Rate |
|------|--------|------|----------|
| 1 | **palimpzest** | 41.0/124 | 33.1% |
| 2 | **thalamusdb** | 32.0/124 | 25.8% |
| 3 | **bigquery** | 27.0/124 | 21.8% |
| 4 | **lotus** | 24.0/124 | 19.4% |


### Money Cost Tolerance Analysis

**Tolerance Level: 0% (Strict)**

| Rank | System | Wins | Win Rate |
|------|--------|------|----------|
| 1 | **thalamusdb** | 28.0/53 | 52.8% |
| 2 | **lotus** | 12.0/53 | 22.6% |
| 3 | **bigquery** | 7.0/53 | 13.2% |
| 4 | **palimpzest** | 6.0/53 | 11.3% |

**Tolerance Level: 200%**

| Rank | System | Wins | Win Rate |
|------|--------|------|----------|
| 1 | **thalamusdb** | 34.0/111 | 30.6% |
| 2 | **bigquery** | 27.0/111 | 24.3% |
| 3 | **lotus** | 26.0/111 | 23.4% |
| 4 | **palimpzest** | 24.0/111 | 21.6% |


### Quality Tolerance Analysis

**Tolerance Level: 0% (Strict)**

| Rank | System | Wins | Win Rate |
|------|--------|------|----------|
| 1 | **palimpzest** | 27.0/79 | 34.2% |
| 2 | **lotus** | 22.0/79 | 27.8% |
| 3 | **bigquery** | 17.0/79 | 21.5% |
| 4 | **thalamusdb** | 13.0/79 | 16.5% |


## Key Insights

- At **0% tolerance (strict comparison)**, only the exactly best-performing system wins each query
- As **tolerance increases**, more systems are considered winners for queries where performance is similar
- **Win counts should increase** (or stay constant) as tolerance increases - systems don't lose wins, only gain them
- **Convergence point**: At high tolerance levels, all systems become winners for most queries
- **Relative tolerance** for time/cost metrics accounts for proportional differences
- **Absolute tolerance** for quality metrics accounts for bounded 0-1 range

## Notes

- Win counts are whole numbers - each system gets 1 win per query where it meets the tolerance threshold
- Execution time: Lower is better
- Money cost: Lower is better
- Quality: Higher is better (f1_score, transformed relative_error, or spearman_correlation)
- The 'snowflake' system has been excluded from this analysis
