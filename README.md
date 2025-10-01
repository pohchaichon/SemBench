# SemBench: Benchmarking Semantic Query Processing Engines (VLDB 2026 Submission)

SemBench is a systematic benchmark designed to evaluate and compare multi-modal data systems in realistic settings. It includes well-defined use cases that integrate structured data (tables) with unstructured modalities (text, images, audio), enabling the assessment of systems' ability to process complex semantic queries with ground truth validation. These systems are expected to balance multiple objectives—accuracy, cost, and efficiency—especially when handling large-scale datasets. SemBench emphasizes the trade-offs among these dimensions rather than performance in isolation.

## 🌟 Overview

Modern data systems increasingly need to process and reason over multi-modal data - combining traditional relational data with images, audio, and text. SemBench provides a standardized evaluation framework with:

- **5 Real-world Use Cases**: wildlife monitoring, medical diagnosis, sentiment analysis, question anwering, product analysis
- **Multi-modal Queries**: Complex semantic operations across multi-modal databases: table, text, image, and audio  
- **System-agnostic Design**: Extensible and already supports LOTUS, Palimpzest, ThalamusDB, CAESURA, BigQuery, Snowflake, DuckDB FlockMTL
- **Comprehensive Metrics**: Quality (precision/recall/F1, relative error...), cost (money, token consumption), and performance evaluation (execution time)
- **Rich Visualizations**: Automated generation of performance comparisons

## 📁 Architecture

```
SemBench/
├── files/              # Use case data and queries
│   ├── {use_case}/
│   │   ├── data/           # Multi-modal datasets
│   │   ├── query/          # Natural language & SQL queries  
│   │   ├── metrics/        # System performance results
│   │   └── raw_results/    # Query execution outputs
├── figures/            # Performance visualizations  
├── src/                # Core implementation
│   ├── runner/            # System-specific implementations
│   ├── scenario/          # Use case logic & evaluation
│   ├── evaluator/         # Quality assessment framework
│   └── run.py            # Main benchmark orchestrator
└── README.md
```

### Core Components

- **🎯 Runner Framework**: Modular system implementations with standardized interfaces
- **📊 Evaluation Engine**: Automated quality assessment with precision/recall/F1 metrics  
- **🔄 Query Processing**: Support for natural language and SQL query formats
- **📈 Visualization Pipeline**: Automated generation of cost/quality/performance charts

## 🚀 Supported Use Cases

### 1. **Animals** - Wildlife Monitoring
*Data Modalities: Tables, Images, Audio*

Studies animal species detection and co-occurrence using camera traps and audio recorders. Features queries about species identification, location-based analysis, and cross-modal correlation between visual and audio evidence.

**Key Features**: Image-based species recognition, audio call classification, spatial-temporal analysis
**Queries**: 10 queries ranging from simple counts to complex multi-way joins
→ *[Detailed documentation](src/scenario/animals/README.md)*

### 2. **Medical** - Electronic Health Records
*Data Modalities: Tables, Text, Images, Audio*  

Comprehensive EHR analysis combining patient demographics, symptom descriptions, chest X-rays, and lung sound recordings. Evaluates disease diagnosis and multi-modal health assessment capabilities.

**Key Features**: Disease detection from multiple modalities, co-occurrence analysis, health status correlation
**Queries**: 7 queries focusing on diagnostic accuracy and patient profiling
→ *[Detailed documentation](src/scenario/medical/README.md)*

### 3. **Movie** - Sentiment Analysis
*Data Modalities: Tables, Text*

Movie review sentiment analysis testing systems' understanding of emotional tone in textual content. Includes sentiment classification, comparison, and aggregation operations.

**Key Features**: Sentiment classification, review comparison, positivity ratio calculation  
**Queries**: 7 queries spanning filtering, joins, and aggregation operations
→ *[Detailed documentation](src/scenario/movie/README.md)*

### 4. **MMQA** - Multi-Modal Question Answering
*Data Modalities: Tables, Text, Images*

Based on the standard MultiModalQA dataset, testing question-answering capabilities across combined textual and visual information sources.

**Key Features**: Cross-modal question answering, information synthesis, knowledge reasoning

### 5. **Product** - Multi-Modal Amazon Fashion Product Analysis
*Data Modalities: Tables, Text, Images*

Based on the amazon fashion product dataset, making analysis over multi-modal product information

**Key Features**: Cross-modal product information analysis

## 🔧 System Support

SemBench supports evaluation of multiple multi-modal data systems:

- **LOTUS**: Semantic operators optimized by reducing costs with guaranteed accuracy
- **Palimpzest**: Semantic operators using cost-based optimization
- **ThalamusDB**: Semantic operators optimized with approximate query processing
- **CAESURA**: LLM-Based Multi-Modal Query Planner
- **FlockMTL**: An open-source extension of DuckDB   
- **BigQuery**: Google's analytics data warehouse

Each system implements a standardized runner interface enabling fair comparison across different architectural approaches.

## ⚡ Quick Start

### Prerequisites
```bash
# Copy environment template and add your API keys
cp .env.example .env
# Edit .env with your OpenAI credentials

# Recommand using Miniconda to manage the Python environment
conda create -n sembench python=3.10
conda activate sembench

# Install dependencies
pip install -r requirements.txt

# Use SemBench as an editable package
conda install conda-build
conda develop <path to the SemBench directory, e.g., ~/Desktop/MMBench-System>
```

### Running Benchmarks
```bash
# Run specific system on specific use case and queries
python3 src/run.py --systems lotus --use-cases movie --queries 1 3 --model gemini-2.5-flash

# Run full evaluation on a use case  
python3 src/run.py --systems lotus --use-cases movie

# Compare multiple systems
python3 src/run.py --systems lotus thalamusdb --use-cases movie

# Generate performance visualizations
python3 src/plot.py
```

### Output Structure
Results are organized as:
- **Query Results**: `files/{use_case}/raw_results/{system}/Q{n}.csv`
- **Performance Metrics**: `files/{use_case}/metrics/{system}.json`  
- **Visualizations**: `figures/{use_case}/{metric}.png`

Performance metrics include execution time, token usage, monetary cost, and quality scores (precision, recall, F1).

## 📊 Evaluation Framework

The benchmark provides comprehensive evaluation across multiple dimensions:

- **Quality Assessment**: Precision, recall, and F1-score for retrieval queries; relative error for aggregation queries
- **Performance Metrics**: Query execution time 
- **Cost Analysis**: Token usage and monetary cost tracking
- **Comparative Analysis**: Automated generation of Pareto frontier charts

## 🎨 Visualization

Automated generation of comparative charts including:
- **Quality vs Cost**: Pareto frontier analysis
- **Execution Time**: Performance comparison across systems
- **Cost Breakdown**: Token usage and monetary cost analysis
- **Quality Metrics**: Precision, recall, and F1-score comparisons

## 🏗️ Extending the Benchmark

The modular architecture supports easy extension:

1. **Add New Use Cases**: Implement scenario-specific runner and evaluator
2. **Support New Systems**: Create system-specific runner inheriting from `GenericRunner`  
3. **Custom Metrics**: Extend evaluation framework with domain-specific quality measures
4. **Additional Queries**: Add query definitions in natural language and SQL formats


## 🤝 Contributing

We welcome contributions! Please see our [contribution guidelines](CONTRIBUTING.md) for details on submitting pull requests, reporting issues, and extending the benchmark.

---

*SemBench enables systematic evaluation of multi-modal data systems across diverse, realistic scenarios. Built for researchers and practitioners working at the intersection of databases, AI, and multi-modal data processing.*
