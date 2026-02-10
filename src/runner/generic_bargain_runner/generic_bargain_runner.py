"""
Created on February 10, 2026

@author: Cline (AI Assistant)

BARGAIN system runner implementation based on generic_runner.
BARGAIN: Guaranteed Accurate AI for Less - A cascade system that uses
proxy (cheap) and oracle (expensive) models to reduce costs while
maintaining accuracy guarantees.
"""

import json
import re
import time
import traceback
from overrides import override
from typing import List, Optional, Tuple, Any
import sys
import os

import pandas as pd

# BARGAIN is already installed in the environment
from BARGAIN import BARGAIN_A, VLLMProxy, VLLMOracle

from runner.generic_runner import GenericRunner, GenericQueryMetric


class GenericBargainRunner(GenericRunner):
    """GenericRunner for BARGAIN system."""

    def __init__(
        self,
        use_case: str,
        scale_factor: int,
        model_name: str = "gpt-4o-mini",  # Default proxy model
        concurrent_llm_worker=20,
        skip_setup: bool = False,
        config_file: Optional[str] = None,
        accuracy_target: float = 0.8,  # User specified 0.8 accuracy target
        delta: float = 0.1,  # Default delta for statistical guarantees
    ):
        """
        Initialize BARGAIN runner.

        Args:
            use_case: The use case to run
            model_name: LLM model to use for proxy (cheap model)
            concurrent_llm_worker: Number of concurrent workers
            skip_setup: Whether to skip scenario setup
            config_file: Optional path to JSON configuration file
            accuracy_target: Accuracy target for BARGAIN (default: 0.8)
            delta: Statistical confidence parameter (default: 0.1)
        """
        super().__init__(
            use_case,
            scale_factor,
            model_name,
            concurrent_llm_worker,
            skip_setup,
        )
        
        # BARGAIN-specific configuration
        self.accuracy_target = accuracy_target
        self.delta = delta
        
        # Check for environment variable for evaluation purposes
        env_config_file = os.getenv("BARGAIN_CONFIG_FILE")
        self.config_file = config_file or env_config_file
        self.config_data = self._load_config() if self.config_file else None
        
        # Initialize proxy and oracle models
        self.proxy_model = None
        self.oracle_model = None
        self.bargain_instance = None
        
        # Track token usage for cost calculation
        self.proxy_token_usage = 0
        self.oracle_token_usage = 0
        self.total_token_usage = 0

    @override
    def get_system_name(self) -> str:
        return "bargain"

    def _load_config(self) -> dict:
        """
        Load configuration from JSON file.

        Returns:
            Configuration dictionary
        """
        if not self.config_file:
            return {}

        from pathlib import Path

        config_path = (
            Path(__file__).parent.parent.parent.parent
            / "config"
            / "system"
            / "bargain"
            / self.config_file
        )

        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )

        with open(config_path, "r") as f:
            return json.load(f)

    def _initialize_models(self, task_template: str, is_binary: bool = False):
        """
        Initialize proxy and oracle models for a specific task.
        
        Args:
            task_template: The task template/prompt for the LLM
            is_binary: Whether the task is binary classification
        """
        # Use configuration if available, otherwise use defaults
        if self.config_data:
            proxy_model_name = self.config_data.get("proxy_model", "google/gemma-3-4b-it")
            oracle_model_name = self.config_data.get("oracle_model", "google/gemma-3-27b-it")
            is_binary = self.config_data.get("is_binary", is_binary)
        else:
            # Default configuration: use Gemma models
            proxy_model_name = "google/gemma-3-4b-it"
            oracle_model_name = "google/gemma-3-27b-it"
        
        # Initialize VLLM-based proxy and oracle
        # Use VLLMProxy on port 8001 and VLLMOracle on port 8000
        self.proxy_model = VLLMProxy(
            task_template,
            model=proxy_model_name,
            is_binary=is_binary,
            base_url="http://localhost:8001/v1",
            api_key=None,
            verbose=True
        )
        self.oracle_model = VLLMOracle(
            task_template,
            model=oracle_model_name,
            is_binary=is_binary,
            base_url="http://localhost:8000/v1",
            api_key=None,
            verbose=True
        )
        
        # Create BARGAIN instance
        self.bargain_instance = BARGAIN_A(
            self.proxy_model,
            self.oracle_model,
            target=self.accuracy_target,
            delta=self.delta,
            seed=42  # Fixed seed for reproducibility
        )

    def execute_query(self, query_id: int) -> GenericQueryMetric:
        """
        Execute a specific query using BARGAIN and return metric with results.

        Args:
            query_id: ID of the query (e.g., 1 for Q1, 5 for Q5)

        Returns:
            QueryMetric object containing results DataFrame and metrics
        """
        # Create appropriate metric object
        metric = GenericQueryMetric(query_id=query_id, status="pending")

        try:
            # Get query implementation
            query_fn = self._discover_query_impl(query_id)
            
            # Reset token usage tracking
            self.proxy_token_usage = 0
            self.oracle_token_usage = 0
            self.total_token_usage = 0
            
            # Execute query
            start_time = time.time()
            results = query_fn()
            execution_time = time.time() - start_time

            # Store results in metric
            metric.execution_time = execution_time
            metric.status = "success"
            
            # Handle different return types
            if isinstance(results, dict):
                # If query returns a dict with results and stats
                metric.results = results.get("results", pd.DataFrame())
                stats = results.get("execution_stats", {})
                self._update_token_usage(metric, stats)
            elif isinstance(results, pd.DataFrame):
                metric.results = results
                # Estimate token usage if not provided
                self._estimate_token_usage(metric, results)
            else:
                # Try to convert to DataFrame
                try:
                    metric.results = pd.DataFrame(results)
                    self._estimate_token_usage(metric, metric.results)
                except Exception as e:
                    print(f"Warning: Could not convert results to DataFrame: {e}")
                    metric.results = self._get_empty_results_dataframe(query_id)

        except Exception as e:
            # Handle failure
            metric.status = "failed"
            metric.error = str(e)
            metric.results = self._get_empty_results_dataframe(query_id)
            print(f"  Error in Q{query_id} execution: {type(e).__name__}: {e}")
            traceback.print_exc()
            raise

        return metric

    def _update_token_usage(self, metric: GenericQueryMetric, stats: dict):
        """Update metric with token usage and cost information from stats."""
        try:
            # Extract token usage from stats
            # Note: BARGAIN doesn't directly provide token usage in its API
            # We need to track this separately or estimate
            proxy_tokens = stats.get("proxy_tokens", 0)
            oracle_tokens = stats.get("oracle_tokens", 0)
            total_tokens = proxy_tokens + oracle_tokens
            
            metric.token_usage = total_tokens
            
            # Calculate cost based on model pricing
            # Default pricing (per 1M tokens)
            proxy_price_per_million = 0.15  # gpt-4o-mini approximate
            oracle_price_per_million = 2.5   # gpt-4o approximate
            
            proxy_cost = (proxy_tokens / 1_000_000) * proxy_price_per_million
            oracle_cost = (oracle_tokens / 1_000_000) * oracle_price_per_million
            total_cost = proxy_cost + oracle_cost
            
            metric.money_cost = total_cost
            
            # Print usage for debugging
            print(
                f"  Token usage: {total_tokens} tokens (proxy: {proxy_tokens}, oracle: {oracle_tokens}), "
                f"Cost: ${total_cost:.4f}"
            )

        except Exception as e:
            print(f"  Warning: Could not get token usage from stats: {e}")
            self._estimate_token_usage(metric, metric.results)

    def _estimate_token_usage(self, metric: GenericQueryMetric, results: pd.DataFrame):
        """
        Estimate token usage based on results size.
        This is a rough estimation since BARGAIN doesn't provide direct token counts.
        
        Args:
            metric: The query metric to update
            results: Results DataFrame
        """
        try:
            # Very rough estimation: ~4 tokens per word, average 10 words per cell
            total_cells = results.size
            estimated_tokens = total_cells * 10 * 4  # Very rough estimate
            
            # Assume 70% proxy usage, 30% oracle usage (typical for 0.8 accuracy target)
            proxy_tokens = int(estimated_tokens * 0.7)
            oracle_tokens = int(estimated_tokens * 0.3)
            total_tokens = proxy_tokens + oracle_tokens
            
            metric.token_usage = total_tokens
            
            # Calculate estimated cost
            proxy_price_per_million = 0.15  # gpt-4o-mini approximate
            oracle_price_per_million = 2.5   # gpt-4o approximate
            
            proxy_cost = (proxy_tokens / 1_000_000) * proxy_price_per_million
            oracle_cost = (oracle_tokens / 1_000_000) * oracle_price_per_million
            total_cost = proxy_cost + oracle_cost
            
            metric.money_cost = total_cost
            
            print(
                f"  Estimated token usage: {total_tokens} tokens, "
                f"Estimated cost: ${total_cost:.4f}"
            )

        except Exception as e:
            print(f"  Warning: Could not estimate token usage: {e}")
            metric.token_usage = 0
            metric.money_cost = 0.0

    def _get_empty_results_dataframe(self, query_id: int) -> pd.DataFrame:
        """
        Get empty DataFrame with correct columns for a query.
        This should be overridden in scenario-specific implementations.
        
        Args:
            query_id: ID of the query

        Returns:
            Empty DataFrame with correct columns
        """
        # Default empty DataFrame
        return pd.DataFrame()

    def _discover_queries(self) -> List[int]:
        """
        Discover available queries for BARGAIN.

        Any method named ``_execute_q<i>`` (where <i> is an integer ≥1) is
        treated as an implemented query. The function returns the list of
        those integer IDs in ascending order.

        Returns:
            List of available query IDs
        """
        pattern = re.compile(r"_execute_q(\d+)$")
        query_ids: List[int] = []

        # `dir(self)` lists all attribute names visible on the instance;
        # we then pick out callables whose names match our pattern.
        for attr_name in dir(self):
            match = pattern.match(attr_name)
            if match:
                attr = getattr(self, attr_name, None)
                if callable(attr):
                    query_ids.append(int(match.group(1)))

        return sorted(query_ids)

    def process_with_bargain(self, data_records: List[str], task_template: str, 
                           is_binary: bool = False) -> List[Any]:
        """
        Process data records using BARGAIN.
        
        Args:
            data_records: List of data records (strings) to process
            task_template: Task template/prompt for the LLM
            is_binary: Whether the task is binary classification
            
        Returns:
            List of processed results
        """
        # Initialize models for this task
        self._initialize_models(task_template, is_binary)
        
        # Process data with BARGAIN
        results = self.bargain_instance.process(data_records)
        
        # Track token usage (this would need to be extracted from BARGAIN)
        # For now, we'll estimate based on the number of records
        self.proxy_token_usage += len(data_records) * 100  # Rough estimate
        self.oracle_token_usage += 0  # BARGAIN would provide actual oracle calls
        
        return results
