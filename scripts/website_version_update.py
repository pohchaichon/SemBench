#!/usr/bin/env python3
"""
Website Version Update Script

This script copies data and figures from the main files/ and figures/ directories
to versioned folders in docs/static/ for the SemBench website version control.

Usage:
    python scripts/website_version_update.py <version_name>

Example:
    python scripts/website_version_update.py paper-2025-10-01
"""

import os
import sys
import shutil
import json
import argparse
from datetime import datetime
from pathlib import Path


def validate_version_name(version_name):
    """Validate version name format and characters."""
    if not version_name:
        raise ValueError("Version name cannot be empty")

    # Check for invalid characters
    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', ' ']
    for char in invalid_chars:
        if char in version_name:
            raise ValueError(f"Version name contains invalid character: '{char}'")

    return True


def copy_directory_contents(src_dir, dst_dir, description="", overwrite=True):
    """Copy directory contents with error handling."""
    if not os.path.exists(src_dir):
        print(f"Warning: Source directory does not exist: {src_dir}")
        return False

    print(f"Copying {description} from {src_dir} to {dst_dir}")

    # Remove destination directory if it exists and overwrite is True
    if overwrite and os.path.exists(dst_dir):
        print(f"  Removing existing directory: {dst_dir}")
        shutil.rmtree(dst_dir)

    # Create destination directory
    os.makedirs(dst_dir, exist_ok=True)

    # Copy all contents
    try:
        for item in os.listdir(src_dir):
            src_item = os.path.join(src_dir, item)
            dst_item = os.path.join(dst_dir, item)

            if os.path.isdir(src_item):
                shutil.copytree(src_item, dst_item, dirs_exist_ok=True)
            else:
                shutil.copy2(src_item, dst_item)

        print(f"✓ Successfully copied {description}")
        return True
    except Exception as e:
        print(f"✗ Error copying {description}: {e}")
        return False


def update_versions_metadata(docs_static_dir, version_name, description=""):
    """Update the versions.json metadata file."""
    versions_file = os.path.join(docs_static_dir, "versions.json")

    # Load existing versions or create new structure
    if os.path.exists(versions_file):
        with open(versions_file, 'r') as f:
            versions_data = json.load(f)
    else:
        versions_data = {
            "versions": [],
            "default_version": None
        }

    # Check if version already exists
    existing_version = next((v for v in versions_data["versions"] if v["name"] == version_name), None)

    if existing_version:
        print(f"Version '{version_name}' already exists, overwriting data and updating timestamp...")
        existing_version["updated_at"] = datetime.now().isoformat()
        if description:
            existing_version["description"] = description
    else:
        # Add new version
        new_version = {
            "name": version_name,
            "description": description or f"Version {version_name}",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        versions_data["versions"].append(new_version)
        print(f"Added new version: {version_name}")

    # Set as default if it's the first version
    if not versions_data["default_version"]:
        versions_data["default_version"] = version_name
        print(f"Set '{version_name}' as default version")

    # Sort versions by creation date (newest first)
    versions_data["versions"].sort(key=lambda x: x["created_at"], reverse=True)

    # Save updated metadata
    with open(versions_file, 'w') as f:
        json.dump(versions_data, f, indent=2)

    print(f"✓ Updated versions metadata in {versions_file}")


def main():
    parser = argparse.ArgumentParser(description="Update SemBench website with versioned data and figures")
    parser.add_argument("version_name", help="Version name (e.g., paper-2025-10-01)")
    parser.add_argument("--description", "-d", default="", help="Version description")
    parser.add_argument("--scenarios", nargs="+", default=None,
                       help="Specific scenarios to copy (default: all available)")
    parser.add_argument("--analysis-results", default="academic_bigquery_snowflake_3scenarios",
                       help="Analysis results directory to use for tolerance analysis plots (default: academic_bigquery_snowflake_3scenarios)")

    args = parser.parse_args()

    # Validate version name
    try:
        validate_version_name(args.version_name)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Define paths
    project_root = Path(__file__).parent.parent
    files_dir = project_root / "files"
    figures_dir = project_root / "figures"
    analysis_results_dir = project_root / "analysis_results" / args.analysis_results
    docs_static_dir = project_root / "docs" / "static"

    # Create main static directories if they don't exist
    (docs_static_dir / "data").mkdir(exist_ok=True)
    (docs_static_dir / "figures").mkdir(exist_ok=True)

    # Define version directories
    version_data_dir = docs_static_dir / "data" / args.version_name
    version_figures_dir = docs_static_dir / "figures" / args.version_name

    print(f"Creating version: {args.version_name}")
    print(f"Description: {args.description or 'No description provided'}")
    print("-" * 50)

    # Get available scenarios
    if args.scenarios:
        scenarios = args.scenarios
    else:
        scenarios = [d for d in os.listdir(files_dir) if os.path.isdir(os.path.join(files_dir, d))]

    print(f"Processing scenarios: {', '.join(scenarios)}")
    print("-" * 50)

    success_count = 0
    total_operations = 0

    # Copy data for each scenario
    for scenario in scenarios:
        scenario_files_dir = files_dir / scenario
        scenario_figures_dir = figures_dir / scenario

        if not scenario_files_dir.exists():
            print(f"Warning: Scenario '{scenario}' not found in files directory")
            continue

        # Copy data (files)
        dst_data_scenario = version_data_dir / scenario
        total_operations += 1
        if copy_directory_contents(str(scenario_files_dir), str(dst_data_scenario),
                                 f"data for {scenario}"):
            success_count += 1

        # Copy figures
        if scenario_figures_dir.exists():
            dst_figures_scenario = version_figures_dir / scenario
            total_operations += 1
            if copy_directory_contents(str(scenario_figures_dir), str(dst_figures_scenario),
                                     f"figures for {scenario}"):
                success_count += 1
        else:
            print(f"Warning: No figures directory found for scenario '{scenario}'")

    # Copy global figures (if any)
    global_figures = [f for f in figures_dir.glob("*.png") if f.is_file()]
    global_figures.extend([f for f in figures_dir.glob("*.pdf") if f.is_file()])

    if global_figures:
        print(f"Copying {len(global_figures)} global figures...")
        # Remove existing global figures first
        for existing_fig in version_figures_dir.glob("*.png"):
            if existing_fig.is_file():
                existing_fig.unlink()
        for existing_fig in version_figures_dir.glob("*.pdf"):
            if existing_fig.is_file():
                existing_fig.unlink()

        # Copy new global figures
        for fig_file in global_figures:
            dst_fig = version_figures_dir / fig_file.name
            dst_fig.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(fig_file, dst_fig)
        print("✓ Copied global figures")

    # Copy tolerance analysis plots from analysis results
    if analysis_results_dir.exists():
        tolerance_plot = analysis_results_dir / "tolerance_analysis_plots_custom_ranges.png"
        if tolerance_plot.exists():
            dst_tolerance = version_figures_dir / "tolerance_analysis_plots_custom_ranges.png"
            dst_tolerance.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(tolerance_plot, dst_tolerance)
            print("✓ Copied tolerance analysis plot")
        else:
            print(f"Warning: Tolerance analysis plot not found in {analysis_results_dir}")
    else:
        print(f"Warning: Analysis results directory not found: {analysis_results_dir}")

    # Update versions metadata
    update_versions_metadata(str(docs_static_dir), args.version_name, args.description)

    print("-" * 50)
    print(f"Version update completed!")
    print(f"Success: {success_count}/{total_operations} operations")
    print(f"Version '{args.version_name}' is now available at:")
    print(f"  Data: docs/static/data/{args.version_name}/")
    print(f"  Figures: docs/static/figures/{args.version_name}/")

    if success_count == total_operations:
        print("✓ All operations completed successfully!")
    else:
        print(f"⚠ {total_operations - success_count} operations had issues")
        sys.exit(1)


if __name__ == "__main__":
    main()