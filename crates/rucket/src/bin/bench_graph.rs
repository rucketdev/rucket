// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmark graph generator.
//!
//! This binary parses Criterion benchmark results and generates SVG charts
//! for visualization in documentation.

#![allow(missing_docs)]

use plotters::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

/// Criterion estimates structure (partial).
#[derive(Debug, Deserialize)]
struct CriterionEstimates {
    mean: Estimate,
}

#[derive(Debug, Deserialize)]
struct Estimate {
    point_estimate: f64,
    #[serde(rename = "confidence_interval")]
    _confidence_interval: ConfidenceInterval,
}

#[derive(Debug, Deserialize)]
struct ConfidenceInterval {
    #[serde(rename = "lower_bound")]
    _lower_bound: f64,
    #[serde(rename = "upper_bound")]
    _upper_bound: f64,
}

/// Benchmark result with throughput calculation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchResult {
    group: String,
    name: String,
    profile: String,
    size: String,
    mean_ns: f64,
    throughput_mbs: f64,
}

/// Collected benchmark results.
#[derive(Debug, Default, Serialize, Deserialize)]
struct BenchResults {
    timestamp: String,
    results: Vec<BenchResult>,
}

/// Size in bytes for throughput calculation.
fn size_bytes(size: &str) -> u64 {
    match size {
        "1KB" => 1024,
        "64KB" => 64 * 1024,
        "1MB" => 1024 * 1024,
        _ => 0,
    }
}

/// Parse all criterion results from target/criterion directory.
fn collect_results(criterion_dir: &Path) -> anyhow::Result<BenchResults> {
    let mut results = BenchResults {
        timestamp: chrono::Utc::now().to_rfc3339(),
        results: Vec::new(),
    };

    // Walk through the criterion directory looking for estimates.json files
    for entry in WalkDir::new(criterion_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.file_name() == Some("estimates.json".as_ref())
            && path.parent().and_then(|p| p.file_name()) == Some("new".as_ref())
        {
            if let Some(result) = parse_benchmark_result(path)? {
                results.results.push(result);
            }
        }
    }

    Ok(results)
}

/// Parse a single benchmark result from estimates.json.
fn parse_benchmark_result(path: &Path) -> anyhow::Result<Option<BenchResult>> {
    // Path structure: target/criterion/{group}/{function}/{variant}/new/estimates.json
    // Example: target/criterion/profile_put/throughput/fast_1KB/new/estimates.json

    let content = fs::read_to_string(path)?;
    let estimates: CriterionEstimates = serde_json::from_str(&content)?;

    // Extract group, function, variant from path
    let components: Vec<_> = path
        .ancestors()
        .skip(2) // Skip new/ and estimates.json
        .take(3)
        .filter_map(|p| p.file_name())
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    if components.len() < 3 {
        return Ok(None);
    }

    // components[0] = variant (e.g., "fast_1KB" or "fast/1KB")
    // components[1] = function (e.g., "throughput")
    // components[2] = group (e.g., "profile_put")

    let variant = &components[0];
    let group = &components[2];

    // Only process profile_put and profile_get groups
    if !group.starts_with("profile_") {
        return Ok(None);
    }

    // Parse variant: could be "fast/1KB" URL-encoded or similar
    let variant_decoded = urlencoding_decode(variant);
    let parts: Vec<&str> = variant_decoded.split('/').collect();

    if parts.len() != 2 {
        return Ok(None);
    }

    let profile = parts[0].to_string();
    let size = parts[1].to_string();

    let bytes = size_bytes(&size);
    if bytes == 0 {
        return Ok(None);
    }

    // Calculate throughput: bytes / ns * 1e9 / 1048576 = MB/s
    let throughput_mbs = (bytes as f64) / estimates.mean.point_estimate * 1e9 / 1_048_576.0;

    Ok(Some(BenchResult {
        group: group.clone(),
        name: format!("{}/{}", profile, size),
        profile,
        size,
        mean_ns: estimates.mean.point_estimate,
        throughput_mbs,
    }))
}

/// Simple URL decoding for common cases.
fn urlencoding_decode(s: &str) -> String {
    s.replace("%2F", "/").replace("%20", " ")
}

/// Generate PUT throughput chart.
fn generate_put_chart(results: &BenchResults, output_dir: &Path) -> anyhow::Result<()> {
    let output_path = output_dir.join("put_throughput.svg");

    // Filter to profile_put results
    let put_results: Vec<_> = results
        .results
        .iter()
        .filter(|r| r.group == "profile_put")
        .collect();

    if put_results.is_empty() {
        println!("No profile_put results found, skipping PUT chart");
        return Ok(());
    }

    generate_throughput_chart(&put_results, &output_path, "PUT Throughput by Sync Profile")?;
    println!("Generated: {}", output_path.display());
    Ok(())
}

/// Generate GET throughput chart.
fn generate_get_chart(results: &BenchResults, output_dir: &Path) -> anyhow::Result<()> {
    let output_path = output_dir.join("get_throughput.svg");

    // Filter to profile_get results
    let get_results: Vec<_> = results
        .results
        .iter()
        .filter(|r| r.group == "profile_get")
        .collect();

    if get_results.is_empty() {
        println!("No profile_get results found, skipping GET chart");
        return Ok(());
    }

    generate_throughput_chart(&get_results, &output_path, "GET Throughput by Sync Profile")?;
    println!("Generated: {}", output_path.display());
    Ok(())
}

/// Generate a grouped bar chart for throughput results.
fn generate_throughput_chart(
    results: &[&BenchResult],
    output_path: &Path,
    title: &str,
) -> anyhow::Result<()> {
    // Group by profile, then by size
    let mut data: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for r in results {
        data.entry(r.profile.clone())
            .or_default()
            .insert(r.size.clone(), r.throughput_mbs);
    }

    let profiles: Vec<_> = data.keys().cloned().collect();
    let sizes = ["1KB", "64KB", "1MB"];

    // Colors optimized for GitHub dark/light mode
    let colors = [
        RGBColor(59, 130, 246),  // Blue - fast
        RGBColor(34, 197, 94),   // Green - balanced
        RGBColor(239, 68, 68),   // Red - durable
    ];

    let root = SVGBackend::new(output_path, (800, 500)).into_drawing_area();
    root.fill(&WHITE)?;

    // Find max throughput for Y axis
    let max_throughput = results
        .iter()
        .map(|r| r.throughput_mbs)
        .fold(0.0_f64, f64::max)
        * 1.15; // Add 15% headroom

    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 24).into_font())
        .margin(20)
        .x_label_area_size(50)
        .y_label_area_size(80)
        .build_cartesian_2d(
            (0..sizes.len()).into_segmented(),
            0.0..max_throughput,
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .y_desc("Throughput (MB/s)")
        .x_desc("Object Size")
        .x_labels(sizes.len())
        .x_label_formatter(&|x| {
            if let SegmentValue::CenterOf(idx) = x {
                sizes.get(*idx).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .draw()?;

    // Draw bars for each profile
    let bar_width = 0.25;
    for (profile_idx, profile) in profiles.iter().enumerate() {
        let color = colors.get(profile_idx).copied().unwrap_or(BLACK);

        if let Some(profile_data) = data.get(profile) {
            let bars: Vec<_> = sizes
                .iter()
                .enumerate()
                .filter_map(|(size_idx, size)| {
                    profile_data.get(*size).map(|&throughput| {
                        let x_offset = (profile_idx as f64 - 1.0) * bar_width;
                        (size_idx, x_offset, throughput)
                    })
                })
                .collect();

            chart.draw_series(bars.iter().map(|&(size_idx, _x_offset, throughput)| {
                Rectangle::new(
                    [
                        (SegmentValue::Exact(size_idx), 0.0),
                        (SegmentValue::Exact(size_idx), throughput),
                    ],
                    color.mix(0.8).filled(),
                )
                .into_dyn()
            }))?;
        }
    }

    // Draw legend
    for (idx, profile) in profiles.iter().enumerate() {
        let color = colors.get(idx).copied().unwrap_or(BLACK);
        chart
            .draw_series(std::iter::once(Rectangle::new(
                [(SegmentValue::Exact(0), 0.0), (SegmentValue::Exact(0), 0.0)],
                color.filled(),
            )))?
            .label(profile.as_str())
            .legend(move |(x, y)| {
                Rectangle::new([(x, y - 5), (x + 15, y + 5)], color.filled())
            });
    }

    chart
        .configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .border_style(BLACK)
        .background_style(WHITE.mix(0.8))
        .draw()?;

    root.present()?;
    Ok(())
}

/// Generate comparison chart showing all profiles.
fn generate_comparison_chart(results: &BenchResults, output_dir: &Path) -> anyhow::Result<()> {
    let output_path = output_dir.join("sync_comparison.svg");

    // Combine PUT and GET results
    let all_results: Vec<_> = results
        .results
        .iter()
        .filter(|r| r.group.starts_with("profile_"))
        .collect();

    if all_results.is_empty() {
        println!("No profile results found, skipping comparison chart");
        return Ok(());
    }

    let root = SVGBackend::new(&output_path, (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    // Split into PUT and GET sections
    let (upper, lower) = root.split_vertically(300);

    // Draw PUT comparison
    let put_results: Vec<_> = all_results
        .iter()
        .filter(|r| r.group == "profile_put")
        .cloned()
        .collect();

    if !put_results.is_empty() {
        draw_comparison_section(&upper, &put_results, "PUT Operations")?;
    }

    // Draw GET comparison
    let get_results: Vec<_> = all_results
        .iter()
        .filter(|r| r.group == "profile_get")
        .cloned()
        .collect();

    if !get_results.is_empty() {
        draw_comparison_section(&lower, &get_results, "GET Operations")?;
    }

    root.present()?;
    println!("Generated: {}", output_path.display());
    Ok(())
}

/// Draw a comparison section for PUT or GET.
fn draw_comparison_section(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    results: &[&BenchResult],
    title: &str,
) -> anyhow::Result<()> {
    let profiles = ["fast", "balanced", "durable"];
    let sizes = ["1KB", "64KB", "1MB"];

    let colors = [
        RGBColor(59, 130, 246),  // Blue
        RGBColor(34, 197, 94),   // Green
        RGBColor(239, 68, 68),   // Red
    ];

    // Build lookup table
    let mut lookup: BTreeMap<(&str, &str), f64> = BTreeMap::new();
    for r in results {
        lookup.insert((r.profile.as_str(), r.size.as_str()), r.throughput_mbs);
    }

    let max_throughput = results
        .iter()
        .map(|r| r.throughput_mbs)
        .fold(0.0_f64, f64::max)
        * 1.15;

    let num_groups = sizes.len() * profiles.len();

    let mut chart = ChartBuilder::on(area)
        .caption(title, ("sans-serif", 20).into_font())
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(70)
        .build_cartesian_2d(0..num_groups, 0.0..max_throughput)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .y_desc("MB/s")
        .x_label_formatter(&|idx| {
            let size_idx = idx / profiles.len();
            let profile_idx = idx % profiles.len();
            if profile_idx == 1 {
                sizes.get(size_idx).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .draw()?;

    // Draw bars
    for (size_idx, size) in sizes.iter().enumerate() {
        for (profile_idx, profile) in profiles.iter().enumerate() {
            if let Some(&throughput) = lookup.get(&(*profile, *size)) {
                let x = size_idx * profiles.len() + profile_idx;
                let color = colors[profile_idx];

                chart.draw_series(std::iter::once(Rectangle::new(
                    [(x, 0.0), (x + 1, throughput)],
                    color.mix(0.8).filled(),
                )))?;
            }
        }
    }

    Ok(())
}

/// Export results to JSON file.
fn export_json(results: &BenchResults, output_path: &Path) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(results)?;
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, json)?;
    println!("Exported: {}", output_path.display());
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let criterion_dir = Path::new("target/criterion");
    let graphs_dir = Path::new("docs/benchmarks/graphs");
    let results_path = Path::new("docs/benchmarks/results/latest.json");

    // Check if criterion results exist
    if !criterion_dir.exists() {
        eprintln!("Error: No benchmark results found at {}", criterion_dir.display());
        eprintln!("Run 'cargo bench --bench throughput' first.");
        std::process::exit(1);
    }

    // Create output directories
    fs::create_dir_all(graphs_dir)?;
    if let Some(parent) = results_path.parent() {
        fs::create_dir_all(parent)?;
    }

    println!("Collecting benchmark results...");
    let results = collect_results(criterion_dir)?;

    if results.results.is_empty() {
        eprintln!("Warning: No profile benchmark results found.");
        eprintln!("Make sure to run the profile_put and profile_get benchmarks.");
        return Ok(());
    }

    println!("Found {} benchmark results", results.results.len());

    // Generate charts
    generate_put_chart(&results, graphs_dir)?;
    generate_get_chart(&results, graphs_dir)?;
    generate_comparison_chart(&results, graphs_dir)?;

    // Export JSON
    export_json(&results, results_path)?;

    println!("\nDone! Charts saved to {}", graphs_dir.display());
    Ok(())
}
