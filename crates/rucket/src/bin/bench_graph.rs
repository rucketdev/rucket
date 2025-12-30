// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmark graph generator.
//!
//! This binary parses Criterion benchmark results and generates SVG charts
//! for visualization in documentation.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use plotters::prelude::*;
use serde::{Deserialize, Serialize};
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
    let mut results =
        BenchResults { timestamp: chrono::Utc::now().to_rfc3339(), results: Vec::new() };

    // Walk through the criterion directory looking for estimates.json files
    for entry in WalkDir::new(criterion_dir).into_iter().filter_map(|e| e.ok()) {
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
    let content = fs::read_to_string(path)?;
    let estimates: CriterionEstimates = serde_json::from_str(&content)?;

    // Extract group, function, variant from path
    let components: Vec<_> = path
        .ancestors()
        .skip(2)
        .take(3)
        .filter_map(|p| p.file_name())
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    if components.len() < 3 {
        return Ok(None);
    }

    let variant = &components[0];
    let group = &components[2];

    if !group.starts_with("profile_") {
        return Ok(None);
    }

    // Parse variant: "fast_1KB" or "fast/1KB"
    let variant_decoded = variant.replace("%2F", "/").replace("%20", " ");
    let parts: Vec<&str> = if variant_decoded.contains('_') {
        variant_decoded.splitn(2, '_').collect()
    } else {
        variant_decoded.split('/').collect()
    };

    if parts.len() != 2 {
        return Ok(None);
    }

    let profile = parts[0].to_string();
    let size = parts[1].to_string();

    let bytes = size_bytes(&size);
    if bytes == 0 {
        return Ok(None);
    }

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

// Profile colors
const COLOR_NEVER: RGBColor = RGBColor(59, 130, 246); // Blue
const COLOR_PERIODIC: RGBColor = RGBColor(34, 197, 94); // Green
const COLOR_ALWAYS: RGBColor = RGBColor(239, 68, 68); // Red

fn profile_color(profile: &str) -> RGBColor {
    match profile {
        "never" => COLOR_NEVER,
        "periodic" => COLOR_PERIODIC,
        "always" => COLOR_ALWAYS,
        _ => RGBColor(128, 128, 128),
    }
}

/// Generate PUT throughput chart.
fn generate_put_chart(results: &BenchResults, output_dir: &Path) -> anyhow::Result<()> {
    let output_path = output_dir.join("put_throughput.svg");
    let put_results: Vec<_> = results.results.iter().filter(|r| r.group == "profile_put").collect();

    if put_results.is_empty() {
        println!("No profile_put results found, skipping PUT chart");
        return Ok(());
    }

    generate_bar_chart(&put_results, &output_path, "PUT Throughput by Sync Profile")?;
    println!("Generated: {}", output_path.display());
    Ok(())
}

/// Generate GET throughput chart.
fn generate_get_chart(results: &BenchResults, output_dir: &Path) -> anyhow::Result<()> {
    let output_path = output_dir.join("get_throughput.svg");
    let get_results: Vec<_> = results.results.iter().filter(|r| r.group == "profile_get").collect();

    if get_results.is_empty() {
        println!("No profile_get results found, skipping GET chart");
        return Ok(());
    }

    generate_bar_chart(&get_results, &output_path, "GET Throughput by Sync Profile")?;
    println!("Generated: {}", output_path.display());
    Ok(())
}

/// Generate a grouped bar chart for throughput results.
fn generate_bar_chart(
    results: &[&BenchResult],
    output_path: &Path,
    title: &str,
) -> anyhow::Result<()> {
    let profiles = ["never", "periodic", "always"];
    let sizes = ["1KB", "64KB", "1MB"];

    // Build lookup table
    let mut lookup: BTreeMap<(&str, &str), f64> = BTreeMap::new();
    for r in results {
        lookup.insert((r.profile.as_str(), r.size.as_str()), r.throughput_mbs);
    }

    let max_throughput = results.iter().map(|r| r.throughput_mbs).fold(0.0_f64, f64::max) * 1.2;

    let root = SVGBackend::new(output_path, (800, 500)).into_drawing_area();
    root.fill(&WHITE)?;

    // Calculate bar positions
    // Each size group has 3 bars (one per profile)
    // Total bars = 9, with gaps between size groups
    let bar_width = 0.8;
    let group_gap = 1.0;
    let total_width = sizes.len() as f64 * (profiles.len() as f64 * bar_width + group_gap);

    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 24).into_font())
        .margin(20)
        .margin_right(120) // Extra space for legend
        .x_label_area_size(60)
        .y_label_area_size(80)
        .build_cartesian_2d(0.0..total_width, 0.0..max_throughput)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .y_desc("Throughput (MB/s)")
        .x_desc("Object Size")
        .x_labels(sizes.len())
        .x_label_formatter(&|x| {
            let group_width = profiles.len() as f64 * bar_width + group_gap;
            let group_idx = (*x / group_width) as usize;
            let within_group = *x - group_idx as f64 * group_width;
            // Only show label at center of group
            if within_group > 0.5 && within_group < group_width - 0.5 {
                sizes.get(group_idx).map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        })
        .draw()?;

    // Draw bars and collect legend entries
    for (profile_idx, profile) in profiles.iter().enumerate() {
        let color = profile_color(profile);
        let mut bar_data = Vec::new();

        for (size_idx, size) in sizes.iter().enumerate() {
            if let Some(&throughput) = lookup.get(&(*profile, *size)) {
                let group_width = profiles.len() as f64 * bar_width + group_gap;
                let x = size_idx as f64 * group_width + profile_idx as f64 * bar_width;
                bar_data.push((x, throughput));
            }
        }

        // Draw bars for this profile
        chart
            .draw_series(bar_data.iter().map(|&(x, throughput)| {
                Rectangle::new(
                    [(x, 0.0), (x + bar_width * 0.9, throughput)],
                    color.mix(0.85).filled(),
                )
            }))?
            .label(*profile)
            .legend(move |(x, y)| Rectangle::new([(x, y - 6), (x + 18, y + 6)], color.filled()));
    }

    // Draw legend manually in the right margin (outside chart area)
    let legend_x = 690;
    let legend_y = 80;
    let box_size = 14;
    let line_height = 22;

    // Legend background
    root.draw(&Rectangle::new(
        [(legend_x - 10, legend_y - 15), (legend_x + 90, legend_y + 3 * line_height)],
        ShapeStyle::from(&WHITE).filled(),
    ))?;
    root.draw(&Rectangle::new(
        [(legend_x - 10, legend_y - 15), (legend_x + 90, legend_y + 3 * line_height)],
        BLACK.stroke_width(1),
    ))?;

    for (i, profile) in profiles.iter().enumerate() {
        let y = legend_y + i as i32 * line_height;
        let color = profile_color(profile);

        root.draw(&Rectangle::new(
            [(legend_x, y), (legend_x + box_size, y + box_size)],
            color.filled(),
        ))?;
        root.draw(&Text::new(
            *profile,
            (legend_x + box_size + 6, y + 2),
            ("sans-serif", 13).into_font(),
        ))?;
    }

    root.present()?;
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

    if !criterion_dir.exists() {
        eprintln!("Error: No benchmark results found at {}", criterion_dir.display());
        eprintln!("Run 'cargo bench --bench throughput' first.");
        std::process::exit(1);
    }

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

    generate_put_chart(&results, graphs_dir)?;
    generate_get_chart(&results, graphs_dir)?;

    export_json(&results, results_path)?;

    println!("\nDone! Charts saved to {}", graphs_dir.display());
    Ok(())
}
