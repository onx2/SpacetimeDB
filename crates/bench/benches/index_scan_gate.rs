use std::time::Duration;

use anyhow::{bail, Context, Result};
use spacetimedb_sats::product;
use spacetimedb_testing::modules::{start_runtime, CompilationMode, CompiledModule, IN_MEMORY_CONFIG};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const WARMUP_RUNS: usize = 5;
const MEASURED_RUNS: usize = 31;
const MEDIAN_THRESHOLD: Duration = Duration::from_micros(100);

const REDUCERS: &[&str] = &[
    "test_index_scan_on_id",
    "test_index_scan_on_chunk",
    "test_index_scan_on_x_z_dimension",
    "test_index_scan_on_x_z",
];

fn main() -> Result<()> {
    let module = CompiledModule::compile("perf-test", CompilationMode::Release);
    let runtime = start_runtime();

    runtime.block_on(async {
        let module = module.load_module(IN_MEMORY_CONFIG, None).await;
        let no_args = product![];

        println!("loading perf-test location table...");
        module
            .call_reducer_binary("load_location_table", &no_args)
            .await
            .context("failed to load perf-test location table")?;

        let mut failures = Vec::new();
        for &reducer in REDUCERS {
            for _ in 0..WARMUP_RUNS {
                module
                    .call_reducer_binary_result(reducer, &no_args)
                    .await
                    .with_context(|| format!("failed during warmup for {reducer}"))?;
            }

            let mut samples = Vec::with_capacity(MEASURED_RUNS);
            for _ in 0..MEASURED_RUNS {
                let result = module
                    .call_reducer_binary_result(reducer, &no_args)
                    .await
                    .with_context(|| format!("failed during measured run for {reducer}"))?;
                samples.push(result.execution_duration);
            }

            samples.sort_unstable();
            let median = samples[samples.len() / 2];

            println!("{reducer:<36} median={median:?}");
            if median >= MEDIAN_THRESHOLD {
                failures.push(format!("{reducer} median {median:?}"));
            }
        }

        if !failures.is_empty() {
            bail!(
                "index scan benchmark failed; median threshold is {:?}; failures: {}",
                MEDIAN_THRESHOLD,
                failures.join(", ")
            );
        }

        println!(
            "index scan benchmark passed; all medians are below {:?}",
            MEDIAN_THRESHOLD
        );

        // Informational comparison (not gated): 3x3 spatial grid query expressed as
        // three `(eq, range)` scans vs. a single multi-range ("skip scan") query.
        println!("\nloading perf-test spatial grid...");
        module
            .call_reducer_binary("load_spatial_table", &no_args)
            .await
            .context("failed to load perf-test spatial table")?;

        // Measures one (baseline, skip) reducer pair, reporting fuel (the billed cost; deterministic)
        // and wall-clock median, for a given scenario.
        let comparisons: &[(&str, &str, &str)] = &[
            (
                "DENSE 3x3 neighborhood (your real-world shape): 3x (eq, eq, range) vs. 1x skip",
                "test_spatial_grid_manual",
                "test_spatial_grid_skip",
            ),
            (
                "SPARSE wide band, radius 8 (17 chunk_x, ~5 populated): 17x (eq,eq,range) vs. 1x skip",
                "test_spatial_sparse_manual",
                "test_spatial_sparse_skip",
            ),
        ];

        println!("\nMulti-range comparisons ({QUERY_REPS} queries/run; fuel is per-run total, deterministic):");
        for (label, manual_reducer, skip_reducer) in comparisons {
            let mut results = Vec::new();
            for &reducer in &[*manual_reducer, *skip_reducer] {
                for _ in 0..WARMUP_RUNS {
                    module
                        .call_reducer_binary_result(reducer, &no_args)
                        .await
                        .with_context(|| format!("failed during warmup for {reducer}"))?;
                }
                let mut durations = Vec::with_capacity(MEASURED_RUNS);
                let mut fuel = 0u64;
                for _ in 0..MEASURED_RUNS {
                    let result = module
                        .call_reducer_binary_result(reducer, &no_args)
                        .await
                        .with_context(|| format!("failed during measured run for {reducer}"))?;
                    durations.push(result.execution_duration);
                    fuel = result.execution_budget_used.get();
                }
                durations.sort_unstable();
                results.push((durations[durations.len() / 2], fuel));
            }
            let (manual_dur, manual_fuel) = results[0];
            let (skip_dur, skip_fuel) = results[1];
            println!("\n{label}");
            println!(
                "  baseline    fuel={manual_fuel:>12} ({:>6}/query)   wall median={manual_dur:?}",
                manual_fuel / QUERY_REPS
            );
            println!(
                "  skip scan   fuel={skip_fuel:>12} ({:>6}/query)   wall median={skip_dur:?}",
                skip_fuel / QUERY_REPS
            );
            let fuel_ratio = manual_fuel as f64 / skip_fuel as f64;
            let wall_ratio = manual_dur.as_secs_f64() / skip_dur.as_secs_f64();
            println!("    -> ENERGY (fuel) savings: {fuel_ratio:.2}x    wall-clock: {wall_ratio:.2}x");
        }

        Ok(())
    })
}

/// Mirrors `QUERY_REPS` in `modules/perf-test`; used only for reporting.
const QUERY_REPS: u64 = 1000;
