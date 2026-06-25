use spacetimedb::{log_stopwatch::LogStopwatch, ReducerContext, Table};

#[spacetimedb::table(accessor = location, index(accessor = coordinates, btree(columns = [x, z, dimension])))]
#[derive(Debug, PartialEq, Eq)]
pub struct Location {
    #[primary_key]
    pub id: u64,
    #[index(btree)]
    pub chunk: u64,
    #[index(btree)]
    pub x: i32,
    pub z: i32,
    pub dimension: u32,
}

// 1000 chunks, 1200 rows per chunk = 1.2M rows
const NUM_CHUNKS: u64 = 1000;
const ROWS_PER_CHUNK: u64 = 1200;

#[spacetimedb::reducer]
pub fn load_location_table(ctx: &ReducerContext) {
    for chunk in 0u64..NUM_CHUNKS {
        for i in 0u64..ROWS_PER_CHUNK {
            let id = chunk * 1200 + i;
            let x = 0i32;
            let z = chunk as i32;
            let dimension = id as u32;
            ctx.db.location().insert(Location {
                id,
                chunk,
                x,
                z,
                dimension,
            });
        }
    }
}

const ID: u64 = 989_987;
const CHUNK: u64 = ID / ROWS_PER_CHUNK;

#[spacetimedb::reducer]
/// Probing a single column index for a single row should be fast!
pub fn test_index_scan_on_id(ctx: &ReducerContext) {
    let span = LogStopwatch::new("Index scan on {id}");
    let location = ctx.db.location().id().find(ID).unwrap();
    span.end();
    assert_eq!(ID, location.id);
}

#[spacetimedb::reducer]
/// Scanning a single column index for `ROWS_PER_CHUNK` rows should also be fast!
pub fn test_index_scan_on_chunk(ctx: &ReducerContext) {
    let span = LogStopwatch::new("Index scan on {chunk}");
    let n = ctx.db.location().chunk().filter(&CHUNK).count();
    span.end();
    assert_eq!(n as u64, ROWS_PER_CHUNK);
}

#[spacetimedb::reducer]
/// Probing a multi-column index for a single row should be fast!
pub fn test_index_scan_on_x_z_dimension(ctx: &ReducerContext) {
    let z = CHUNK as i32;
    let dimension = ID as u32;
    let span = LogStopwatch::new("Index scan on {x, z, dimension}");
    let n = ctx.db.location().coordinates().filter((0, z, dimension)).count();
    span.end();
    assert_eq!(n, 1);
}

#[spacetimedb::reducer]
/// Probing a multi-column index for `ROWS_PER_CHUNK` rows should also be fast!
pub fn test_index_scan_on_x_z(ctx: &ReducerContext) {
    let z = CHUNK as i32;
    let span = LogStopwatch::new("Index scan on {x, z}");
    let n = ctx.db.location().coordinates().filter((0, z)).count();
    span.end();
    assert_eq!(n as u64, ROWS_PER_CHUNK);
}

// ---- Multi-range ("skip scan") comparison: real-world 3x3 spatial neighborhood query ----
//
// Mirrors the typical MMO pattern: look up a character's chunk, then gather everyone in the
// surrounding 3x3 chunk grid *within the same zone*. The index is `(zone, chunk_x, chunk_z)`.
//
// The neighborhood `zone == Z AND chunk_x in [cx-1, cx+1] AND chunk_z in [cz-1, cz+1]` is NOT a
// single contiguous B-tree range (the `chunk_x` band breaks contiguity), so today it is issued as
// one `(zone_eq, chunk_x_eq, chunk_z_range)` scan per `chunk_x` -- three host calls / WASM boundary
// crossings. The multi-range overload expresses it as a single skip-scan call.

#[spacetimedb::table(accessor = spatial_tbl, index(accessor = locality, btree(columns = [zone, chunk_x, chunk_z])))]
pub struct Spatial {
    #[primary_key]
    pub actor_id: u64,
    pub zone: u32,
    pub chunk_x: i32,
    pub chunk_z: i32,
}

const ZONES: u32 = 4;
/// Chunk grid side length per zone.
const CHUNKS: i32 = 64;
/// Actors per chunk cell (rows returned scale with this).
const ACTORS_PER_CHUNK: u64 = 4;

/// A zone whose `chunk_x` values are *sparse* (only every `SPARSE_STRIDE`-th is populated),
/// modeling a wide "area of interest" over a thinly-populated world.
const SPARSE_ZONE: u32 = ZONES;
const SPARSE_STRIDE: i32 = 4;
/// Half-width of the sparse query band (in chunks). Band = `[CENTER_X - R, CENTER_X + R]`.
const SPARSE_RADIUS: i32 = 8;
/// Times each benchmarked reducer repeats its query (amortizes wall-clock noise;
/// fuel is deterministic so the per-query fuel is exact regardless).
const QUERY_REPS: u64 = 1000;

/// Center of the queried neighborhood (interior, so the 3x3 grid is full).
const CENTER_ZONE: u32 = 0;
const CENTER_X: i32 = CHUNKS / 2;
const CENTER_Z: i32 = CHUNKS / 2;
/// Rows returned by one 3x3 neighborhood query: 9 cells x actors-per-chunk.
const NEIGHBORHOOD_ROWS: u64 = 9 * ACTORS_PER_CHUNK;

#[spacetimedb::reducer]
pub fn load_spatial_table(ctx: &ReducerContext) {
    let mut actor_id = 0u64;
    // Dense zones: every chunk_x is populated.
    for zone in 0..ZONES {
        for chunk_x in 0..CHUNKS {
            for chunk_z in 0..CHUNKS {
                for _ in 0..ACTORS_PER_CHUNK {
                    ctx.db.spatial_tbl().insert(Spatial {
                        actor_id,
                        zone,
                        chunk_x,
                        chunk_z,
                    });
                    actor_id += 1;
                }
            }
        }
    }
    // Sparse zone: only every `SPARSE_STRIDE`-th chunk_x is populated.
    let mut chunk_x = 0;
    while chunk_x < CHUNKS {
        for chunk_z in 0..CHUNKS {
            for _ in 0..ACTORS_PER_CHUNK {
                ctx.db.spatial_tbl().insert(Spatial {
                    actor_id,
                    zone: SPARSE_ZONE,
                    chunk_x,
                    chunk_z,
                });
                actor_id += 1;
            }
        }
        chunk_x += SPARSE_STRIDE;
    }
}

#[spacetimedb::reducer]
/// Baseline: the 3x3 neighborhood as three `(zone_eq, chunk_x_eq, chunk_z_range)` scans,
/// i.e. one host call / WASM boundary crossing per `chunk_x`. Mirrors current real-world code.
pub fn test_spatial_grid_manual(ctx: &ReducerContext) {
    let (z_lo, z_hi) = (CENTER_Z - 1, CENTER_Z + 1);
    let span = LogStopwatch::new("Spatial 3x3 neighborhood: 3x (eq, eq, range)");
    let mut checksum = 0u64;
    let mut count = 0u64;
    for _ in 0..QUERY_REPS {
        for chunk_x in (CENTER_X - 1)..=(CENTER_X + 1) {
            for actor_id in ctx
                .db
                .spatial_tbl()
                .locality()
                .filter((CENTER_ZONE, chunk_x, z_lo..=z_hi))
                .map(|row| row.actor_id)
            {
                checksum = checksum.wrapping_add(actor_id);
                count += 1;
            }
        }
    }
    span.end();
    assert_eq!(count, NEIGHBORHOOD_ROWS * QUERY_REPS);
    assert_ne!(checksum, 0);
}

/// Distinct populated `chunk_x` values within the sparse query band.
const SPARSE_GROUPS: u64 = (2 * (SPARSE_RADIUS / SPARSE_STRIDE) + 1) as u64;
/// Rows per sparse query: populated chunk_x in band x 3 chunk_z cells x actors-per-chunk.
const SPARSE_ROWS: u64 = SPARSE_GROUPS * 3 * ACTORS_PER_CHUNK;

#[spacetimedb::reducer]
/// Baseline over a *sparse* wide band: one `(eq, eq, range)` host call per `chunk_x` in the band,
/// including the many absent ones. This is what a fixed-radius manual loop costs today.
pub fn test_spatial_sparse_manual(ctx: &ReducerContext) {
    let (z_lo, z_hi) = (CENTER_Z - 1, CENTER_Z + 1);
    let span = LogStopwatch::new("Sparse wide band: (2R+1)x (eq, eq, range)");
    let mut checksum = 0u64;
    let mut count = 0u64;
    for _ in 0..QUERY_REPS {
        for chunk_x in (CENTER_X - SPARSE_RADIUS)..=(CENTER_X + SPARSE_RADIUS) {
            for actor_id in ctx
                .db
                .spatial_tbl()
                .locality()
                .filter((SPARSE_ZONE, chunk_x, z_lo..=z_hi))
                .map(|row| row.actor_id)
            {
                checksum = checksum.wrapping_add(actor_id);
                count += 1;
            }
        }
    }
    span.end();
    assert_eq!(count, SPARSE_ROWS * QUERY_REPS);
    assert_ne!(checksum, 0);
}

#[spacetimedb::reducer]
/// New: the sparse wide band as a single skip scan, which jumps only to populated `chunk_x`.
pub fn test_spatial_sparse_skip(ctx: &ReducerContext) {
    let (x_lo, x_hi) = (CENTER_X - SPARSE_RADIUS, CENTER_X + SPARSE_RADIUS);
    let (z_lo, z_hi) = (CENTER_Z - 1, CENTER_Z + 1);
    let span = LogStopwatch::new("Sparse wide band: 1x skip scan");
    let mut checksum = 0u64;
    let mut count = 0u64;
    for _ in 0..QUERY_REPS {
        for actor_id in ctx
            .db
            .spatial_tbl()
            .locality()
            .filter((SPARSE_ZONE, x_lo..=x_hi, z_lo..=z_hi))
            .map(|row| row.actor_id)
        {
            checksum = checksum.wrapping_add(actor_id);
            count += 1;
        }
    }
    span.end();
    assert_eq!(count, SPARSE_ROWS * QUERY_REPS);
    assert_ne!(checksum, 0);
}

#[spacetimedb::reducer]
/// New: the same 3x3 neighborhood as a single multi-range ("skip scan") query.
pub fn test_spatial_grid_skip(ctx: &ReducerContext) {
    let (x_lo, x_hi) = (CENTER_X - 1, CENTER_X + 1);
    let (z_lo, z_hi) = (CENTER_Z - 1, CENTER_Z + 1);
    let span = LogStopwatch::new("Spatial 3x3 neighborhood: 1x skip scan");
    let mut checksum = 0u64;
    let mut count = 0u64;
    for _ in 0..QUERY_REPS {
        for actor_id in ctx
            .db
            .spatial_tbl()
            .locality()
            .filter((CENTER_ZONE, x_lo..=x_hi, z_lo..=z_hi))
            .map(|row| row.actor_id)
        {
            checksum = checksum.wrapping_add(actor_id);
            count += 1;
        }
    }
    span.end();
    assert_eq!(count, NEIGHBORHOOD_ROWS * QUERY_REPS);
    assert_ne!(checksum, 0);
}
