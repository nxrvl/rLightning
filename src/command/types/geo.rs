use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::command::types::sorted_set::SortedSetData;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;

type SortedSet = Vec<(f64, Vec<u8>)>;

// Redis uses this earth radius constant (meters)
const EARTH_RADIUS_M: f64 = 6372797.560856;

// Longitude range: -180 to 180
const GEO_LONG_MIN: f64 = -180.0;
const GEO_LONG_MAX: f64 = 180.0;
// Latitude range: -85.05112878 to 85.05112878 (Mercator projection limit)
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;

// 26 bits for each coordinate = 52 bits total
const GEO_STEP_MAX: u32 = 26;

// Geohash base32 alphabet (same as geohash.org, no 'a', 'i', 'l', 'o')
const BASE32_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

// --- Geohash Encoding/Decoding ---

/// Encode longitude and latitude into a 52-bit geohash integer.
fn geohash_encode(longitude: f64, latitude: f64) -> u64 {
    let lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let long_offset = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);

    let lat_bits = (lat_offset * ((1u64 << GEO_STEP_MAX) as f64)) as u64;
    let long_bits = (long_offset * ((1u64 << GEO_STEP_MAX) as f64)) as u64;

    interleave(long_bits, lat_bits)
}

/// Decode a 52-bit geohash integer back to longitude and latitude.
fn geohash_decode(hash: u64) -> (f64, f64) {
    let (long_bits, lat_bits) = deinterleave(hash);

    let lon = GEO_LONG_MIN
        + (long_bits as f64 / (1u64 << GEO_STEP_MAX) as f64) * (GEO_LONG_MAX - GEO_LONG_MIN);
    let lat = GEO_LAT_MIN
        + (lat_bits as f64 / (1u64 << GEO_STEP_MAX) as f64) * (GEO_LAT_MAX - GEO_LAT_MIN);

    (lon, lat)
}

/// Interleave bits: even bit positions for x (longitude), odd for y (latitude).
fn interleave(x: u64, y: u64) -> u64 {
    let mut result: u64 = 0;
    for i in 0..GEO_STEP_MAX {
        result |= ((x >> i) & 1) << (2 * i + 1);
        result |= ((y >> i) & 1) << (2 * i);
    }
    result
}

/// De-interleave a 52-bit hash back to x (longitude bits) and y (latitude bits).
fn deinterleave(hash: u64) -> (u64, u64) {
    let mut x: u64 = 0;
    let mut y: u64 = 0;
    for i in 0..GEO_STEP_MAX {
        x |= ((hash >> (2 * i + 1)) & 1) << i;
        y |= ((hash >> (2 * i)) & 1) << i;
    }
    (x, y)
}

/// Convert a 52-bit geohash integer to a base32 geohash string (11 characters).
/// Redis pads the 52-bit hash to 55 bits (11 * 5) by shifting left 3 bits.
fn geohash_to_string(hash: u64) -> String {
    // 11 base32 chars = 55 bits. We have 52 bits, so shift left by 3.
    let hash55 = hash << 3;
    let mut result = String::with_capacity(11);
    for i in (0..11).rev() {
        let idx = ((hash55 >> (i * 5)) & 0x1F) as usize;
        result.push(BASE32_ALPHABET[idx] as char);
    }
    result
}

/// Encode longitude/latitude into a standard geohash base32 string (11 characters).
/// Uses the standard geohash latitude range [-90, 90] (not the internal Mercator range),
/// matching Redis's GEOHASH command output and geohash.org convention.
fn geohash_string_from_lonlat(longitude: f64, latitude: f64) -> String {
    let lat_offset = (latitude - (-90.0)) / (90.0 - (-90.0));
    let long_offset = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);

    let lat_bits = (lat_offset * ((1u64 << GEO_STEP_MAX) as f64)) as u64;
    let long_bits = (long_offset * ((1u64 << GEO_STEP_MAX) as f64)) as u64;

    let hash = interleave(long_bits, lat_bits);
    geohash_to_string(hash)
}

// --- Distance Calculation ---

/// Haversine distance between two points in meters.
fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

/// Convert meters to the given unit.
fn meters_to_unit(meters: f64, unit: &str) -> f64 {
    match unit {
        "m" => meters,
        "km" => meters / 1000.0,
        "mi" => meters / 1609.34,
        "ft" => meters / 0.3048,
        _ => meters,
    }
}

/// Convert the given unit value to meters.
fn unit_to_meters(value: f64, unit: &str) -> f64 {
    match unit {
        "m" => value,
        "km" => value * 1000.0,
        "mi" => value * 1609.34,
        "ft" => value * 0.3048,
        _ => value,
    }
}

/// Parse a distance unit string, returning the lowercase form.
fn parse_unit(s: &str) -> Result<String, CommandError> {
    let lower = s.to_lowercase();
    match lower.as_str() {
        "m" | "km" | "mi" | "ft" => Ok(lower),
        _ => Err(CommandError::InvalidArgument(
            "unsupported unit provided. please use M, KM, FT, MI".to_string(),
        )),
    }
}

// --- Sorted Set Helpers (reused from sorted_set.rs pattern) ---

async fn load_sorted_set(
    engine: &StorageEngine,
    key: &[u8],
) -> Result<Option<SortedSet>, CommandError> {
    match engine.get(key).await? {
        Some(data) => {
            let ss_data = bincode::deserialize::<SortedSetData>(&data)
                .map_err(|_| CommandError::WrongType)?;
            // Convert from SortedSetData to Vec<(f64, Vec<u8>)> - already sorted by BTreeSet
            let vec: Vec<(f64, Vec<u8>)> = ss_data.entries.into_iter()
                .map(|(score, member)| (score.into_inner(), member))
                .collect();
            Ok(Some(vec))
        }
        None => Ok(None),
    }
}

async fn save_sorted_set(
    engine: &StorageEngine,
    key: &[u8],
    ss: &SortedSet,
) -> Result<(), CommandError> {
    if ss.is_empty() {
        engine.del(key).await?;
    } else {
        let mut ss_data = SortedSetData::new();
        for (score, member) in ss {
            ss_data.insert(*score, member.clone());
        }
        let serialized = bincode::serialize(&ss_data)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine
            .set_with_type_preserve_ttl(key.to_vec(), serialized, RedisDataType::ZSet)
            .await?;
    }
    Ok(())
}

fn validate_longitude(lon: f64) -> Result<(), CommandError> {
    if !(-180.0..=180.0).contains(&lon) {
        return Err(CommandError::InvalidArgument(format!(
            "invalid longitude,latitude pair {:.6},*",
            lon
        )));
    }
    Ok(())
}

fn validate_latitude(lat: f64) -> Result<(), CommandError> {
    if !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Err(CommandError::InvalidArgument(format!(
            "invalid longitude,latitude pair *,{:.6}",
            lat
        )));
    }
    Ok(())
}

fn parse_float_arg(bytes: &[u8], name: &str) -> Result<f64, CommandError> {
    let s = bytes_to_string(bytes)?;
    s.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument(format!("{} is not a valid float", name))
    })
}

// --- Geo search result item ---

struct GeoResult {
    member: Vec<u8>,
    dist: f64,
    hash: u64,
    lon: f64,
    lat: f64,
}

/// Common search logic used by GEOSEARCH, GEOSEARCHSTORE, GEORADIUS, GEORADIUSBYMEMBER
#[allow(clippy::too_many_arguments)]
fn search_members(
    ss: &SortedSet,
    center_lon: f64,
    center_lat: f64,
    shape: &SearchShape,
    unit: &str,
    count: Option<usize>,
    any: bool,
    sort: SortOrder,
) -> Vec<GeoResult> {
    let mut results: Vec<GeoResult> = Vec::new();

    for (score, member) in ss {
        let hash = *score as u64;
        let (lon, lat) = geohash_decode(hash);
        let dist_m = haversine_distance(center_lon, center_lat, lon, lat);

        let in_shape = match shape {
            SearchShape::Radius(radius_m) => dist_m <= *radius_m,
            SearchShape::Box(width_m, height_m) => {
                // Box search: check if point is within width/2 and height/2 of center
                let dist_lon = haversine_distance(center_lon, center_lat, lon, center_lat);
                let dist_lat = haversine_distance(center_lon, center_lat, center_lon, lat);
                dist_lon <= width_m / 2.0 && dist_lat <= height_m / 2.0
            }
        };

        if in_shape {
            let dist = meters_to_unit(dist_m, unit);
            results.push(GeoResult {
                member: member.clone(),
                dist,
                hash,
                lon,
                lat,
            });
            // If ANY is specified with COUNT, we can stop early (no sorting needed)
            if any
                && let Some(c) = count
                    && results.len() >= c {
                        break;
                    }
        }
    }

    // Sort results
    match sort {
        SortOrder::Asc => results.sort_by(|a, b| {
            a.dist
                .partial_cmp(&b.dist)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        SortOrder::Desc => results.sort_by(|a, b| {
            b.dist
                .partial_cmp(&a.dist)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
        SortOrder::None => {}
    }

    // Apply COUNT limit after sorting (unless ANY was used)
    if !any
        && let Some(c) = count {
            results.truncate(c);
        }

    results
}

enum SearchShape {
    Radius(f64),    // radius in meters
    Box(f64, f64),  // width, height in meters
}

#[derive(Clone, Copy)]
enum SortOrder {
    None,
    Asc,
    Desc,
}

/// Format search results into a RespValue array, respecting WITHCOORD/WITHDIST/WITHHASH flags.
fn format_geo_results(
    results: &[GeoResult],
    withcoord: bool,
    withdist: bool,
    withhash: bool,
) -> RespValue {
    let items: Vec<RespValue> = results
        .iter()
        .map(|r| {
            if !withcoord && !withdist && !withhash {
                RespValue::BulkString(Some(r.member.clone()))
            } else {
                let mut parts: Vec<RespValue> = Vec::new();
                parts.push(RespValue::BulkString(Some(r.member.clone())));
                if withdist {
                    let dist_str = format!("{:.4}", r.dist);
                    parts.push(RespValue::BulkString(Some(dist_str.into_bytes())));
                }
                if withhash {
                    parts.push(RespValue::Integer(r.hash as i64));
                }
                if withcoord {
                    let lon_str = format!("{:.6}", r.lon);
                    let lat_str = format!("{:.6}", r.lat);
                    parts.push(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(lon_str.into_bytes())),
                        RespValue::BulkString(Some(lat_str.into_bytes())),
                    ])));
                }
                RespValue::Array(Some(parts))
            }
        })
        .collect();

    RespValue::Array(Some(items))
}

// --- Command Implementations ---

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
pub async fn geoadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let mut idx = 1;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    // Parse optional flags
    while idx < args.len() {
        let flag = bytes_to_string(&args[idx])?.to_uppercase();
        match flag.as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "CH" => {
                ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    if nx && xx {
        return Err(CommandError::InvalidArgument(
            "XX and NX options at the same time are not compatible".to_string(),
        ));
    }

    // Remaining args must be triples: longitude latitude member
    let remaining = args.len() - idx;
    if remaining == 0 || !remaining.is_multiple_of(3) {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Parse and validate all geo entries before entering the atomic section
    let mut entries = Vec::new();
    while idx + 2 < args.len() {
        let lon = parse_float_arg(&args[idx], "longitude")?;
        let lat = parse_float_arg(&args[idx + 1], "latitude")?;
        let member = args[idx + 2].clone();
        idx += 3;

        validate_longitude(lon)?;
        validate_latitude(lat)?;

        let score = geohash_encode(lon, lat) as f64;
        entries.push((score, member));
    }

    let result = engine.atomic_modify(key, RedisDataType::ZSet, |current| {
        let mut ss: SortedSet = match current {
            Some(data) => {
                let ss_data = bincode::deserialize::<SortedSetData>(data)
                    .map_err(|_| crate::storage::error::StorageError::WrongType)?;
                ss_data.entries.into_iter()
                    .map(|(score, member)| (score.into_inner(), member))
                    .collect()
            }
            None => Vec::new(),
        };

        let mut added: i64 = 0;
        let mut changed: i64 = 0;

        for (score, member) in &entries {
            if let Some(pos) = ss.iter().position(|(_, m)| m == member) {
                if nx {
                    continue;
                }
                let old_score = ss[pos].0;
                ss[pos].0 = *score;
                if ch && (old_score - score).abs() > f64::EPSILON {
                    changed += 1;
                }
            } else {
                if xx {
                    continue;
                }
                ss.push((*score, member.clone()));
                added += 1;
            }
        }

        ss.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });

        let mut ss_data = SortedSetData::new();
        for (score, member) in &ss {
            ss_data.insert(*score, member.clone());
        }
        let serialized = bincode::serialize(&ss_data)
            .map_err(|e| crate::storage::error::StorageError::InternalError(format!("Serialization error: {}", e)))?;

        let result = if ch { added + changed } else { added };
        Ok((Some(serialized), result))
    })?;

    Ok(RespValue::Integer(result))
}

/// GEODIST key member1 member2 [M|KM|FT|MI]
pub async fn geodist(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let member1 = &args[1];
    let member2 = &args[2];
    let unit = if args.len() == 4 {
        parse_unit(&bytes_to_string(&args[3])?)?
    } else {
        "m".to_string()
    };

    let ss = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::BulkString(None)),
    };

    let pos1 = ss.iter().find(|(_, m)| m == member1);
    let pos2 = ss.iter().find(|(_, m)| m == member2);

    match (pos1, pos2) {
        (Some((score1, _)), Some((score2, _))) => {
            let (lon1, lat1) = geohash_decode(*score1 as u64);
            let (lon2, lat2) = geohash_decode(*score2 as u64);
            let dist_m = haversine_distance(lon1, lat1, lon2, lat2);
            let dist = meters_to_unit(dist_m, &unit);
            let dist_str = format!("{:.4}", dist);
            Ok(RespValue::BulkString(Some(dist_str.into_bytes())))
        }
        _ => Ok(RespValue::BulkString(None)),
    }
}

/// GEOHASH key member [member ...]
pub async fn geohash(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let ss = load_sorted_set(engine, key).await?.unwrap_or_default();

    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|member| {
            match ss.iter().find(|(_, m)| m == member) {
                Some((score, _)) => {
                    let hash = *score as u64;
                    // Decode internal hash (Mercator range) back to lon/lat,
                    // then re-encode with standard geohash range [-90, 90] for lat
                    let (lon, lat) = geohash_decode(hash);
                    let hash_str = geohash_string_from_lonlat(lon, lat);
                    RespValue::BulkString(Some(hash_str.into_bytes()))
                }
                None => RespValue::BulkString(None),
            }
        })
        .collect();

    Ok(RespValue::Array(Some(results)))
}

/// GEOPOS key member [member ...]
pub async fn geopos(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let ss = load_sorted_set(engine, key).await?.unwrap_or_default();

    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|member| {
            match ss.iter().find(|(_, m)| m == member) {
                Some((score, _)) => {
                    let hash = *score as u64;
                    let (lon, lat) = geohash_decode(hash);
                    let lon_str = format!("{:.6}", lon);
                    let lat_str = format!("{:.6}", lat);
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(lon_str.into_bytes())),
                        RespValue::BulkString(Some(lat_str.into_bytes())),
                    ]))
                }
                None => RespValue::Array(None),
            }
        })
        .collect();

    Ok(RespValue::Array(Some(results)))
}

/// GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude
///   BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
///   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
pub async fn geosearch(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let ss = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let (center_lon, center_lat, shape, unit, sort, count, any, withcoord, withdist, withhash) =
        parse_geosearch_args(&args[1..], &ss)?;

    let results = search_members(
        &ss, center_lon, center_lat, &shape, &unit, count, any, sort,
    );

    Ok(format_geo_results(&results, withcoord, withdist, withhash))
}

/// GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT longitude latitude
///   BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
///   [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
pub async fn geosearchstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let dest = &args[0];
    let source = &args[1];

    // Lock both source and destination keys for cross-key atomicity
    let _guard = engine.lock_keys(&[source.clone(), dest.clone()]).await;

    let ss = match load_sorted_set(engine, source).await? {
        Some(ss) => ss,
        None => {
            // Delete destination if source is empty
            engine.del(dest).await?;
            return Ok(RespValue::Integer(0));
        }
    };

    // Check if STOREDIST is present
    let mut storedist = false;
    let mut filtered_args: Vec<Vec<u8>> = Vec::new();
    for arg in &args[2..] {
        let upper = bytes_to_string(arg)?.to_uppercase();
        if upper == "STOREDIST" {
            storedist = true;
        } else {
            filtered_args.push(arg.clone());
        }
    }

    let (center_lon, center_lat, shape, unit, sort, count, any, _, _, _) =
        parse_geosearch_args(&filtered_args, &ss)?;

    let results = search_members(
        &ss, center_lon, center_lat, &shape, &unit, count, any, sort,
    );

    // Build destination sorted set and write atomically
    let dest_entries: Vec<(f64, Vec<u8>)> = results
        .iter()
        .map(|r| {
            let score = if storedist { r.dist } else { r.hash as f64 };
            (score, r.member.clone())
        })
        .collect();

    let entry_count = dest_entries.len() as i64;

    engine.atomic_modify(dest, RedisDataType::ZSet, |_current| {
        // Overwrite destination entirely with search results
        if dest_entries.is_empty() {
            Ok((None, ()))
        } else {
            let mut ss_data = SortedSetData::new();
            for (score, member) in &dest_entries {
                ss_data.insert(*score, member.clone());
            }
            let serialized = bincode::serialize(&ss_data)
                .map_err(|e| crate::storage::error::StorageError::InternalError(format!("Serialization error: {}", e)))?;
            Ok((Some(serialized), ()))
        }
    })?;

    Ok(RespValue::Integer(entry_count))
}

/// GEORADIUS key longitude latitude radius M|KM|FT|MI
///   [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
pub async fn georadius(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let lon = parse_float_arg(&args[1], "longitude")?;
    let lat = parse_float_arg(&args[2], "latitude")?;
    let radius = parse_float_arg(&args[3], "radius")?;
    let unit = parse_unit(&bytes_to_string(&args[4])?)?;

    validate_longitude(lon)?;
    validate_latitude(lat)?;

    if radius < 0.0 {
        return Err(CommandError::InvalidArgument(
            "radius cannot be negative".to_string(),
        ));
    }

    let ss = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let (sort, count, any, withcoord, withdist, withhash, store_key, storedist_key) =
        parse_georadius_options(&args[5..])?;

    let radius_m = unit_to_meters(radius, &unit);
    let shape = SearchShape::Radius(radius_m);

    let results = search_members(&ss, lon, lat, &shape, &unit, count, any, sort);

    // Handle STORE / STOREDIST
    if let Some(sk) = &store_key {
        let dest_ss: SortedSet = results
            .iter()
            .map(|r| (r.hash as f64, r.member.clone()))
            .collect();
        let count = dest_ss.len() as i64;
        save_sorted_set(engine, sk, &dest_ss).await?;
        return Ok(RespValue::Integer(count));
    }
    if let Some(sk) = &storedist_key {
        let dest_ss: SortedSet = results
            .iter()
            .map(|r| (r.dist, r.member.clone()))
            .collect();
        let count = dest_ss.len() as i64;
        save_sorted_set(engine, sk, &dest_ss).await?;
        return Ok(RespValue::Integer(count));
    }

    Ok(format_geo_results(&results, withcoord, withdist, withhash))
}

/// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [options...]
pub async fn georadiusbymember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let member = &args[1];
    let radius = parse_float_arg(&args[2], "radius")?;
    let unit = parse_unit(&bytes_to_string(&args[3])?)?;

    if radius < 0.0 {
        return Err(CommandError::InvalidArgument(
            "radius cannot be negative".to_string(),
        ));
    }

    let ss = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    // Find member position
    let member_entry = ss
        .iter()
        .find(|(_, m)| m == member)
        .ok_or_else(|| CommandError::InvalidArgument("could not decode requested zset member".to_string()))?;
    let (lon, lat) = geohash_decode(member_entry.0 as u64);

    let (sort, count, any, withcoord, withdist, withhash, store_key, storedist_key) =
        parse_georadius_options(&args[4..])?;

    let radius_m = unit_to_meters(radius, &unit);
    let shape = SearchShape::Radius(radius_m);

    let results = search_members(&ss, lon, lat, &shape, &unit, count, any, sort);

    // Handle STORE / STOREDIST
    if let Some(sk) = &store_key {
        let dest_ss: SortedSet = results
            .iter()
            .map(|r| (r.hash as f64, r.member.clone()))
            .collect();
        let count = dest_ss.len() as i64;
        save_sorted_set(engine, sk, &dest_ss).await?;
        return Ok(RespValue::Integer(count));
    }
    if let Some(sk) = &storedist_key {
        let dest_ss: SortedSet = results
            .iter()
            .map(|r| (r.dist, r.member.clone()))
            .collect();
        let count = dest_ss.len() as i64;
        save_sorted_set(engine, sk, &dest_ss).await?;
        return Ok(RespValue::Integer(count));
    }

    Ok(format_geo_results(&results, withcoord, withdist, withhash))
}

// --- Argument Parsers ---

/// Parse GEOSEARCH style arguments (after key).
/// Returns (center_lon, center_lat, shape, unit, sort, count, any, withcoord, withdist, withhash)
#[allow(clippy::type_complexity)]
fn parse_geosearch_args(
    args: &[Vec<u8>],
    ss: &SortedSet,
) -> Result<(f64, f64, SearchShape, String, SortOrder, Option<usize>, bool, bool, bool, bool), CommandError> {
    let mut idx = 0;
    let mut center_lon: Option<f64> = None;
    let mut center_lat: Option<f64> = None;
    let mut shape: Option<SearchShape> = None;
    let mut unit = "m".to_string();
    let mut sort = SortOrder::None;
    let mut count: Option<usize> = None;
    let mut any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;

    while idx < args.len() {
        let upper = bytes_to_string(&args[idx])?.to_uppercase();
        match upper.as_str() {
            "FROMMEMBER" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let member = &args[idx];
                let entry = ss
                    .iter()
                    .find(|(_, m)| m == member)
                    .ok_or_else(|| CommandError::InvalidArgument("could not decode requested zset member".to_string()))?;
                let (lon, lat) = geohash_decode(entry.0 as u64);
                center_lon = Some(lon);
                center_lat = Some(lat);
                idx += 1;
            }
            "FROMLONLAT" => {
                idx += 1;
                if idx + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let lon = parse_float_arg(&args[idx], "longitude")?;
                let lat = parse_float_arg(&args[idx + 1], "latitude")?;
                validate_longitude(lon)?;
                validate_latitude(lat)?;
                center_lon = Some(lon);
                center_lat = Some(lat);
                idx += 2;
            }
            "BYRADIUS" => {
                idx += 1;
                if idx + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let radius = parse_float_arg(&args[idx], "radius")?;
                if radius < 0.0 {
                    return Err(CommandError::InvalidArgument(
                        "radius cannot be negative".to_string(),
                    ));
                }
                idx += 1;
                unit = parse_unit(&bytes_to_string(&args[idx])?)?;
                let radius_m = unit_to_meters(radius, &unit);
                shape = Some(SearchShape::Radius(radius_m));
                idx += 1;
            }
            "BYBOX" => {
                idx += 1;
                if idx + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let width = parse_float_arg(&args[idx], "width")?;
                let height = parse_float_arg(&args[idx + 1], "height")?;
                if width < 0.0 || height < 0.0 {
                    return Err(CommandError::InvalidArgument(
                        "width or height cannot be negative".to_string(),
                    ));
                }
                idx += 2;
                unit = parse_unit(&bytes_to_string(&args[idx])?)?;
                let width_m = unit_to_meters(width, &unit);
                let height_m = unit_to_meters(height, &unit);
                shape = Some(SearchShape::Box(width_m, height_m));
                idx += 1;
            }
            "ASC" => {
                sort = SortOrder::Asc;
                idx += 1;
            }
            "DESC" => {
                sort = SortOrder::Desc;
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let c = bytes_to_string(&args[idx])?
                    .parse::<usize>()
                    .map_err(|_| CommandError::NotANumber)?;
                count = Some(c);
                idx += 1;
                // Check for ANY
                if idx < args.len() {
                    let next = bytes_to_string(&args[idx])?.to_uppercase();
                    if next == "ANY" {
                        any = true;
                        idx += 1;
                    }
                }
            }
            "WITHCOORD" => {
                withcoord = true;
                idx += 1;
            }
            "WITHDIST" => {
                withdist = true;
                idx += 1;
            }
            "WITHHASH" => {
                withhash = true;
                idx += 1;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option '{}'",
                    upper
                )));
            }
        }
    }

    let center_lon = center_lon
        .ok_or_else(|| CommandError::InvalidArgument("exactly one of FROMMEMBER or FROMLONLAT must be provided".to_string()))?;
    let center_lat = center_lat.unwrap(); // always set with center_lon

    let shape = shape
        .ok_or_else(|| CommandError::InvalidArgument("exactly one of BYRADIUS or BYBOX must be provided".to_string()))?;

    Ok((center_lon, center_lat, shape, unit, sort, count, any, withcoord, withdist, withhash))
}

/// Parse GEORADIUS optional arguments.
/// Returns (sort, count, any, withcoord, withdist, withhash, store_key, storedist_key)
#[allow(clippy::type_complexity)]
fn parse_georadius_options(
    args: &[Vec<u8>],
) -> Result<(SortOrder, Option<usize>, bool, bool, bool, bool, Option<Vec<u8>>, Option<Vec<u8>>), CommandError> {
    let mut sort = SortOrder::None;
    let mut count: Option<usize> = None;
    let mut any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;
    let mut store_key: Option<Vec<u8>> = None;
    let mut storedist_key: Option<Vec<u8>> = None;
    let mut idx = 0;

    while idx < args.len() {
        let upper = bytes_to_string(&args[idx])?.to_uppercase();
        match upper.as_str() {
            "ASC" => {
                sort = SortOrder::Asc;
                idx += 1;
            }
            "DESC" => {
                sort = SortOrder::Desc;
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let c = bytes_to_string(&args[idx])?
                    .parse::<usize>()
                    .map_err(|_| CommandError::NotANumber)?;
                count = Some(c);
                idx += 1;
                if idx < args.len() {
                    let next = bytes_to_string(&args[idx])?.to_uppercase();
                    if next == "ANY" {
                        any = true;
                        idx += 1;
                    }
                }
            }
            "WITHCOORD" => {
                withcoord = true;
                idx += 1;
            }
            "WITHDIST" => {
                withdist = true;
                idx += 1;
            }
            "WITHHASH" => {
                withhash = true;
                idx += 1;
            }
            "STORE" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                store_key = Some(args[idx].clone());
                idx += 1;
            }
            "STOREDIST" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                storedist_key = Some(args[idx].clone());
                idx += 1;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option '{}'",
                    upper
                )));
            }
        }
    }

    Ok((sort, count, any, withcoord, withdist, withhash, store_key, storedist_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    async fn setup() -> Arc<StorageEngine> {
        let config = StorageConfig::default();
        StorageEngine::new(config)
    }

    // Helper to add geo members
    async fn geoadd_helper(engine: &StorageEngine, key: &str, items: &[(f64, f64, &str)]) {
        let mut args: Vec<Vec<u8>> = vec![key.as_bytes().to_vec()];
        for (lon, lat, member) in items {
            args.push(format!("{}", lon).into_bytes());
            args.push(format!("{}", lat).into_bytes());
            args.push(member.as_bytes().to_vec());
        }
        geoadd(engine, &args).await.unwrap();
    }

    // --- Geohash encoding/decoding tests ---

    #[test]
    fn test_geohash_encode_decode_roundtrip() {
        // Test several known coordinates
        let cases = vec![
            (13.361389, 38.115556),   // Palermo
            (15.087269, 37.502669),   // Catania
            (-122.4194, 37.7749),     // San Francisco
            (0.0, 0.0),              // Null Island
            (-179.9, 0.0),           // Near extreme west
            (179.9, 0.0),            // Near extreme east
        ];

        for (lon, lat) in cases {
            let hash = geohash_encode(lon, lat);
            let (decoded_lon, decoded_lat) = geohash_decode(hash);
            // Precision should be within ~0.001 degrees
            assert!(
                (decoded_lon - lon).abs() < 0.001,
                "Longitude mismatch for ({}, {}): got {}",
                lon, lat, decoded_lon
            );
            assert!(
                (decoded_lat - lat).abs() < 0.001,
                "Latitude mismatch for ({}, {}): got {}",
                lon, lat, decoded_lat
            );
        }
    }

    #[test]
    fn test_geohash_to_string_length() {
        let hash = geohash_encode(13.361389, 38.115556);
        let s = geohash_to_string(hash);
        assert_eq!(s.len(), 11);
        // All chars should be from the base32 alphabet
        for c in s.chars() {
            assert!(
                BASE32_ALPHABET.contains(&(c as u8)),
                "Invalid base32 char: {}",
                c
            );
        }
    }

    #[test]
    fn test_haversine_distance() {
        // Palermo to Catania: approx 166 km
        let dist = haversine_distance(13.361389, 38.115556, 15.087269, 37.502669);
        let dist_km = dist / 1000.0;
        assert!(
            (dist_km - 166.274).abs() < 1.0,
            "Distance should be ~166 km, got {}",
            dist_km
        );
    }

    #[test]
    fn test_meters_to_unit_conversions() {
        assert!((meters_to_unit(1000.0, "m") - 1000.0).abs() < f64::EPSILON);
        assert!((meters_to_unit(1000.0, "km") - 1.0).abs() < f64::EPSILON);
        assert!((meters_to_unit(1609.34, "mi") - 1.0).abs() < 0.001);
        assert!((meters_to_unit(0.3048, "ft") - 1.0).abs() < 0.001);
    }

    // --- GEOADD tests ---

    #[tokio::test]
    async fn test_geoadd_basic() {
        let engine = setup().await;

        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"13.361389".to_vec(),
                b"38.115556".to_vec(),
                b"Palermo".to_vec(),
                b"15.087269".to_vec(),
                b"37.502669".to_vec(),
                b"Catania".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_geoadd_update_existing() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // Add same member again with different coords - should not increment count
        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"15.0".to_vec(),
                b"37.0".to_vec(),
                b"Palermo".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::Integer(0)); // 0 added (updated)
    }

    #[tokio::test]
    async fn test_geoadd_nx_flag() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // NX should skip existing member
        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"NX".to_vec(),
                b"15.0".to_vec(),
                b"37.0".to_vec(),
                b"Palermo".to_vec(),
                b"12.0".to_vec(),
                b"42.0".to_vec(),
                b"Rome".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::Integer(1)); // Only Rome added
    }

    #[tokio::test]
    async fn test_geoadd_xx_flag() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // XX should only update existing, skip new
        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"XX".to_vec(),
                b"15.0".to_vec(),
                b"37.0".to_vec(),
                b"Palermo".to_vec(),
                b"12.0".to_vec(),
                b"42.0".to_vec(),
                b"Rome".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::Integer(0)); // 0 added (Palermo updated, Rome skipped)
    }

    #[tokio::test]
    async fn test_geoadd_ch_flag() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // CH should count changed elements
        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"CH".to_vec(),
                b"15.0".to_vec(),
                b"37.0".to_vec(),
                b"Palermo".to_vec(),
                b"12.0".to_vec(),
                b"42.0".to_vec(),
                b"Rome".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::Integer(2)); // 1 changed + 1 added
    }

    #[tokio::test]
    async fn test_geoadd_invalid_coordinates() {
        let engine = setup().await;

        // Invalid longitude
        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"200.0".to_vec(),
                b"38.0".to_vec(),
                b"Invalid".to_vec(),
            ],
        )
        .await;
        assert!(result.is_err());

        // Invalid latitude (beyond Mercator limit)
        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"13.0".to_vec(),
                b"90.0".to_vec(),
                b"Invalid".to_vec(),
            ],
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_geoadd_wrong_args() {
        let engine = setup().await;

        // Too few args
        let result = geoadd(&engine, &[b"key".to_vec()]).await;
        assert!(result.is_err());

        // Wrong number of triple args
        let result = geoadd(
            &engine,
            &[b"key".to_vec(), b"13.0".to_vec(), b"38.0".to_vec()],
        )
        .await;
        assert!(result.is_err());
    }

    // --- GEODIST tests ---

    #[tokio::test]
    async fn test_geodist_basic() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        let result = geodist(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"Catania".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::BulkString(Some(data)) = result {
            let dist: f64 = String::from_utf8(data).unwrap().parse().unwrap();
            assert!(
                (dist - 166.274).abs() < 1.0,
                "Distance should be ~166 km, got {}",
                dist
            );
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_geodist_default_unit() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        // No unit = meters
        let result = geodist(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"Catania".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::BulkString(Some(data)) = result {
            let dist: f64 = String::from_utf8(data).unwrap().parse().unwrap();
            assert!(dist > 100000.0, "Distance in meters should be > 100000");
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_geodist_missing_member() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        let result = geodist(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"NonExistent".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_geodist_missing_key() {
        let engine = setup().await;

        let result = geodist(
            &engine,
            &[
                b"nokey".to_vec(),
                b"A".to_vec(),
                b"B".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::BulkString(None));
    }

    // --- GEOHASH tests ---

    #[tokio::test]
    async fn test_geohash_basic() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        let result = geohash(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"Catania".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            // Each should be an 11-char base32 string
            for item in &items {
                if let RespValue::BulkString(Some(data)) = item {
                    assert_eq!(data.len(), 11);
                } else {
                    panic!("Expected BulkString");
                }
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geohash_missing_member() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        let result = geohash(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"NonExistent".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], RespValue::BulkString(Some(_))));
            assert_eq!(items[1], RespValue::BulkString(None));
        } else {
            panic!("Expected Array");
        }
    }

    // --- GEOPOS tests ---

    #[tokio::test]
    async fn test_geopos_basic() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[(13.361389, 38.115556, "Palermo")],
        )
        .await;

        let result = geopos(
            &engine,
            &[b"mygeo".to_vec(), b"Palermo".to_vec()],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 1);
            if let RespValue::Array(Some(coords)) = &items[0] {
                assert_eq!(coords.len(), 2);
                if let RespValue::BulkString(Some(lon_bytes)) = &coords[0] {
                    let lon: f64 = String::from_utf8(lon_bytes.clone()).unwrap().parse().unwrap();
                    assert!((lon - 13.361389).abs() < 0.001);
                }
                if let RespValue::BulkString(Some(lat_bytes)) = &coords[1] {
                    let lat: f64 = String::from_utf8(lat_bytes.clone()).unwrap().parse().unwrap();
                    assert!((lat - 38.115556).abs() < 0.001);
                }
            } else {
                panic!("Expected inner array");
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geopos_missing_member() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        let result = geopos(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"NonExistent".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], RespValue::Array(Some(_))));
            assert_eq!(items[1], RespValue::Array(None));
        } else {
            panic!("Expected Array");
        }
    }

    // --- GEOSEARCH tests ---

    #[tokio::test]
    async fn test_geosearch_by_radius_fromlonlat() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (2.349014, 48.864716, "Paris"),
            ],
        )
        .await;

        // Search within 200 km from a point near Sicily
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            // Should find Palermo and Catania, not Paris
            assert_eq!(items.len(), 2);
            let members: Vec<String> = items
                .iter()
                .filter_map(|i| {
                    if let RespValue::BulkString(Some(d)) = i {
                        Some(String::from_utf8(d.clone()).unwrap())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(members.contains(&"Palermo".to_string()));
            assert!(members.contains(&"Catania".to_string()));
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geosearch_by_radius_frommember() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (2.349014, 48.864716, "Paris"),
            ],
        )
        .await;

        // Search within 200 km from Palermo
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"FROMMEMBER".to_vec(),
                b"Palermo".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // Palermo itself + Catania
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geosearch_by_box() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (2.349014, 48.864716, "Paris"),
            ],
        )
        .await;

        // Search within a 400x200 km box from a point near Sicily
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYBOX".to_vec(),
                b"400".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert!(items.len() >= 1, "Should find at least Palermo or Catania within box");
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geosearch_with_options() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        // Search with WITHCOORD, WITHDIST, WITHHASH
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"WITHCOORD".to_vec(),
                b"WITHDIST".to_vec(),
                b"WITHHASH".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            // Each result should be an array with [member, dist, hash, [lon, lat]]
            for item in &items {
                if let RespValue::Array(Some(parts)) = item {
                    assert_eq!(parts.len(), 4); // member + dist + hash + coord
                } else {
                    panic!("Expected inner array");
                }
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geosearch_count() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (14.0, 37.5, "Enna"),
            ],
        )
        .await;

        // Search with COUNT 1
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"COUNT".to_vec(),
                b"1".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 1);
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_geosearch_nonexistent_key() {
        let engine = setup().await;

        let result = geosearch(
            &engine,
            &[
                b"nokey".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // --- GEOSEARCHSTORE tests ---

    #[tokio::test]
    async fn test_geosearchstore_basic() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (2.349014, 48.864716, "Paris"),
            ],
        )
        .await;

        let result = geosearchstore(
            &engine,
            &[
                b"dest".to_vec(),
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Integer(count) = result {
            assert_eq!(count, 2); // Palermo and Catania
        } else {
            panic!("Expected Integer");
        }

        // Verify destination exists
        let ss = load_sorted_set(&engine, b"dest").await.unwrap();
        assert!(ss.is_some());
        assert_eq!(ss.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_geosearchstore_storedist() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        let result = geosearchstore(
            &engine,
            &[
                b"dest".to_vec(),
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"STOREDIST".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Integer(count) = result {
            assert_eq!(count, 2);
        } else {
            panic!("Expected Integer");
        }

        // Stored scores should be distances, not geohashes
        let ss = load_sorted_set(&engine, b"dest").await.unwrap().unwrap();
        for (score, _) in &ss {
            // Distance in km should be small (< 200)
            assert!(*score < 200.0, "Score should be distance in km, got {}", score);
        }
    }

    // --- GEORADIUS tests ---

    #[tokio::test]
    async fn test_georadius_basic() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (2.349014, 48.864716, "Paris"),
            ],
        )
        .await;

        let result = georadius(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_georadius_with_store() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        let result = georadius(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"STORE".to_vec(),
                b"dest".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Integer(count) = result {
            assert_eq!(count, 2);
        } else {
            panic!("Expected Integer");
        }
    }

    #[tokio::test]
    async fn test_georadius_withdist() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        let result = georadius(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"WITHDIST".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            // Each should be [member, dist]
            for item in &items {
                if let RespValue::Array(Some(parts)) = item {
                    assert_eq!(parts.len(), 2);
                } else {
                    panic!("Expected inner array");
                }
            }
        } else {
            panic!("Expected Array");
        }
    }

    // --- GEORADIUSBYMEMBER tests ---

    #[tokio::test]
    async fn test_georadiusbymember_basic() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
                (2.349014, 48.864716, "Paris"),
            ],
        )
        .await;

        let result = georadiusbymember(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
                b"ASC".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // Palermo + Catania
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_georadiusbymember_nonexistent_member() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        let result = georadiusbymember(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"NonExistent".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await;

        assert!(result.is_err());
    }

    // --- Interoperability with sorted set commands ---

    #[tokio::test]
    async fn test_geo_data_is_sorted_set() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // Geo data should be accessible as a sorted set
        let data_type = engine.get_raw_data_type(b"mygeo").await.unwrap();
        assert_eq!(data_type, "zset");
    }

    // --- Edge cases ---

    #[tokio::test]
    async fn test_geoadd_nx_xx_conflict() {
        let engine = setup().await;

        let result = geoadd(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"NX".to_vec(),
                b"XX".to_vec(),
                b"13.0".to_vec(),
                b"38.0".to_vec(),
                b"Place".to_vec(),
            ],
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_geodist_invalid_unit() {
        let engine = setup().await;

        geoadd_helper(
            &engine,
            "mygeo",
            &[
                (13.361389, 38.115556, "Palermo"),
                (15.087269, 37.502669, "Catania"),
            ],
        )
        .await;

        let result = geodist(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"Palermo".to_vec(),
                b"Catania".to_vec(),
                b"invalid".to_vec(),
            ],
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_geosearch_missing_center() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // Missing FROMMEMBER/FROMLONLAT
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"BYRADIUS".to_vec(),
                b"200".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_geosearch_missing_shape() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        // Missing BYRADIUS/BYBOX
        let result = geosearch(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
            ],
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_georadius_negative_radius() {
        let engine = setup().await;

        geoadd_helper(&engine, "mygeo", &[(13.361389, 38.115556, "Palermo")]).await;

        let result = georadius(
            &engine,
            &[
                b"mygeo".to_vec(),
                b"14.0".to_vec(),
                b"38.0".to_vec(),
                b"-100".to_vec(),
                b"km".to_vec(),
            ],
        )
        .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_geohash_known_reference_values() {
        // Paris (2.3522, 48.8566) — should start with "u09t"
        let paris_str = geohash_string_from_lonlat(2.3522, 48.8566);
        assert!(
            paris_str.starts_with("u09t"),
            "Paris geohash should start with 'u09t', got '{}'",
            paris_str
        );

        // Rome (12.4964, 41.9028) — should start with "sr2y"
        let rome_str = geohash_string_from_lonlat(12.4964, 41.9028);
        assert!(
            rome_str.starts_with("sr2y"),
            "Rome geohash should start with 'sr2y', got '{}'",
            rome_str
        );

        // New York (-74.006, 40.7128) — should start with "dr5r"
        let ny_str = geohash_string_from_lonlat(-74.006, 40.7128);
        assert!(
            ny_str.starts_with("dr5r"),
            "New York geohash should start with 'dr5r', got '{}'",
            ny_str
        );
    }

    #[test]
    fn test_geohash_interleave_convention() {
        // Verify standard geohash string encoding produces correct results
        // Palermo (13.361389, 38.115556) — Redis GEOHASH reference: starts with "sqc8b"
        let hash_str = geohash_string_from_lonlat(13.361389, 38.115556);
        assert!(
            hash_str.starts_with("sqc8b"),
            "Palermo geohash should start with 'sqc8b', got '{}'",
            hash_str
        );

        // Catania (15.087269, 37.502669) — Redis GEOHASH reference: starts with "sqdtr"
        let hash_str = geohash_string_from_lonlat(15.087269, 37.502669);
        assert!(
            hash_str.starts_with("sqdtr"),
            "Catania geohash should start with 'sqdtr', got '{}'",
            hash_str
        );
    }

    #[tokio::test]
    async fn test_geohash_accuracy_via_command() {
        let engine = setup().await;

        // Add known cities
        geoadd_helper(
            &engine,
            "cities",
            &[
                (2.3522, 48.8566, "Paris"),
                (12.4964, 41.9028, "Rome"),
                (-74.006, 40.7128, "NewYork"),
            ],
        )
        .await;

        let result = geohash(
            &engine,
            &[
                b"cities".to_vec(),
                b"Paris".to_vec(),
                b"Rome".to_vec(),
                b"NewYork".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);

            // Paris should start with "u09t"
            if let RespValue::BulkString(Some(data)) = &items[0] {
                let s = String::from_utf8_lossy(data);
                assert!(s.starts_with("u09t"), "Paris: expected 'u09t...', got '{}'", s);
            } else {
                panic!("Expected BulkString for Paris");
            }

            // Rome should start with "sr2y"
            if let RespValue::BulkString(Some(data)) = &items[1] {
                let s = String::from_utf8_lossy(data);
                assert!(s.starts_with("sr2y"), "Rome: expected 'sr2y...', got '{}'", s);
            } else {
                panic!("Expected BulkString for Rome");
            }

            // New York should start with "dr5r"
            if let RespValue::BulkString(Some(data)) = &items[2] {
                let s = String::from_utf8_lossy(data);
                assert!(s.starts_with("dr5r"), "NewYork: expected 'dr5r...', got '{}'", s);
            } else {
                panic!("Expected BulkString for NewYork");
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_concurrent_geoadd_same_key() {
        let engine = StorageEngine::new(StorageConfig::default());
        let key = b"geotest:concurrent".to_vec();

        // 10 tasks, each adding 10 unique members
        let num_tasks = 10usize;
        let members_per_task = 10usize;

        let mut handles = Vec::new();
        for task_id in 0..num_tasks {
            let engine_clone = engine.clone();
            let key_clone = key.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..members_per_task {
                    let lon = -180.0 + ((task_id * members_per_task + i) as f64 * 3.0) % 360.0;
                    let lat = -85.0 + ((task_id * members_per_task + i) as f64 * 1.5) % 170.0;
                    let member = format!("member_{}_{}", task_id, i);
                    let args: Vec<Vec<u8>> = vec![
                        key_clone.clone(),
                        format!("{}", lon).into_bytes(),
                        format!("{}", lat).into_bytes(),
                        member.into_bytes(),
                    ];
                    let result = geoadd(&engine_clone, &args).await;
                    assert!(result.is_ok(), "GEOADD failed: {:?}", result);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Verify all members were added - use ZCARD equivalent via load_sorted_set
        let ss = load_sorted_set(&engine, &key).await.unwrap().unwrap();
        assert_eq!(
            ss.len(),
            num_tasks * members_per_task,
            "Expected {} members, got {}",
            num_tasks * members_per_task,
            ss.len()
        );
    }

    #[tokio::test]
    async fn test_concurrent_geoadd_with_updates() {
        let engine = StorageEngine::new(StorageConfig::default());
        let key = b"geotest:concurrent_update".to_vec();

        // First add a member
        let args: Vec<Vec<u8>> = vec![
            key.clone(),
            b"2.3522".to_vec(),
            b"48.8566".to_vec(),
            b"Paris".to_vec(),
        ];
        geoadd(&engine, &args).await.unwrap();

        // 10 tasks all updating the same member concurrently
        let num_tasks = 10usize;
        let mut handles = Vec::new();
        for task_id in 0..num_tasks {
            let engine_clone = engine.clone();
            let key_clone = key.clone();
            handles.push(tokio::spawn(async move {
                let lon = 2.0 + task_id as f64 * 0.1;
                let lat = 48.0 + task_id as f64 * 0.1;
                let args: Vec<Vec<u8>> = vec![
                    key_clone,
                    format!("{}", lon).into_bytes(),
                    format!("{}", lat).into_bytes(),
                    b"Paris".to_vec(),
                ];
                geoadd(&engine_clone, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Verify only 1 member exists (updates, not new adds)
        let ss = load_sorted_set(&engine, &key).await.unwrap().unwrap();
        assert_eq!(ss.len(), 1, "Should have exactly 1 member after updates");
    }
}
