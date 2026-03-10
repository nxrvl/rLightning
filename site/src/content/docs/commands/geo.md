---
title: "Geospatial"
description: "Geospatial command reference"
order: 10
category: "commands"
---

# Geospatial Commands

Geospatial commands store longitude/latitude coordinates and enable radius-based and bounding-box queries. Internally, coordinates are stored as sorted set scores using geohash encoding.

## GEOADD

Synopsis: `GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member ...]`

Add one or more geospatial items (longitude, latitude, name) to a sorted set. Returns the number of new elements added.

- `NX` -- Only add new elements, do not update existing.
- `XX` -- Only update existing elements, do not add new.
- `CH` -- Return the number of changed (added + updated) elements.

```bash
> GEOADD places -122.4194 37.7749 "San Francisco" -73.9857 40.7484 "New York"
(integer) 2
> GEOADD places -0.1278 51.5074 "London"
(integer) 1
```

## GEODIST

Synopsis: `GEODIST key member1 member2 [M | KM | FT | MI]`

Return the distance between two members. The default unit is meters.

```bash
> GEOADD places -122.4194 37.7749 "SF" -73.9857 40.7484 "NYC"
(integer) 2
> GEODIST places "SF" "NYC" km
"4138.5355"
> GEODIST places "SF" "NYC" mi
"2571.0561"
```

## GEOHASH

Synopsis: `GEOHASH key member [member ...]`

Return the geohash strings of one or more members.

```bash
> GEOADD places -122.4194 37.7749 "SF"
(integer) 1
> GEOHASH places "SF"
1) "9q8yyk8yuv0"
```

## GEOPOS

Synopsis: `GEOPOS key member [member ...]`

Return the longitude and latitude of one or more members. Returns nil for members that do not exist.

```bash
> GEOADD places -122.4194 37.7749 "SF"
(integer) 1
> GEOPOS places "SF"
1) 1) "-122.41940140724182"
   2) "37.77490042908397"
```

## GEOSEARCH

Synopsis: `GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude BYRADIUS radius M | KM | FT | MI | BYBOX width height M | KM | FT | MI [ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`

Search for members within a geographic area (circle or rectangle). Available since Redis 6.2. Replaces GEORADIUS and GEORADIUSBYMEMBER.

```bash
> GEOADD places -122.4194 37.7749 "SF" -122.2727 37.8044 "Oakland" -73.9857 40.7484 "NYC"
(integer) 3
> GEOSEARCH places FROMLONLAT -122.4 37.78 BYRADIUS 50 km ASC
1) "SF"
2) "Oakland"
> GEOSEARCH places FROMMEMBER "SF" BYRADIUS 30 km WITHDIST ASC COUNT 2
1) 1) "SF"
   2) "0.0000"
2) 1) "Oakland"
   2) "13.3178"
```

## GEOSEARCHSTORE

Synopsis: `GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT longitude latitude BYRADIUS radius M | KM | FT | MI | BYBOX width height M | KM | FT | MI [ASC | DESC] [COUNT count [ANY]] [STOREDIST]`

Store the results of a GEOSEARCH into a destination key. With `STOREDIST`, stores distances instead of geohashes as scores. Returns the number of elements stored.

```bash
> GEOSEARCHSTORE nearby places FROMLONLAT -122.4 37.78 BYRADIUS 50 km ASC
(integer) 2
```

## GEORADIUS

Synopsis: `GEORADIUS key longitude latitude radius M | KM | FT | MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC] [STORE key] [STOREDIST key]`

Search for members within a given radius from a point. Deprecated in Redis 6.2; use GEOSEARCH instead.

```bash
> GEORADIUS places -122.4 37.78 50 km ASC WITHDIST
1) 1) "SF"
   2) "2.1645"
2) 1) "Oakland"
   2) "13.5491"
```

## GEORADIUSBYMEMBER

Synopsis: `GEORADIUSBYMEMBER key member radius M | KM | FT | MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC] [STORE key] [STOREDIST key]`

Search for members within a given radius from another member. Deprecated in Redis 6.2; use GEOSEARCH with FROMMEMBER instead.

```bash
> GEORADIUSBYMEMBER places "SF" 30 km ASC
1) "SF"
2) "Oakland"
```
