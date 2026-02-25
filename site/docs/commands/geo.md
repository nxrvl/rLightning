# Geospatial Commands

Geospatial commands store and query geographic coordinates. Internally, geo data is stored in sorted sets using geohash-encoded scores.

## Commands

### GEOADD

Add geospatial items (longitude, latitude, member name) to a key.

```bash
GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
```

Options:

- `NX`: Only add new elements
- `XX`: Only update existing elements
- `CH`: Return number of changed elements

```bash
GEOADD locations -122.4194 37.7749 "San Francisco"
GEOADD locations -118.2437 34.0522 "Los Angeles" -73.9857 40.7484 "New York"
# (integer) 2
```

### GEODIST

Return the distance between two members.

```bash
GEODIST key member1 member2 [M|KM|MI|FT]
```

Units: `m` (meters, default), `km`, `mi` (miles), `ft` (feet)

```bash
GEODIST locations "San Francisco" "Los Angeles" km
# "559.1174"
GEODIST locations "San Francisco" "New York" mi
# "2565.6478"
```

### GEOHASH

Return geohash strings for members.

```bash
GEOHASH key member [member ...]
```

```bash
GEOHASH locations "San Francisco"
# 1) "9q8yyk8yuv0"
```

### GEOPOS

Return longitude and latitude of members.

```bash
GEOPOS key member [member ...]
```

```bash
GEOPOS locations "San Francisco" "Los Angeles"
# 1) 1) "-122.41940..."
#    2) "37.77490..."
# 2) 1) "-118.24370..."
#    2) "34.05220..."
```

### GEOSEARCH

Search for members within a geographic area. Replaces the deprecated GEORADIUS command.

```bash
GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude
  BYRADIUS radius M|KM|MI|FT | BYBOX width height M|KM|MI|FT
  [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
```

```bash
# Find locations within 600km of San Francisco
GEOSEARCH locations FROMMEMBER "San Francisco" BYRADIUS 600 km ASC WITHDIST
# 1) 1) "San Francisco"
#    2) "0.0000"
# 2) 1) "Los Angeles"
#    2) "559.1174"

# Find locations within a box around coordinates
GEOSEARCH locations FROMLONLAT -100 40 BYBOX 5000 3000 km COUNT 10 ASC
```

### GEOSEARCHSTORE

Store the results of a GEOSEARCH into a destination key.

```bash
GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT longitude latitude
  BYRADIUS radius M|KM|MI|FT | BYBOX width height M|KM|MI|FT
  [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
```

```bash
GEOSEARCHSTORE nearby locations FROMLONLAT -122.4 37.8 BYRADIUS 100 km ASC
```

### GEORADIUS (Deprecated)

Search by radius from coordinates. Use GEOSEARCH instead.

```bash
GEORADIUS key longitude latitude radius M|KM|MI|FT [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
```

### GEORADIUSBYMEMBER (Deprecated)

Search by radius from a member. Use GEOSEARCH instead.

```bash
GEORADIUSBYMEMBER key member radius M|KM|MI|FT [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
```

## Use Cases

### Store Locator

```bash
GEOADD stores -122.4 37.78 "Store A" -122.41 37.77 "Store B" -122.39 37.79 "Store C"
GEOSEARCH stores FROMLONLAT -122.405 37.775 BYRADIUS 2 km ASC COUNT 5 WITHDIST
```

### Ride-Sharing / Delivery

```bash
GEOADD drivers -122.42 37.77 "driver:1" -122.41 37.78 "driver:2"
GEOSEARCH drivers FROMLONLAT -122.415 37.775 BYRADIUS 1 km ASC COUNT 3 WITHDIST
```
