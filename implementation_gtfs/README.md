# GTFS Line Integration Tool

A Python tool for integrating new transit lines into an existing GTFS dataset. This project allows you to easily add new lines, extend existing lines, and create forks/branches in a GTFS dataset.

## Features

- **Add new lines**: Complete integration of new transit lines
- **Extend existing lines**: Extension of existing routes
- **Create branches**: Add forks to existing lines with custom frequencies
- **Automatic validation**: Data consistency verification

## Prerequisites

```bash
pip install pandas geopandas polars geopy scikit-learn scipy matplotlib seaborn
```

## Installation and Usage

1. **Configuration**: Create a `config.json` file in the project directory
2. **Data preparation**: Prepare your CSV/SHP files according to the required format
3. **Execution**:
   ```bash
   python add_lines_gtfs.py [config.json]
   ```
   If no file is specified, the script will use `config.json` by default.

## Configuration (config.json)

```json
{
    "stations": "data/clean_data/stations.csv",
    "lines": "data/clean_data/lines.csv", 
    "journey_time": "data/clean_data/journey_time.csv",
    "gtfs_data": "data/IDFM-gtfs",
    "dest_folder": "GTFS_versions/GTFS_completed",
    "forks": "data/clean_data/forks.csv",
    "agency": "GPE",
    "suffix": "GPE"
}
```

### Required parameters:
- `stations`: Path to the stations file
- `lines`: Path to the lines file
- `journey_time`: Path to the journey times file
- `gtfs_data`: Folder containing source GTFS data
- `dest_folder`: Destination folder for modified GTFS

### Optional parameters:
- `forks`: File for branch management
- `agency`: Agency name for new lines
- `suffix`: Suffix for generated identifiers (not currently used)

## Input Data Format

### Stations (CSV or Shapefile)

| name | latitude | longitude |
|------|----------|-----------|
| La Défense | 48.8918 | 2.2380 |
| Châtelet | 48.8566 | 2.3477 |

**Notes:**
- Latitude/longitude coordinates are not required for .shp files
- Station name must be unique
- Coordinate format: WGS84 (EPSG:4326)

### Lines

| name | type | frequency | agency | color |
|------|------|-----------|--------|-------|
| 18 | 1 | 42 | RATP | FF0000 |
| E | 2 | 12 | SNCF | 8F7EE6 |

**Required fields:**
- `name`: Line name/number
- `type`: Transport type according to GTFS standards

**Optional fields:**
- `frequency`: Trains per hour per direction (peak hours)
- `agency`: Operating agency
- `color`: Hexadecimal color of the line

#### GTFS Standards for Transport Types

| Code | Type |
|------|------|
| 0 | Tram |
| 1 | Subway |
| 2 | Rail |
| 3 | Bus |
| 4 | Ferry |
| 5 | Cable Car |
| 6 | Gondola |
| 7 | Funicular |
| 11 | Trolleybus |
| 12 | Monorail |

### Journey Time Between Stations

| line | departure | arrival | time | direction |
|------|-----------|---------|------|-----------|
| 16 | Chelles | Noisy-Champs | 178 | 0 |
| E | Haussmann | Châtelet | 120 | 1 |

**Format:**
- `line`: Line name (must match the lines file)
- `departure`: Departure station
- `arrival`: Arrival station
- `time`: Travel time in seconds
- `direction`: 0 = bidirectional, 1 = unidirectional

### Forks (optional)

| line | itinerary | frequency |
|------|-----------|-----------|
| E | Saint-Lazare | 0.5 |
| 14 | Olympiades | 0.3 |

- `frequency`: Proportion of trains using this branch (0-1)

## Utility Scripts

- `find_frequency.py`: Analyze frequencies in an existing GTFS
- `add_fork_script.py`: Specific branch management
- `line_extension.py`: Existing line extensions
- `utils.py`: Utility functions (calculations, conversions, etc.)

## Project Structure

```
implementation_gtfs/
├── add_lines_gtfs.py      # Main script
├── config.json            # Configuration
├── utils.py              # Utility functions
├── line_extension.py     # Line extensions
├── add_fork_script.py   # Branch management
├── find_frequency.py    # Frequency analysis
└── data/                # Input data
    ├── clean_data/      # Cleaned data
    └── IDFM-gtfs/      # Source GTFS
```

## Important Notes

- **Line extensions**: To extend an existing line, the `stations.csv` file should only contain the new stations, but `journey_time.csv` must include the time between the last existing station and the first new station.
- **Journey time** : Must be given in order

## Troubleshooting

- **Error "No stop match found"**: Check that station names match exactly between your files
- **"Too many matches found"**: Part of your extension already exists in the GTFS
- **Coordinate issues**: Make sure your coordinates are in WGS84

## License

This project is part of the GPE (Grand Paris Express) exploration project for transport analysis and simulation.
