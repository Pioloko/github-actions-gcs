SELECT
  LatD,
  LatM,
  LatS,
  CASE WHEN NS = 'S' THEN 'south' ELSE 'north' END as ns,
  LonD,
  LonM,
  LonS,
  CASE WHEN EW = 'E' THEN 'east' ELSE 'west' END as ew,
  City,
  State,
  CASE WHEN country IN ('Us', 'USA', 'United States') THEN 'United States' else country END as country,
  Continent,
  CONCAT(LatD, 'º', LatM, "'", LatS, '"', ns, ' ', LonD, 'º', LonM, "'", LonS, '"', ew) as cordinate
FROM 
  `bedu-tech-dev.cities.cities_raw`;