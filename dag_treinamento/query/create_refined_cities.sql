CREATE TABLE IF NOT EXISTS `bedu-tech-dev.cities.cities_refined`
(
  Latd int64 OPTIONS(description="Grau Latitude") ,
  latm int64 OPTIONS(description="Minuto Latitude"),
  lats int64 OPTIONS(description="Segundo Latitude"),
  ns string OPTIONS(description="Norte ou Sul"),
  lond int64 OPTIONS(description="Grau Longitude"),
  lonm int64 OPTIONS(description="Minuto Longitude"),
  lons int64 OPTIONS(description="Segundos Longtude"),
  ew string  OPTIONS(description="Leste Oeste"),
  city string OPTIONS(description="Cidade"),
  state string OPTIONS(description="Estado"),
  country string OPTIONS(description="País"),
  continent string OPTIONS(description="Continente"),
  cordinate string OPTIONS(description="Cordenada Completa")
 )