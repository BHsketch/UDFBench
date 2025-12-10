SELECT 
id,
extractFromDateScala(date).year AS year,
extractFromDateScala(date).month AS month,
extractFromDateScala(date).day AS day
FROM artifacts;
