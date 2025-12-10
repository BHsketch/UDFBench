SELECT 
id,
extractfromdateMod1(date).year AS year,
extractfromdateMod1(date).month AS month,
extractfromdateMod1(date).day AS day
FROM artifacts;
