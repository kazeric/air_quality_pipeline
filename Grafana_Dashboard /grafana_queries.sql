---note 
--- whenever doing an agregation function like AVG, use the AS keyword to rename the components in the graghs for better readability


--- The source of ground level ozone
--- A higher uv index during the day leads to higher grond level ozone created from the reaction with nitogen diovide, leading to its deplition

SELECT 
    timestamp AS time,
    nitrogen_dioxide,
    ozone/10 AS ozone,
    uv_index

FROM default_keyspace.air_quality_data;


---Dusty commutes
---During the morning times these is a significant spike in particles kicked up but commuters in the city and this slowly goes down over the course of the day

SELECT 
    timestamp,
    AVG(pm2_5) AS avg_pm2_5,
    AVG(pm10) AS avg_pm10,
    city
FROM default_keyspace.air_quality_data
GROUP BY city, timestamp;

---Strongly correlated pollutants
---SELECT AVG(carbon_monoxide), AVG(nitrogen_dioxide), AVG(sulphur_dioxide), timestamp FROM default_keyspace.air_quality_data GROUP BY city, timestamp;

SELECT 
    AVG(carbon_monoxide) AS carbon_monoxide, 
    AVG(nitrogen_dioxide) AS nitrogen_dioxide, 
    AVG(sulphur_dioxide) AS sulphur_dioxide, 
    timestamp 
FROM default_keyspace.air_quality_data 
GROUP BY city, timestamp;


---Major air polutants in Mombasa
SELECT 
  AVG(carbon_monoxide) AS carbon_monoxide, 
  city, 
  AVG(nitrogen_dioxide) AS nitrogen_dioxide, 
  AVG(sulphur_dioxide) AS sulphur_dioxide, 
  AVG(pm2_5) AS pm2_5, 
  AVG(pm10) AS pm10
FROM default_keyspace.air_quality_data 
WHERE city = 'mombasa' 
GROUP BY timestamp;

--- Comparison between Mombasa and Nairobi
SELECT 
  AVG(carbon_monoxide) AS carbon_monoxide, 
  city 
FROM default_keyspace.air_quality_data 
GROUP BY city; 