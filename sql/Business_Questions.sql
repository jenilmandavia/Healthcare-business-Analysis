--TOP 5 STATES WHERE OLDER ADULTS NEED ASSISTATNCE IN DAY TO DAY ACTIVITIES DUE TO COGNITIVE DECLINE
SELECT 
	c.ClassificationName AS Classification,
    l.locationdesc AS State,
    ROUND (AVG(hsf.Data_Value)::numeric,2) AS Avg_Percentage
FROM 
    HealthSurveyFact hsf
JOIN 
    Locations l ON hsf.LocationID = l.LocationID
JOIN 
    Topics t ON hsf.TopicID = t.TopicID
JOIN 
    Classifications c ON t.ClassID = c.ClassID
WHERE 
    t.TopicID = 'TCC03' 
    AND hsf.YearEnd = 2021  
    AND hsf.Data_Value <> 0
	AND l.locationid <= 56 --Only states
GROUP BY 
    1,2
ORDER BY 
    Avg_Percentage DESC
LIMIT 5;

--Relationship between healthcare expenditure and workforce size across different states
SELECT 
    w.locationid,
	l.locationdesc AS State,
    (w.phys_wkforc_21+w.rn_21) AS WorkforceSize, 
    e.val AS Expenditure
FROM 
    healthcare_workforce w
JOIN 
    healthcare_expenditure e ON w.locationid = e.locationid
JOIN 
    Locations l ON w.LocationID = l.LocationID
WHERE 
    e.metric = 'Standardized spending per capita'
	AND e.group_name = 'Total'
	And e.year = 2019
GROUP BY 
    1,2,3,4
ORDER BY 
    4 DESC;

--Year Wise Average Spending Per Capita by Payer Across States
SELECT
    he.year,
	l.locationid,
    he.state, 
    he.subgroup AS PayerType,
    AVG(he.val) AS AvgSpendingPerCapita
FROM 
    healthcare_expenditure he
JOIN 
    Locations l ON he.LocationID = l.LocationID
WHERE 
    he.metric = 'Standardized spending per capita'
    AND he.group_name <> 'Total'
GROUP BY 
    1,2,3,4
ORDER BY 
    1 DESC;
