-- select count(*) as volcano_count,country  
-- from volcano_schema.volcano_regional_data 
-- group by country
-- order by volcano_count desc;


select 
    *
from volcano_schema.volcano_regional_data 
where country = 'United States'
order by elevation_ft desc;

