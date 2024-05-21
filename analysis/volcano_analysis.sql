-- select count(*) as volcano_count,country  
-- from volcano_schema.volcano_regional_data 
-- group by country
-- order by volcano_count desc;


with 
sea_level_group as (
    select 
        case when elevation_ft < 0 then 'under_sea_level' 
            when elevation_ft >= 0 then 'above_sea_level'
            else 'extra'
        end as sea_level_elevation,
        country
    from volcano_schema.volcano_regional_data
),
sea_level_count as (
    select 
        count(*) as number_of_each_type,
        sea_level_elevation,
        country
    from sea_level_group
    group by sea_level_elevation,country
    order by number_of_each_type desc
),
country_select as (
    select
        country
    from sea_level_count
    where number_of_each_type >= 10
)
select 
    number_of_each_type,
    sea_level_elevation,
    country
from sea_level_count
where country in (select country from country_select);

select * FROM example_table