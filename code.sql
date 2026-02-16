create table arxiv_raw as
select * from read_json_auto('C:/Users/bmkol/OneDrive/Документи/KSE Studying/Year 3 - Term 8/Data Engineering/DataEngineering/Ass1/arxiv-metadata-oai-snapshot.json');



select count(*) as bad_rows_count
from arxiv_raw
where id is null or title is null;



select (count(*) - count(distinct id)) as duplicate_count
from arxiv_raw;



create table parsed as
select id, split_part(categories, ' ', 1) as cat, authors_parsed, unnest(versions) as v_info
from arxiv_raw;



select count(*) as no_authors_count
from parsed
where len(authors_parsed) = 0;



select cat, count(*) as total_activity, rank() over (order by count(*) desc) as rank_place
from parsed
group by cat
limit 10;



with author_stats as (
    select year(strptime(v_info['created'], '%a, %d %b %Y %H:%M:%S %Z')) as pub_year,
    json_array_length(authors_parsed) as author_count
    from parsed
)
select pub_year, round(avg(author_count), 2) as avg_authors,
    round(avg(avg(author_count)) OVER (order by pub_year rows between 1 preceding and 1 following), 2) as moving_avg_3_years
from author_stats
where pub_year between 1991 and 2024
group by pub_year
order by pub_year;




with yearly_counts as (
    select year(strptime(v_info['created'], '%a, %d %b %Y %H:%M:%S %Z')) as pub_year,
    cat, count(*) as total_papers
from parsed
group by pub_year, cat
),
growth_stats as (
    select pub_year, cat, total_papers,
    lag(total_papers) over (partition by cat order by pub_year) as prev_year_papers
from yearly_counts
where pub_year >= 2015
)
select pub_year, cat, total_papers, round((total_papers - prev_year_papers) * 100.0 / prev_year_papers, 2) as growth_percent
from growth_stats
where prev_year_papers > 500
order by growth_percent desc
limit 10;