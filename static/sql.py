MOVIES_SQL = """
        SELECT
           fw.id,
           fw.title,
           fw.description,
           fw.rating,
           fw.type,
           fw.created_at,
           greatest(fw.updated_at, max(p.updated_at), max(g.updated_at)) as updated_at,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'person_role', pfw.role,
                       'person_id', p.id,
                       'person_name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
           ) as persons,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'genre_id', g.id,
                       'genre_name', g.name
                   )
               ) FILTER (WHERE g.id is not null),
               '[]'
           ) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.updated_at > %s or p.updated_at > %s or g.updated_at > %s
        GROUP BY fw.id
        ORDER BY fw.updated_at;
"""

GENRES_SQL = """
        select * from content.genre
        where updated_at > %s
        ORDER BY updated_at;
"""
PERSONS_SQL = """
    select 
	p.id, 
	p.full_name, 
	p.updated_at,
	ARRAY_AGG(DISTINCT pfw.role) as roles,
	COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'role', pfw.role,
					   'film_id', pfw.film_work_id
                   )
               ) FILTER (WHERE pfw.film_work_id is not null),
               '[]'
           ) as films
    from content.person p
    join content.person_film_work pfw on p.id = pfw.person_id
    where p.updated_at > %s
    group by p.id
    ORDER BY p.updated_at;
"""