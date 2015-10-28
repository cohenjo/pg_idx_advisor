CREATE TABLE webapp (doc JSONB);

INSERT INTO webapp VALUES('{"name" : "Bill", "active" : true}'), ('{"name" : "Jack", "active" : true}');
insert into webapp select row_to_json(foo) from (select 'jack_'||i as name, (i%5 = 0) as active from generate_series(1,2000) i)foo;


SELECT ctid, * FROM webapp ORDER BY 1;
CREATE INDEX i_webapp_yc ON webapp USING gin (doc /* jsonb_ops */);
CREATE INDEX i_webapp_doc_path ON webapp USING gin (doc jsonb_path_ops);

explain select doc->'active' from webapp where doc @> '{"name" : "Bill"}'::jsonb;

