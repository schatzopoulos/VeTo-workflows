# create table
CREATE TABLE paper
(
    id           uuid
        constraint paper_pk
            primary key,
    title        text,
    authors      varchar[],
    venue        varchar[],
    year         int,
    n_citation   int,
    "references" varchar[],
    abstract     text
);

CREATE UNIQUE INDEX paper_id_uindex
    ON paper (id);

CREATE INDEX paper_idx ON paper USING GIN (to_tsvector('english', title || ' ' || abstract));

# load data from json

CREATE TABLE tmp (c text);

\copy tmp from '/data/grigoris-data/paper_data/DBLP-v10/dblp-ref/dblp-ref-0.json'

INSERT INTO paper 
SELECT q.* FROM tmp, json_populate_record(null::notifies, c::json) AS q;