-- This SQL file generates our wikipedia schema 
create database wikipedia;
USE wikipedia

-- This table stores topics that we have seen.
-- Topics are stored as the string after /wiki/ in a wikipedia
-- URL, stripped of things like anchors and URL params
CREATE TABLE topics(
	topic VARCHAR(255) PRIMARY KEY
);

-- This table stores topics that we hae already visited.
-- Format is identical to the topics table
CREATE TABLE finished(
	topic VARCHAR(255) PRIMARY KEY
);

-- This table stores links as source /destination pairs.
-- Links are stored using canonical page names, which is NOT
-- the same format as for 'topics' and 'finished', as those tables,
-- use URL names, which often result in redirects.
-- No index is present as writes are too slow on a table this large.
-- Duplicates are removed outside of SQL using a Java script.
CREATE TABLE links(
	source VARCHAR(255),
	dest VARCHAR(255)
);

-- This table maps from the name of a wikipedia link (the part after /wiki/)
-- To the name of an actual article. This table is used because Wikipedia
-- allows for many links to point to the same article, and this makes
-- sure that we can reduce from the set of all links to the set of all
-- articles.
CREATE TABLE canon (
	link_name VARCHAR(255),
	canon_name VARCHAR(255)
);


-- The system needs some starting articles
INSERT INTO topics VALUES ("Philosophy");
INSERT INTO topics VALUES ("Biology");
INSERT INTO topics VALUES ("Physics");