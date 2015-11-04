-- This SQL file generates our wikipedia schema 
create database wiki;
USE wiki

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

-- The system needs some starting article-- philosophy is a good place to start.
INSERT INTO topics VALUES ("Philosophy");