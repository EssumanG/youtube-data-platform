
CREATE TABLE video_metrics (
    description TEXT,
    channel_name TEXT NOT NULL,
    total_views NUMERIC CHECK (total_views >= 0),
    total_likes NUMERIC CHECK (total_likes >= 0),
    total_dislikes NUMERIC CHECK (total_dislikes >= 0),
    date_updated TIMESTAMP NOT NULL,
    -- like_rate NUMERIC(6,4),
    -- dislike_rate NUMERIC(6,4),
    PRIMARY KEY (description, channel_name, date_updated)
);


CREATE TABLE channel_stats (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    channel_subscribers NUMERIC CHECK (channel_subscribers >= 0),
    total_videos NUMERIC CHECK (total_videos >= 0),
    total_views NUMERIC CHECK (total_views >= 0),
    total_likes NUMERIC CHECK (total_likes >= 0),
    total_dislikes NUMERIC CHECK (total_dislikes >= 0)
);


CREATE MATERIALIZED VIEW video_latest AS
SELECT DISTINCT ON (description)
    description, total_views, total_likes,
    total_dislikes, date_updated
FROM video_metrics
ORDER BY description, date_updated DESC;
