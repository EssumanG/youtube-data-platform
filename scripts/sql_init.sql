CREATE TABLE video_metrics (
    description TEXT,
    channel_name TEXT NOT NULL,
    total_views BIGINT CHECK (total_views >= 0),
    total_likes BIGINT CHECK (total_likes >= 0),
    total_dislikes BIGINT CHECK (total_dislikes >= 0),
    date_updated TIMESTAMP NOT NULL,
    -- like_rate NUMERIC(6,4),
    -- dislike_rate NUMERIC(6,4),
    PRIMARY KEY (description, channel_name, date_updated)
);


-- Optional: a "current state" table for quick queries
CREATE MATERIALIZED VIEW video_latest AS
SELECT DISTINCT ON (video_id)
    video_id, channel_id, description, total_views, total_likes,
    total_dislikes, date_created, recorded_at,
    like_rate, dislike_rate, engagement_rate
FROM video_metrics
ORDER BY video_id, recorded_at DESC;

CREATE TABLE channel_stats (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    channel_subscribers BIGINT CHECK (channel_subscribers >= 0),
    total_videos BIGINT CHECK (total_videos >= 0),
    total_views BIGINT CHECK (total_views >= 0),
    total_likes BIGINT CHECK (total_likes >= 0),
    total_dislikes BIGINT CHECK (total_dislikes >= 0)
);

-- top 5 vid
SELECT video_id, SUM(view_diff) AS views_gained
FROM video_trends
WHERE recorded_at >= NOW() - INTERVAL '3 minutes'
GROUP BY video_id
ORDER BY views_gained DESC
LIMIT 5;

top 3 channels
SELECT video_id, SUM(view_diff) AS views_gained
FROM video_trends
WHERE recorded_at >= NOW() - INTERVAL '3 minutes'
GROUP BY video_id
ORDER BY views_gained DESC
LIMIT 5;