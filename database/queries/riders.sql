-- name: GetRider :one
SELECT * FROM riders
WHERE uid = $1;

-- name: UpsertRider :exec
INSERT INTO riders (uid, x, y, online, cell_id, count)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (uid) DO UPDATE SET
    x = EXCLUDED.x,
    y = EXCLUDED.y,
    online = EXCLUDED.online,
    cell_id = EXCLUDED.cell_id,
    count = EXCLUDED.count;

-- name: ReserveRider :one
UPDATE riders
SET count = count + 1
WHERE uid = sqlc.arg(uid)
  AND online = TRUE
  AND count < sqlc.arg(max_count)
RETURNING *;
