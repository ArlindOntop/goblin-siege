# Siege of Bezzgod

A browser tower defense game: 26 defenders, 10 bosses, 5 maps picked at random, 4 game modes (Classic, Endless, Nightmare, Boss Rush), and online co-op for up to 4 players with room codes.

- `index.html`: the whole game
- `server.js`: serves the game and runs online rooms (no dependencies)

Endless mode has an online leaderboard. To keep scores across restarts, set `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN` (free Upstash Redis database) in Render's Environment settings. Without them, scores are kept in a local file that resets when the server restarts.

Hosted on Render as a Node web service (`npm start`). Every push to `main` redeploys automatically.
