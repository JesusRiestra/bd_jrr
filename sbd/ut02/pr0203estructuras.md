```python
import redis

r = redis.Redis(
    host="redis",
    port=6379,
    db=0,
    decode_responses=True
)
```


```python
def add_player(id, name, country, score=0):
    player_key = f"player:{id}"
    player = {
        "name": name,
        "country": country,
        "score": score,
        "games_played": 0
    }
    r.hset(player_key, mapping = player)
    r.zadd("leaderboard", {id:score})
```


```python
def update_score(id, points):
    player_key = f"player:{id}"

    games_played = r.hget(player_key, "games_played")
    games_played = int(games_played) + 1
    r.hset(player_key, "games_played", games_played)

    score = r.hget(player_key, "score")
    new_score = int(score) + points
    r.hset(player_key, "score", new_score)

    r.zadd("leaderboard", {id:new_score})
```


```python
def player_info(id):
    player_key = f"player:{id}"

    info = r.hgetall(player_key)

    for key, value in info.items():
        print(f"- {key.capitalize()}: {value}")
    return info
```


```python
def show_top_players(n):
    print(f"~~~ TOP {n} JUGADORES ~~~")
    top_players = r.zrevrange("leaderboard", 0, n-1, withscores=True)

    for i, (id, score) in enumerate(top_players):
        player_key = f"player:{id}"

        res = r.hgetall(player_key)

        print(f" #{i+1}     {res['name']} - {res['country']}")
        print(f"     ID: {id} | Puntuación: {res['score']}")
        print("- - - - - - - - - - - - - - - -")
```


```python
def get_date_key(date_obj):
    return f"unique:players:{date_obj.isoformat()}"
```


```python
def register_login(player_id):
    today_key = get_date_key(date.today())

    changed = r.pfadd(today_key, player_id)
```


```python
def count_unique_logins(date):
    key = f"unique:players:{date}"

    count = r.pfcount(key)

    print(f"Jugadores únicos aproximados ({date}): {count}")
```


```python
def weekly_report(dates):
    keys_to_merge = [f"unique:players:{d}" for d in dates_list]

    report_key = "unique:players:week_report"

    r.pfmerge(report_key, *keys_to_merge)

    total_unique = r.pfcount(report_key)

    print(f"Reporte semanal (Días): {len(dates)})")
    print(f"Total de jugadores únicos apróximado: {total_unique}")

    r.delete(report_key)

    return total_unique
```
