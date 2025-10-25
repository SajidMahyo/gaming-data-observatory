"""Test IGDB API to see available data."""

import json

import requests

CLIENT_ID = "kijjpxueo9fzhrv9elm09ehwdpl3w6"
ACCESS_TOKEN = "n34iegxz5die6hha1aieq3ivfa159f"

headers = {
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "text/plain",
}

# Test 1: Get all fields for Dota 2
print("=" * 80)
print("TEST 1: All fields for Dota 2 (game ID: 2963)")
print("=" * 80)

query = """
where id = 2963;
fields name,summary,genres.name,platforms.name,first_release_date,
       rating,aggregated_rating,websites.url,cover.url,category,
       status,themes.name,game_modes.name,player_perspectives.name,
       involved_companies.company.name,involved_companies.developer,
       involved_companies.publisher;
"""

response = requests.post("https://api.igdb.com/v4/games", headers=headers, data=query)

print(json.dumps(response.json(), indent=2))

# Test 2: Get external game IDs
print("\n" + "=" * 80)
print("TEST 2: External IDs for Dota 2")
print("=" * 80)

query2 = "where game = 2963; fields game,category,uid,name;"

response2 = requests.post("https://api.igdb.com/v4/external_games", headers=headers, data=query2)

print(json.dumps(response2.json(), indent=2))

# Test 3: Check what category codes mean
print("\n" + "=" * 80)
print("TEST 3: Category codes (from IGDB docs)")
print("=" * 80)
print(
    """
Category mapping:
1  = steam
5  = gog
10 = youtube
11 = microsoft
13 = steam (alt)
14 = twitch
15 = android
20 = discord
26 = epic
28 = oculus
29 = utomik
30 = itch.io
31 = xbox marketplace
32 = kartridge
33 = battlenet
34 = origin
35 = uplay
36 = epicgames
37 = humble
"""
)

# Test 4: Get popular games to see what we can discover
print("\n" + "=" * 80)
print("TEST 4: Top 10 popular games with all platform IDs")
print("=" * 80)

query4 = """
fields name,rating,aggregated_rating,total_rating_count;
where total_rating_count > 100;
sort total_rating_count desc;
limit 10;
"""

response4 = requests.post("https://api.igdb.com/v4/games", headers=headers, data=query4)

print(json.dumps(response4.json(), indent=2))
