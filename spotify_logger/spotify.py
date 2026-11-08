from urllib.parse import urlencode, quote, urlparse
from .http import ApiError, SpotifyInvalidGrant, json_response


class Spotify:
    def __init__(self, settings, http):
        self.settings, self.http = settings, http

    def refresh(self, refresh_token):
        response = self.http.request("POST", "https://accounts.spotify.com/api/token", retry=True,
                                     auth=(self.settings.spotify_id, self.settings.spotify_secret),
                                     data={"grant_type": "refresh_token", "refresh_token": refresh_token})
        if response.status_code == 400:
            try:
                if response.json().get("error") == "invalid_grant":
                    raise SpotifyInvalidGrant("Spotify reauthorization required")
            except ValueError:
                pass
        data = json_response(response, "Spotify token")
        if not data.get("access_token"):
            raise ApiError("Spotify access token is missing")
        return data

    def _get(self, url, access_token):
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != "api.spotify.com":
            raise ApiError("Unexpected Spotify pagination URL")
        return json_response(self.http.request("GET", url, retry=True,
                                             headers={"authorization": f"Bearer {access_token}"}), "Spotify")

    def recent(self, access_token, after):
        url = "https://api.spotify.com/v1/me/player/recently-played?" + urlencode({"limit": 50, "after": max(0, after)})
        items = []
        for _ in range(5):
            data = self._get(url, access_token)
            items.extend(data.get("items", []))
            url = data.get("next")
            if not url:
                break
        return items

    def artist(self, access_token, artist_id):
        return self._get("https://api.spotify.com/v1/artists/" + quote(artist_id, safe=""), access_token)
