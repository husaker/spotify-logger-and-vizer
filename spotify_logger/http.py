"""Bounded HTTP. No automatic retries for Google mutations or ambiguous writes."""
import time
import requests


class ApiError(RuntimeError):
    pass


class SpotifyInvalidGrant(ApiError):
    pass


class HttpClient:
    def __init__(self, seconds=120, session=None, clock=time.monotonic, sleep=time.sleep):
        self.session = session or requests.Session()
        self.clock, self.sleep = clock, sleep
        self.deadline = clock() + seconds

    def request(self, method, url, *, retry=False, **kwargs):
        for attempt in range(4 if retry else 1):
            remaining = self.deadline - self.clock()
            if remaining <= 0:
                raise ApiError("Request deadline exceeded")
            try:
                response = self.session.request(method, url, timeout=(min(10, remaining / 2), min(20, remaining / 2)),
                                                allow_redirects=False, **kwargs)
            except requests.RequestException:
                raise ApiError("Network request failed") from None
            if self.clock() >= self.deadline:
                raise ApiError("Request deadline exceeded")
            if retry and (response.status_code == 429 or response.status_code >= 500) and attempt < 3:
                try:
                    delay = max(0, float(response.headers["Retry-After"]))
                except (KeyError, ValueError):
                    delay = min(8, .5 * 2 ** attempt)
                if delay >= self.deadline - self.clock():
                    raise ApiError("Retry exceeds request deadline")
                self.sleep(delay)
                continue
            return response
        raise ApiError("Request failed")


def json_response(response, service):
    if not 200 <= response.status_code < 300:
        raise ApiError(f"{service} request failed (HTTP {response.status_code})")
    try:
        result = response.json()
        if not isinstance(result, dict):
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ApiError(f"Invalid {service} response") from None
