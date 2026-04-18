"""
GitHub REST API v3 client for gh-autofollow.

Features:
  - Persistent session with retry / exponential backoff
  - Proactive rate-limit tracking (reads X-RateLimit-* headers on every response)
  - Transparent pagination via Link header
  - Context manager support
  - ETag / Last-Modified conditional requests
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import urlencode, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ── Rate-limit state ──────────────────────────────────────────────────────────

@dataclass
class RateLimitState:
    core_limit: int = 5000
    core_remaining: int = 5000
    core_reset: int = 0
    search_limit: int = 30
    search_remaining: int = 30
    search_reset: int = 0

    def seconds_until_core_reset(self) -> float:
        return max(0.0, self.core_reset - time.time())

    def seconds_until_search_reset(self) -> float:
        return max(0.0, self.search_reset - time.time())

    def is_core_exhausted(self, buffer: int = 0) -> bool:
        return self.core_remaining <= buffer

    def is_search_exhausted(self, buffer: int = 0) -> bool:
        return self.search_remaining <= buffer


class RateLimitExceeded(Exception):
    """Raised when we cannot proceed without violating rate limit."""
    def __init__(self, reset_at: float, resource: str = "core") -> None:
        self.reset_at = reset_at
        self.resource = resource
        super().__init__(
            f"GitHub {resource} rate limit hit. Resets in "
            f"{max(0, reset_at - time.time()):.0f}s"
        )


class GitHubAPIError(Exception):
    """General GitHub API error (non-rate-limit)."""
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"GitHub API {status_code}: {message}")


# ── Client ────────────────────────────────────────────────────────────────────

class GitHubClient:
    """
    Authenticated GitHub REST API v3 client.
    """

    _ACCEPT_V3 = "application/vnd.github.v3+json"
    _ACCEPT_STAR = "application/vnd.github.star+json"

    def __init__(
        self,
        token: str,
        base_url: str = "https://api.github.com",
        timeout: int = 30,
        max_retries: int = 5,
        retry_backoff: float = 2.0,
        rate_limit_buffer: int = 100,
    ) -> None:
        if not token:
            raise ValueError("GitHub token is required")

        self._token = token
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._rate_limit_buffer = rate_limit_buffer
        self.rate_limit = RateLimitState()

        self._session = self._build_session()

    # ── Session factory ───────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._token}",
                "Accept": self._ACCEPT_V3,
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "gh-autofollow/1.0.0 (+https://github.com/gh-autofollow)",
            }
        )
        retry = Retry(
            total=self._max_retries,
            backoff_factor=self._retry_backoff,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "PUT", "DELETE"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "GitHubClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ── Low-level request ─────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None,
        json: Optional[Dict] = None,
        extra_headers: Optional[Dict] = None,
        _retry_after_sleep: bool = True,
    ) -> requests.Response:
        url = f"{self._base_url}/{path.lstrip('/')}"
        headers = dict(extra_headers or {})

        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self._session.request(
                    method,
                    url,
                    params=params,
                    json=json,
                    headers=headers,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                if attempt > self._max_retries:
                    raise
                sleep = self._retry_backoff ** attempt
                logger.warning("Network error (%s), retrying in %.1fs", exc, sleep)
                time.sleep(sleep)
                continue

            self._update_rate_limit(resp)

            if resp.status_code == 200 or resp.status_code == 204:
                return resp

            if resp.status_code == 202:
                # Accepted — retry after a short wait
                time.sleep(1)
                continue

            if resp.status_code == 304:
                return resp  # Not Modified

            if resp.status_code == 401:
                raise GitHubAPIError(401, "Bad credentials — check your GitHub token")

            if resp.status_code == 403:
                # Could be secondary rate limit or permission issue
                retry_after = int(resp.headers.get("Retry-After", 0))
                if retry_after and _retry_after_sleep:
                    logger.warning("Secondary rate limit; sleeping %ds", retry_after)
                    time.sleep(retry_after + 1)
                    continue
                msg = self._extract_message(resp)
                if "rate limit" in msg.lower() or "abuse" in msg.lower():
                    reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                    raise RateLimitExceeded(float(reset))
                raise GitHubAPIError(403, msg)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                if _retry_after_sleep and attempt <= self._max_retries:
                    logger.warning("429 Too Many Requests; sleeping %ds", retry_after)
                    time.sleep(retry_after + 1)
                    continue
                raise RateLimitExceeded(time.time() + retry_after)

            if resp.status_code == 404:
                raise GitHubAPIError(404, f"Not found: {url}")

            if resp.status_code == 422:
                raise GitHubAPIError(422, self._extract_message(resp))

            if resp.status_code >= 500:
                if attempt <= self._max_retries:
                    sleep = self._retry_backoff ** attempt
                    logger.warning("Server error %d; retrying in %.1fs", resp.status_code, sleep)
                    time.sleep(sleep)
                    continue
                raise GitHubAPIError(resp.status_code, "GitHub server error")

            raise GitHubAPIError(resp.status_code, self._extract_message(resp))

    @staticmethod
    def _extract_message(resp: requests.Response) -> str:
        try:
            return resp.json().get("message", resp.text[:200])
        except Exception:
            return resp.text[:200]

    def _update_rate_limit(self, resp: requests.Response) -> None:
        h = resp.headers
        try:
            self.rate_limit.core_remaining = int(h.get("X-RateLimit-Remaining", self.rate_limit.core_remaining))
            self.rate_limit.core_limit = int(h.get("X-RateLimit-Limit", self.rate_limit.core_limit))
            self.rate_limit.core_reset = int(h.get("X-RateLimit-Reset", self.rate_limit.core_reset))
        except (ValueError, TypeError):
            pass

    # ── Pagination ────────────────────────────────────────────────────────────

    def _paginate(
        self,
        path: str,
        params: Optional[Dict] = None,
        max_pages: int = 10,
    ) -> Generator[List[Dict], None, None]:
        """Yield pages (lists of dicts) until there is no next page."""
        url: Optional[str] = f"{self._base_url}/{path.lstrip('/')}"
        page = 0
        _params = dict(params or {})
        _params.setdefault("per_page", 100)

        while url and page < max_pages:
            resp = self._session.request("GET", url, params=_params if page == 0 else None, timeout=self._timeout)
            self._update_rate_limit(resp)
            resp.raise_for_status()
            yield resp.json()
            page += 1
            link = resp.headers.get("Link", "")
            url = self._parse_next_link(link)

    @staticmethod
    def _parse_next_link(link_header: str) -> Optional[str]:
        """Parse RFC 5988 Link header for rel="next"."""
        for part in link_header.split(","):
            segments = [s.strip() for s in part.split(";")]
            if len(segments) < 2:
                continue
            url = segments[0].strip("<>")
            rel = segments[1]
            if 'rel="next"' in rel:
                return url
        return None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def get_authenticated_user(self) -> Dict[str, Any]:
        return self._request("GET", "/user").json()

    def get_rate_limits(self) -> Dict[str, Any]:
        data = self._request("GET", "/rate_limit").json()
        resources = data.get("resources", {})
        core = resources.get("core", {})
        search = resources.get("search", {})
        self.rate_limit.core_limit = core.get("limit", 5000)
        self.rate_limit.core_remaining = core.get("remaining", 0)
        self.rate_limit.core_reset = core.get("reset", 0)
        self.rate_limit.search_limit = search.get("limit", 30)
        self.rate_limit.search_remaining = search.get("remaining", 0)
        self.rate_limit.search_reset = search.get("reset", 0)
        return data

    # ── Follow ────────────────────────────────────────────────────────────────

    def follow_user(self, login: str) -> bool:
        """
        PUT /user/following/{login}
        Returns True on success (204), False if already following.
        Raises on error.
        """
        if self.rate_limit.is_core_exhausted(self._rate_limit_buffer):
            raise RateLimitExceeded(float(self.rate_limit.core_reset))

        resp = self._request("PUT", f"/user/following/{login}")
        return resp.status_code == 204

    def is_following(self, login: str) -> bool:
        """
        GET /user/following/{login} → 204 if following, 404 if not.
        """
        try:
            resp = self._request("GET", f"/user/following/{login}")
            return resp.status_code == 204
        except GitHubAPIError as exc:
            if exc.status_code == 404:
                return False
            raise

    def get_following(self, username: str = "", max_pages: int = 5) -> List[Dict]:
        path = f"/users/{username}/following" if username else "/user/following"
        result = []
        for page in self._paginate(path, max_pages=max_pages):
            result.extend(page)
        return result

    def get_followers(self, username: str, max_pages: int = 3) -> List[Dict]:
        result = []
        for page in self._paginate(f"/users/{username}/followers", max_pages=max_pages):
            result.extend(page)
        return result

    # ── User info ─────────────────────────────────────────────────────────────

    def get_user(self, login: str) -> Dict[str, Any]:
        return self._request("GET", f"/users/{login}").json()

    def get_user_bulk(self, logins: List[str]) -> List[Dict[str, Any]]:
        """Fetch user profiles for a list of logins (sequential, respects rate limit)."""
        results = []
        for login in logins:
            try:
                results.append(self.get_user(login))
            except GitHubAPIError as exc:
                logger.debug("Could not fetch user %s: %s", login, exc)
        return results

    # ── Repos / starred ───────────────────────────────────────────────────────

    def get_starred_repos(self, username: str = "", max_pages: int = 2) -> List[Dict]:
        path = f"/users/{username}/starred" if username else "/user/starred"
        result = []
        for page in self._paginate(path, max_pages=max_pages):
            result.extend(page)
        return result

    def get_repo_stargazers(self, owner: str, repo: str, max_pages: int = 3) -> List[Dict]:
        result = []
        for page in self._paginate(f"/repos/{owner}/{repo}/stargazers", max_pages=max_pages):
            result.extend(page)
        return result

    def get_repo_contributors(self, owner: str, repo: str, max_pages: int = 2) -> List[Dict]:
        result = []
        for page in self._paginate(f"/repos/{owner}/{repo}/contributors", max_pages=max_pages):
            result.extend(page)
        return result

    # ── Search ────────────────────────────────────────────────────────────────

    def search_users(self, query: str, sort: str = "followers", max_pages: int = 2) -> List[Dict]:
        """
        Search for users.  Respects search rate limit (30/min).
        Returns list of user search result items.
        """
        if self.rate_limit.is_search_exhausted(2):
            wait = self.rate_limit.seconds_until_search_reset()
            logger.warning("Search rate limit near; sleeping %.0fs", wait + 1)
            time.sleep(wait + 1)

        result = []
        params = {"q": query, "sort": sort, "order": "desc"}
        for page in self._paginate("/search/users", params=params, max_pages=max_pages):
            if isinstance(page, dict):
                result.extend(page.get("items", []))
            elif isinstance(page, list):
                result.extend(page)
        return result

    def search_repositories(self, query: str, max_pages: int = 2) -> List[Dict]:
        result = []
        params = {"q": query, "sort": "stars", "order": "desc"}
        for page in self._paginate("/search/repositories", params=params, max_pages=max_pages):
            if isinstance(page, dict):
                result.extend(page.get("items", []))
            elif isinstance(page, list):
                result.extend(page)
        return result

    # ── Topics ────────────────────────────────────────────────────────────────

    def get_topic_repositories(self, topic: str, max_pages: int = 2) -> List[Dict]:
        query = f"topic:{topic}"
        return self.search_repositories(query, max_pages=max_pages)

    # ── Trending (scrape GH trending page) ───────────────────────────────────

    def get_trending_repos(self, language: str = "", since: str = "daily") -> List[Dict]:
        """
        Scrape https://github.com/trending for repository owners.
        Returns simplified dicts with 'login' keys.
        Falls back to starred_repos on parse failure.
        """
        import re
        url = "https://github.com/trending"
        if language:
            url += f"/{language.lower().replace(' ', '-')}"
        url += f"?since={since}"

        try:
            resp = self._session.get(
                url,
                headers={"Accept": "text/html"},
                timeout=self._timeout,
            )
            if resp.status_code != 200:
                return []

            # Extract repo URLs like /owner/repo
            pattern = re.compile(r'href="/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)"')
            matches = pattern.findall(resp.text)
            seen = set()
            results = []
            for owner, _repo in matches:
                if owner in seen or owner.lower() in (
                    "trending", "explore", "topics", "collections",
                    "login", "signup", "features", "github",
                ):
                    continue
                seen.add(owner)
                results.append({"login": owner, "type": "User"})
            return results

        except Exception as exc:
            logger.warning("Failed to scrape trending page: %s", exc)
            return []
