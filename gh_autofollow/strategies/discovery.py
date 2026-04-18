"""
Discovery strategies for finding GitHub users to follow.

Each strategy implements `discover(client, config, db) -> List[Candidate]`.
"""

from __future__ import annotations

import abc
import logging
import random
import time
from typing import Any, Dict, List, Optional

from gh_autofollow.api.client import GitHubClient, GitHubAPIError
from gh_autofollow.config import Config
from gh_autofollow.db.database import Candidate, Database

logger = logging.getLogger(__name__)


def _make_candidate(user: Dict[str, Any], strategy: str, score: float = 0.0) -> Candidate:
    return Candidate(
        login=user.get("login", ""),
        github_id=user.get("id"),
        via_strategy=strategy,
        followers_count=user.get("followers", 0),
        public_repos=user.get("public_repos", 0),
        is_org=user.get("type", "User") == "Organization",
        score=score,
    )


def _score(user: Dict[str, Any]) -> float:
    """
    Simple scoring heuristic based on engagement signals.
    Higher is better.
    """
    followers = user.get("followers", 0) or 0
    repos = user.get("public_repos", 0) or 0
    following = user.get("following", 0) or 0

    # Penalise extremely high follower counts (celebrities, not typical devs)
    if followers > 50_000:
        follower_score = 30.0
    else:
        follower_score = min(followers / 100, 30.0)

    repo_score = min(repos / 5, 20.0)

    # Reward a healthy follower/following ratio (active networker)
    if following > 0:
        ratio_score = min((followers / following), 5.0) * 2
    else:
        ratio_score = 0.0

    return round(follower_score + repo_score + ratio_score, 3)


# ── Abstract base ─────────────────────────────────────────────────────────────

class BaseStrategy(abc.ABC):
    name: str = "base"

    @abc.abstractmethod
    def discover(
        self,
        client: GitHubClient,
        config: Config,
        db: Database,
    ) -> List[Candidate]:
        ...

    def _enrich_logins(
        self,
        client: GitHubClient,
        logins: List[str],
        strategy_name: str,
    ) -> List[Candidate]:
        """Fetch full user profiles and build scored Candidate objects."""
        candidates = []
        for login in logins:
            if not login:
                continue
            try:
                user = client.get_user(login)
                candidates.append(_make_candidate(user, strategy_name, _score(user)))
            except GitHubAPIError as exc:
                logger.debug("Could not enrich %s: %s", login, exc)
        return candidates


# ── Trending strategy ─────────────────────────────────────────────────────────

class TrendingStrategy(BaseStrategy):
    """
    Discovers users by scraping the GitHub trending page.
    Collects repo owners from trending repos.
    """
    name = "trending"

    def discover(self, client: GitHubClient, config: Config, db: Database) -> List[Candidate]:
        logins: List[str] = []
        languages = config.trending_languages or [""]

        for lang in languages:
            repos = client.get_trending_repos(language=lang, since="weekly")
            logins.extend(r.get("login", "") for r in repos)
            time.sleep(0.5)

        # Deduplicate
        logins = list(dict.fromkeys(l for l in logins if l))
        random.shuffle(logins)
        logins = logins[: config.candidate_pool_size]

        logger.info("[trending] Found %d unique owners", len(logins))
        return self._enrich_logins(client, logins, self.name)


# ── Followers-of-following strategy ──────────────────────────────────────────

class FollowersOfFollowingStrategy(BaseStrategy):
    """
    Discovers users who follow the people you already follow
    (second-degree connections).
    """
    name = "followers_of_following"

    def discover(self, client: GitHubClient, config: Config, db: Database) -> List[Candidate]:
        me = client.get_authenticated_user()
        my_login = me["login"]

        # Get who I'm following (up to 2 pages = 200 users)
        my_following = client.get_following(max_pages=2)
        if not my_following:
            return []

        # Sample up to 10 of them to expand
        sample = random.sample(my_following, min(10, len(my_following)))
        logins: List[str] = []
        already_following = {u["login"] for u in my_following}
        already_following.add(my_login)

        for followed_user in sample:
            try:
                followers = client.get_followers(followed_user["login"], max_pages=1)
                new = [
                    u["login"] for u in followers
                    if u["login"] not in already_following
                ]
                logins.extend(new)
                time.sleep(0.3)
            except GitHubAPIError as exc:
                logger.debug("followers_of_following: skip %s: %s", followed_user["login"], exc)

        logins = list(dict.fromkeys(logins))
        random.shuffle(logins)
        logins = logins[: config.candidate_pool_size]

        logger.info("[followers_of_following] Found %d candidates", len(logins))
        return self._enrich_logins(client, logins, self.name)


# ── Starred-repos strategy ────────────────────────────────────────────────────

class StarredReposStrategy(BaseStrategy):
    """
    Discovers users who starred the same repos you have starred
    (shared-interest signals).
    """
    name = "starred_repos"

    def discover(self, client: GitHubClient, config: Config, db: Database) -> List[Candidate]:
        my_starred = client.get_starred_repos(max_pages=1)
        if not my_starred:
            return []

        # Pick up to 5 starred repos and fetch their stargazers
        sample = random.sample(my_starred, min(5, len(my_starred)))
        logins: List[str] = []
        me = client.get_authenticated_user()
        my_login = me["login"]

        for repo in sample:
            owner = repo.get("owner", {}).get("login", "")
            name = repo.get("name", "")
            if not owner or not name:
                continue
            try:
                stargazers = client.get_repo_stargazers(owner, name, max_pages=2)
                for u in stargazers:
                    login = u.get("login", "")
                    if login and login != my_login:
                        logins.append(login)
                time.sleep(0.4)
            except GitHubAPIError as exc:
                logger.debug("starred_repos: skip %s/%s: %s", owner, name, exc)

        logins = list(dict.fromkeys(logins))
        random.shuffle(logins)
        logins = logins[: config.candidate_pool_size]

        logger.info("[starred_repos] Found %d candidates", len(logins))
        return self._enrich_logins(client, logins, self.name)


# ── Topic-search strategy ─────────────────────────────────────────────────────

class TopicSearchStrategy(BaseStrategy):
    """
    Searches for users via GitHub's search API using configured topics.
    """
    name = "topic_search"

    def discover(self, client: GitHubClient, config: Config, db: Database) -> List[Candidate]:
        if not config.topics:
            return []

        topics = random.sample(config.topics, min(3, len(config.topics)))
        logins: List[str] = []

        for topic in topics:
            try:
                users = client.search_users(
                    query=f"topic:{topic} type:user",
                    sort="followers",
                    max_pages=1,
                )
                for u in users:
                    login = u.get("login", "")
                    if login:
                        logins.append(login)
                # Also search repos with that topic and grab owners
                repos = client.get_topic_repositories(topic, max_pages=1)
                for r in repos:
                    owner = r.get("owner", {}).get("login", "")
                    if owner:
                        logins.append(owner)
                time.sleep(1.5)  # search rate limit is 30/min
            except GitHubAPIError as exc:
                logger.warning("topic_search: error for topic %s: %s", topic, exc)

        logins = list(dict.fromkeys(logins))
        random.shuffle(logins)
        logins = logins[: config.candidate_pool_size]

        logger.info("[topic_search] Found %d candidates", len(logins))
        return self._enrich_logins(client, logins, self.name)


# ── Random explore strategy ───────────────────────────────────────────────────

class RandomExploreStrategy(BaseStrategy):
    """
    Discovers random users by listing contributors of popular repos
    and repos with specific languages.
    """
    name = "random_explore"

    # Curated list of popular repos whose contributor list is large and diverse
    _SEED_REPOS = [
        ("torvalds", "linux"),
        ("microsoft", "vscode"),
        ("facebook", "react"),
        ("golang", "go"),
        ("rust-lang", "rust"),
        ("django", "django"),
        ("rails", "rails"),
        ("kubernetes", "kubernetes"),
        ("tensorflow", "tensorflow"),
        ("pytorch", "pytorch"),
    ]

    def discover(self, client: GitHubClient, config: Config, db: Database) -> List[Candidate]:
        sample_repos = random.sample(self._SEED_REPOS, min(4, len(self._SEED_REPOS)))
        logins: List[str] = []

        for owner, repo in sample_repos:
            try:
                contribs = client.get_repo_contributors(owner, repo, max_pages=2)
                for u in contribs:
                    login = u.get("login", "")
                    if login and not login.endswith("[bot]"):
                        logins.append(login)
                time.sleep(0.5)
            except GitHubAPIError as exc:
                logger.debug("random_explore: skip %s/%s: %s", owner, repo, exc)

        logins = list(dict.fromkeys(logins))
        random.shuffle(logins)
        logins = logins[: config.candidate_pool_size]

        logger.info("[random_explore] Found %d candidates", len(logins))
        return self._enrich_logins(client, logins, self.name)


# ── Registry ──────────────────────────────────────────────────────────────────

_STRATEGY_REGISTRY: Dict[str, type[BaseStrategy]] = {
    "trending": TrendingStrategy,
    "followers_of_following": FollowersOfFollowingStrategy,
    "starred_repos": StarredReposStrategy,
    "topic_search": TopicSearchStrategy,
    "random_explore": RandomExploreStrategy,
}


def get_strategy(name: str) -> BaseStrategy:
    cls = _STRATEGY_REGISTRY.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown strategy: {name!r}. Available: {list(_STRATEGY_REGISTRY)}"
        )
    return cls()


def get_all_strategies(names: List[str]) -> List[BaseStrategy]:
    return [get_strategy(n) for n in names]
