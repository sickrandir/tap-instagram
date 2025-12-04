"""Stream type classes for tap-instagram."""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.exceptions import FatalAPIError

from tap_instagram.client import InstagramStream

# SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class UsersStream(InstagramStream):
    """Define custom stream."""

    name = "users"
    path = "/{user_id}"
    primary_keys = ["id"]
    replication_key = None
    fields = [
        "id",
        "ig_id",
        "name",
        "username",
        "biography",
        "followers_count",
        "media_count",
    ]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ig_id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("username", th.StringType),
        th.Property("biography", th.StringType),
        th.Property("followers_count", th.IntegerType),
        th.Property("media_count", th.IntegerType),
    ).to_dict()

    @property
    def partitions(self) -> Optional[List[dict]]:
        return [{"user_id": user_id} for user_id in self.config["ig_user_ids"]]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.fields)
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"user_id": record["id"]}


class MediaStream(InstagramStream):
    """Define custom stream."""

    name = "media"
    # user_id is populated using child context keys from UsersStream
    path = "/{user_id}/media"
    parent_stream_type = UsersStream
    primary_keys = ["id"]
    replication_key = "timestamp"
    records_jsonpath = "$.data[*]"
    fields = [
        "id",
        "ig_id",
        "caption",
        "comments_count",
        "is_comment_enabled",
        "like_count",
        "media_product_type",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Media ID.",
        ),
        th.Property(
            "ig_id",
            th.StringType,
            description="Instagram media ID.",
        ),
        th.Property(
            "caption",
            th.StringType,
            description=(
                "Caption. Excludes album children. @ symbol excluded unless the app user can perform "
                "admin-equivalent tasks on the Facebook Page connected to the Instagram account used to "
                "create the caption."
            ),
        ),
        th.Property(
            "comments_count",
            th.IntegerType,
            description=(
                "Count of comments on the media. Excludes comments on album child media and the media's "
                "caption. Includes replies on comments."
            ),
        ),
        th.Property(
            "is_comment_enabled",
            th.BooleanType,
            description="Indicates if comments are enabled or disabled. Excludes album children.",
        ),
        th.Property(
            "like_count",
            th.IntegerType,
            description=(
                "Count of likes on the media. Excludes likes on album child media and likes on promoted posts "
                "created from the media. Includes replies on comments."
            ),
        ),
        th.Property(
            "media_product_type",
            th.StringType,
            description="Surface where the media is published. Can be AD, FEED, IGTV, or STORY.",
        ),
        th.Property(
            "media_type",
            th.StringType,
            description="Media type. Can be CAROUSEL_ALBUM, IMAGE, or VIDEO.",
        ),
        th.Property(
            "media_url",
            th.StringType,
            description=(
                "Media URL. Will be omitted from responses if the media contains copyrighted material, "
                "or has been flagged for a copyright violation."
            ),
        ),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="ID of Instagram user who created the media.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="Username of Instagram user who created the media.",
                ),
            ),
            description=(
                "ID of Instagram user who created the media. Only returned if the app user making the query "
                "also created the media, otherwise username field will be returned instead."
            ),
        ),
        th.Property(
            "permalink",
            th.StringType,
            description="Permanent URL to the media.",
        ),
        th.Property(
            "shortcode",
            th.StringType,
            description="Shortcode to the media.",
        ),
        th.Property(
            "thumbnail_url",
            th.StringType,
            description="Media thumbnail URL. Only available on VIDEO media.",
        ),
        th.Property(
            "timestamp",
            th.DateTimeType,
            description="ISO 8601 formatted creation date in UTC (default is UTC ±00:00)",
        ),
        th.Property(
            "username",
            th.StringType,
            description="Username of user who created the media.",
        ),
    ).to_dict()

    def make_since_param(self, context: Optional[dict]) -> datetime:
        state_ts = self.get_starting_timestamp(context)
        if state_ts:
            return pendulum.instance(state_ts).subtract(
                days=self.config["media_insights_lookback_days"]
            )
        return state_ts

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.fields)
        params["since"] = self.make_since_param(context)
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "media_id": record["id"],
            "media_type": record["media_type"],
            # media_product_type not present for carousel children media
            "media_product_type": record.get("media_product_type"),
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class StoriesStream(InstagramStream):
    """Define custom stream."""

    name = "stories"
    # user_id is populated using child context keys from UsersStream
    path = "/{user_id}/stories"
    parent_stream_type = UsersStream
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]"
    fields = [
        "id",
        "ig_id",
        "caption",
        "comments_count",
        "like_count",
        "media_product_type",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Media ID.",
        ),
        th.Property(
            "ig_id",
            th.StringType,
            description="Instagram media ID.",
        ),
        th.Property(
            "caption",
            th.StringType,
            description=(
                "Caption. Excludes album children. @ symbol excluded unless the app user can perform "
                "admin-equivalent tasks on the Facebook Page connected to the Instagram account used to "
                "create the caption."
            ),
        ),
        th.Property(
            "comments_count",
            th.IntegerType,
            description=(
                "Count of comments on the media. Excludes comments on album child media and the media's "
                "caption. Includes replies on comments."
            ),
        ),
        th.Property(
            "is_comment_enabled",
            th.BooleanType,
            description="Indicates if comments are enabled or disabled. Excludes album children.",
        ),
        th.Property(
            "like_count",
            th.IntegerType,
            description=(
                "Count of likes on the media. Excludes likes on album child media and likes on promoted posts "
                "created from the media. Includes replies on comments."
            ),
        ),
        th.Property(
            "media_product_type",
            th.StringType,
            description="Surface where the media is published. Can be AD, FEED, IGTV, or STORY.",
        ),
        th.Property(
            "media_type",
            th.StringType,
            description="Media type. Can be CAROUSEL_ALBUM, IMAGE, or VIDEO.",
        ),
        th.Property(
            "media_url",
            th.StringType,
            description=(
                "Media URL. Will be omitted from responses if the media contains copyrighted material, "
                "or has been flagged for a copyright violation."
            ),
        ),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="ID of Instagram user who created the media.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="Username of Instagram user who created the media.",
                ),
            ),
            description=(
                "ID of Instagram user who created the media. Only returned if the app user making the query "
                "also created the media, otherwise username field will be returned instead."
            ),
        ),
        th.Property(
            "permalink",
            th.StringType,
            description="Permanent URL to the media.",
        ),
        th.Property(
            "shortcode",
            th.StringType,
            description="Shortcode to the media.",
        ),
        th.Property(
            "thumbnail_url",
            th.StringType,
            description="Media thumbnail URL. Only available on VIDEO media.",
        ),
        th.Property(
            "timestamp",
            th.DateTimeType,
            description="ISO 8601 formatted creation date in UTC (default is UTC ±00:00)",
        ),
        th.Property(
            "username",
            th.StringType,
            description="Username of user who created the media.",
        ),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.fields)
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "media_id": record["id"],
            "media_type": record["media_type"],
            # media_product_type not present for carousel children media
            "media_product_type": record.get("media_product_type"),
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class MediaChildrenStream(MediaStream):
    """Define custom stream."""

    name = "media_children"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    # media_id is populated using child context keys from MediaStream
    path = "/{media_id}/children"
    # caption, comments_count, is_comment_enabled, like_count, media_product_type
    # not available on album children
    # https://developers.facebook.com/docs/instagram-api/reference/ig-media#fields
    fields = [
        "id",
        "ig_id",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class MediaInsightsStream(InstagramStream):
    """Define custom stream for media insights."""

    name = "media_insights"
    path = "/{media_id}/insights"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("period", th.StringType),
        th.Property("end_time", th.DateTimeType),
        th.Property("context", th.StringType),
        th.Property("value", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    @staticmethod
    def _metrics_for_media_type(
        media_type: str, media_product_type: Optional[str]
    ) -> List[str]:
        """
        Decide metrics per media type/product type.

        IMPORTANT for v22:
        - DO NOT request 'plays' (deprecated) → caused your first error.
        - DO NOT request 'impressions' for media insights (deprecated) → caused your new error.
        """
        if media_type in ("IMAGE", "VIDEO"):
            if media_product_type == "STORY":
                # Story-like media insights
                return [
                    "reach",
                    "replies",
                    "exits",
                    "taps_forward",
                    "taps_back",
                ]
            elif media_product_type == "REELS":
                # Reels insights (no plays, no impressions)
                return [
                    "comments",
                    "likes",
                    "reach",
                    "saved",
                    "shares",
                    "total_interactions",
                    "views",
                ]
            else:  # media_product_type is "AD", "FEED" or None
                # Generic feed posts / videos: keep core KPIs without impressions
                metrics: List[str] = [
                    "total_interactions",
                    "reach",
                    "saved",
                    "views",
                ]
                return metrics

        if media_type == "CAROUSEL_ALBUM":
            # Carousel posts – again, no impressions or video_views
            return [
                "total_interactions",
                "reach",
                "saved",
                "views",
            ]

        raise ValueError(
            f"media_type from parent record must be one of IMAGE, VIDEO, CAROUSEL_ALBUM, got: {media_type}"
        )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        metrics = self._metrics_for_media_type(
            context["media_type"], context.get("media_product_type")
        )
        params["metric"] = ",".join(metrics)
        return params

    def validate_response(self, response: requests.Response) -> None:
        err = response.json().get("error", {})
        message = str(err.get("message"))
        user_title = err.get("error_user_title")

        if user_title == "Media posted before business account conversion" or "(#10) Not enough viewers for the media to show insights" in message:
            self.logger.warning(f"Skipping media insights: {err}")
            return

        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        err = resp_json.get("error", {})
        message = str(err.get("message"))
        user_title = err.get("error_user_title")

        # Same special-case handling as validate_response
        if user_title == "Media posted before business account conversion" or "(#10) Not enough viewers for the media to show insights" in message:
            return

        for row in resp_json.get("data", []):
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row.get("title"),
                "id": row["id"],
                "description": row.get("description"),
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            item = {
                                "context": key,
                                "value": value,
                                "end_time": pendulum.parse(values["end_time"]).format(
                                    "YYYY-MM-DD HH:mm:ss"
                                ),
                            }
                            item.update(base_item)
                            yield item
                    else:
                        values.update(base_item)
                        if "end_time" in values:
                            values["end_time"] = pendulum.parse(
                                values["end_time"]
                            ).format("YYYY-MM-DD HH:mm:ss")
                        yield values


# Insights not available for children media objects
# https://developers.facebook.com/docs/instagram-api/reference/ig-media/insights#limitations
# class MediaChildrenInsightsStream(BaseMediaInsightsStream):
#     """Define custom stream."""
#     name = "media_children_insights"
#     parent_stream_type = MediaChildrenStream


class StoryInsightsStream(InstagramStream):
    """Define custom stream for story insights."""

    name = "story_insights"
    path = "/{media_id}/insights"
    parent_stream_type = StoriesStream
    state_partitioning_keys = ["user_id"]
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("period", th.StringType),
        th.Property("end_time", th.DateTimeType),
        th.Property("context", th.StringType),
        th.Property("value", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    @staticmethod
    def _metrics_for_media_type(
        media_type: str, media_product_type: Optional[str]
    ) -> List[str]:
        """
        Story insights – v22-safe:
        - No 'impressions' here either to avoid future surprises.
        """
        if media_type in ("IMAGE", "VIDEO"):
            if media_product_type == "STORY":
                return [
                    "reach",
                    "replies",
                    "exits",
                    "taps_forward",
                    "taps_back",
                    "views",
                ]
            # Fallback: treat like generic media
            return [
                "reach",
                "views",
                "total_interactions",
                "saved",
            ]

        if media_type == "CAROUSEL_ALBUM":
            return [
                "reach",
                "views",
                "total_interactions",
                "saved",
            ]

        raise ValueError(
            f"media_type from parent record must be one of IMAGE, VIDEO, CAROUSEL_ALBUM, got: {media_type}"
        )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        metrics = self._metrics_for_media_type(
            context["media_type"], context.get("media_product_type")
        )
        params["metric"] = ",".join(metrics)
        return params

    def validate_response(self, response: requests.Response) -> None:
        err = response.json().get("error", {})
        message = str(err.get("message"))
        user_title = err.get("error_user_title")

        if user_title == "Media posted before business account conversion" or "(#10) Not enough viewers for the media to show insights" in message:
            self.logger.warning(f"Skipping story insights: {err}")
            return

        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        err = resp_json.get("error", {})
        message = str(err.get("message"))
        user_title = err.get("error_user_title")

        if user_title == "Media posted before business account conversion" or "(#10) Not enough viewers for the media to show insights" in message:
            return

        for row in resp_json.get("data", []):
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row.get("title"),
                "id": row["id"],
                "description": row.get("description"),
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            item = {
                                "context": key,
                                "value": value,
                                "end_time": pendulum.parse(values["end_time"]).format(
                                    "YYYY-MM-DD HH:mm:ss"
                                ),
                            }
                            item.update(base_item)
                            yield item
                    else:
                        values.update(base_item)
                        if "end_time" in values:
                            values["end_time"] = pendulum.parse(
                                values["end_time"]
                            ).format("YYYY-MM-DD HH:mm:ss")
                        yield values


class UserInsightsStream(InstagramStream):
    parent_stream_type = UsersStream
    # user_id is populated using child context keys from UsersStream
    path = "/{user_id}/insights"
    primary_keys = ["id"]
    replication_key = "end_time"
    records_jsonpath = "$.data[*]"
    has_pagination = True
    min_start_date: datetime = pendulum.now("UTC").subtract(years=2).add(days=1)
    max_end_date: datetime = pendulum.today("UTC").subtract(days=1)
    max_time_window: timedelta = pendulum.duration(days=30)
    time_period: str  # e.g. "day", "week", "days_28", "lifetime"
    metrics: List[str]
    _unsupported_metrics: Set[str]
    _active_metrics: Optional[List[str]] = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("period", th.StringType),
        th.Property("end_time", th.DateTimeType),
        th.Property("context", th.StringType),
        th.Property("value", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._unsupported_metrics = set()

    def _effective_metrics(self) -> List[str]:
        """Return metrics filtered to exclude ones already rejected by the API."""
        return [m for m in self.metrics if m not in self._unsupported_metrics]

    @staticmethod
    def _parse_incompatible_metric(error_message: str) -> Optional[str]:
        """Extract the metric name from the API error payload."""
        if "incompatible" not in error_message:
            return None
        match = re.search(r"metric \(([^)]+)\)", error_message)
        if match:
            return match.group(1)
        return None

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        while True:
            metrics = self._effective_metrics()
            if not metrics:
                self.logger.warning(
                    "Skipping %s: no compatible metrics remain for period '%s'.",
                    self.name,
                    self.time_period,
                )
                return

            self._active_metrics = metrics
            try:
                yield from super().get_records(context)
                return
            except FatalAPIError as exc:
                metric = self._parse_incompatible_metric(str(exc))
                if metric:
                    self.logger.warning(
                        "API rejected metric '%s' for period '%s'; retrying without it.",
                        metric,
                        self.time_period,
                    )
                    self._unsupported_metrics.add(metric)
                    continue
                raise
            finally:
                self._active_metrics = None

    def _fetch_time_based_pagination_range(
        self,
        context,
        min_since: datetime,
        max_until: datetime,
        max_time_window: timedelta,
    ) -> Tuple[datetime, datetime]:
        """
        Make "since" and "until" pagination timestamps.
        """
        try:
            since = min(max(self.get_starting_timestamp(context), min_since), max_until)
            window_end = min(
                self.get_replication_key_signpost(context),
                pendulum.instance(since).add(seconds=max_time_window.seconds),
            )
        # seeing cases where self.get_starting_timestamp() is null
        # possibly related to target-bigquery pushing malformed state - https://gitlab.com/meltano/sdk/-/issues/300
        except TypeError:
            since = min_since
            window_end = pendulum.instance(since).add(seconds=max_time_window.seconds)
        until = min(window_end, max_until)
        return since, until

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        if next_page_token:
            return params

        metrics = self._active_metrics or self._effective_metrics()
        if not metrics:
            return params

        params["metric"] = ",".join(metrics)
        params["period"] = self.time_period

        # v22: metrics like impressions, accounts_engaged, total_interactions
        # must explicitly use metric_type=total_value
        params["metric_type"] = "total_value"

        if self.has_pagination:
            since, until = self._fetch_time_based_pagination_range(
                context,
                min_since=self.min_start_date,
                max_until=self.max_end_date,
                max_time_window=self.max_time_window,
            )
            params["since"] = since
            params["until"] = until

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json.get("data", []):
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row.get("title"),
                "id": row["id"],
                "description": row.get("description"),
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            item = {
                                "context": key,
                                "value": value,
                                "end_time": pendulum.parse(values["end_time"]).format(
                                    "YYYY-MM-DD HH:mm:ss"
                                ),
                            }
                            item.update(base_item)
                            yield item
                    else:
                        values.update(base_item)
                        if "end_time" in values:
                            values["end_time"] = pendulum.parse(
                                values["end_time"]
                            ).format("YYYY-MM-DD HH:mm:ss")
                        yield values


class UserInsightsOnlineFollowersStream(UserInsightsStream):
    """Define custom stream."""

    name = "user_insights_online_followers"
    metrics = ["online_followers"]
    time_period = "lifetime"
    # NOTE: online_followers has a limited historical range


class UserInsightsFollowersStream(UserInsightsStream):
    """Define custom stream for follower_count."""

    name = "user_insights_followers"
    metrics = ["follower_count"]
    time_period = "day"
    min_start_date = pendulum.now("UTC").subtract(days=30)


class UserInsightsDailyStream(UserInsightsStream):
    """Daily user insights (v22-safe subset)."""

    name = "user_insights_daily"
    metrics = [
        "accounts_engaged",
        "reach",
        "profile_views",
        "website_clicks",
        "total_interactions",
        "views",
        "likes",
        "comments",
        "shares",
        "saves",
        "replies",
        "profile_links_taps",
    ]
    time_period = "day"


class UserInsightsWeeklyStream(UserInsightsStream):
    """Weekly user insights (v22-safe subset)."""

    name = "user_insights_weekly"
    metrics = [
        "accounts_engaged",
        "reach",
        "profile_views",
        "total_interactions",
        "views",
        "likes",
        "comments",
        "shares",
        "saves",
        "replies",
    ]
    time_period = "week"


class UserInsights28DayStream(UserInsightsStream):
    """28-day user insights (v22-safe subset)."""

    name = "user_insights_28day"
    metrics = [
        "reach",
        "views",
        "total_interactions",
        "likes",
        "comments",
        "shares",
        "saves",
    ]
    time_period = "days_28"
