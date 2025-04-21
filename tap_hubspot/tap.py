"""tap-hubspot tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_hubspot import streams


class TapHubspot(Tap):
    """tap-hubspot is a Singer tap for Hubspot."""

    name = "tap-hubspot"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=False,
            description="Token to authenticate against the API service",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="The OAuth app client ID.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            description="The OAuth app client secret.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            description="The OAuth app refresh token.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="Latest record date to sync",
         ),
         th.Property(
            "streams",
            th.ArrayType(th.StringType()),
            required=False,
            description="List of streams",
        ),
        th.Property(
            "fields",
            th.ArrayType(th.StringType()),
            required=False,
            description="List of fields",
        ),
        th.Property(
            "filter_field",
            th.StringType,
            required=False,
            description="Filter field to use for the stream",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.HubspotStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        all_streams_dict = {
        'contacts': 'ContactStream',
        'users': 'UsersStream',
        'owners': 'OwnersStream',
        'tickets_pipeline': 'TicketPipelineStream',
        'deal_pipelines': 'DealPipelineStream',
        'email_subscriptions': 'EmailSubscriptionStream',
        'property_notes': 'PropertyNotesStream',
        'companies': 'CompanyStream',
        'deals': 'DealStream',
        'feedback_submissions': 'FeedbackSubmissionsStream',
        'line_items': 'LineItemStream',
        'products': 'ProductStream',
        'tickets': 'TicketStream',
        'quotes': 'QuoteStream',
        'goals': 'GoalStream',
        'calls': 'CallStream',
        'communications': 'CommunicationStream',
        'emails': 'EmailStream',
        'meetings': 'MeetingStream',
        'notes': 'NoteStream',
        'postal_mail': 'PostalMailStream',
        'tasks': 'TaskStream'
    }
       
        selected_streams = self.config.get("streams")
        if selected_streams:
           
            valid_streams = [
                stream_key for stream_key in selected_streams
                if stream_key in all_streams_dict
            ]

            if not valid_streams:
                self.logger.warning(
                    "All stream keys in config are invalid. "
                    "Falling back to all streams."
                )
                valid_streams = list(all_streams_dict.keys())
        else:
            self.logger.info("No 'streams' config specified. Loading all streams.")
            valid_streams = list(all_streams_dict.keys())

        stream_instances = []
        for stream_key in valid_streams:
            class_name = all_streams_dict[stream_key]

            try:
                stream_class = getattr(streams, class_name)
                stream_instances.append(stream_class(self))
            except AttributeError:
                self.logger.exception(
                    "Class '%s' not found in streams module.", class_name
                )
            except Exception as e:
                self.logger.exception("Error instantiating stream '%s'", class_name)
                raise

        self.logger.info("Instantiated streams: %s", [s.name for s in stream_instances])
        return stream_instances

if __name__ == "__main__":
    TapHubspot.cli()
