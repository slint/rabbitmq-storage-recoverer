from collections import namedtuple
import msgpack
import json

Message = namedtuple("Message", ["id", "vhost", "exchange", "routing_key", "body"])

AVAILABLE_TASKS = [
    "invenio_accounts.tasks.clean_session_table",
    "invenio_accounts.tasks.delete_ips",
    "invenio_accounts.tasks.send_security_email",
    "invenio_accounts.tasks.update_domain_status",
    "invenio_files_rest.tasks.clear_orphaned_files",
    "invenio_files_rest.tasks.merge_multipartobject",
    "invenio_files_rest.tasks.migrate_file",
    "invenio_files_rest.tasks.remove_expired_multipartobjects",
    "invenio_files_rest.tasks.remove_file_data",
    "invenio_files_rest.tasks.schedule_checksum_verification",
    "invenio_files_rest.tasks.verify_checksum",
    "invenio_github.tasks.disconnect_github",
    "invenio_github.tasks.process_release",
    "invenio_github.tasks.refresh_accounts",
    "invenio_github.tasks.sync_account",
    "invenio_github.tasks.sync_hooks",
    "invenio_indexer.tasks.delete_record",
    "invenio_indexer.tasks.index_record",
    "invenio_indexer.tasks.process_bulk_queue",
    "invenio_jobs.tasks.execute_run",
    "invenio_mail.tasks._send_email_with_attachments",
    "invenio_mail.tasks.send_email",
    "invenio_notifications.tasks.broadcast_notification",
    "invenio_notifications.tasks.dispatch_notification",
    "invenio_oauthclient.tasks.create_or_update_roles_task",
    "invenio_rdm_records.services.iiif.tasks.cleanup_tiles_file",
    "invenio_rdm_records.services.iiif.tasks.generate_tiles",
    "invenio_rdm_records.services.pids.tasks.register_or_update_pid",
    "invenio_rdm_records.services.tasks.reindex_stats",
    "invenio_rdm_records.services.tasks.send_post_published_signal",
    "invenio_rdm_records.services.tasks.update_expired_embargos",
    "invenio_records_resources.services.files.tasks.fetch_file",
    "invenio_records_resources.tasks.extract_file_metadata",
    "invenio_records_resources.tasks.manage_indexer_queues",
    "invenio_records_resources.tasks.send_change_notifications",
    "invenio_requests.tasks.check_expired_requests",
    "invenio_requests.tasks.request_moderation",
    "invenio_stats.tasks.aggregate_events",
    "invenio_stats.tasks.process_events",
    "invenio_swh.tasks.cleanup_depositions",
    "invenio_swh.tasks.poll_deposit",
    "invenio_swh.tasks.process_published_record",
    "invenio_users_resources.services.domains.tasks.delete_domains",
    "invenio_users_resources.services.domains.tasks.reindex_domains",
    "invenio_users_resources.services.groups.tasks.reindex_groups",
    "invenio_users_resources.services.groups.tasks.unindex_groups",
    "invenio_users_resources.services.users.tasks.execute_moderation_actions",
    "invenio_users_resources.services.users.tasks.reindex_users",
    "invenio_users_resources.services.users.tasks.unindex_users",
    "invenio_vocabularies.services.tasks.import_funders",
    "invenio_vocabularies.services.tasks.process_datastream",
    "invenio_webhooks.models.process_event",
    "zenodo_rdm.metrics.tasks.calculate_metrics",
    "zenodo_rdm.openaire.tasks.openaire_delete",
    "zenodo_rdm.openaire.tasks.openaire_direct_index",
    "zenodo_rdm.openaire.tasks.retry_openaire_failures",
]


def message_from_decoded_etf(message_id: int, decoded_message):
    # Message structure is a bit complicated, so trust me on this one. Pprint the decoded_message if needed!
    # pprint(decoded_message)
    assert decoded_message[0][0] == "basic_message"
    assert decoded_message[0][1][0] == "resource"
    message_vhost = decoded_message[0][1][1].decode("utf-8")
    assert decoded_message[0][1][2] == "exchange"
    message_exchange = decoded_message[0][1][3].decode("utf-8")
    assert len(decoded_message[0][2]) == 1
    message_routing_key = decoded_message[0][2][0].decode("utf-8")

    # The header needs to be parsed separately
    message_headers = decoded_message[0][3][3]
    # Extract task from headers
    for task in AVAILABLE_TASKS:
        if task in message_headers.decode(errors="ignore"):
            message_celery_task = task

    # The body itself can be in multiple chunks if its very long. They need to be reassembled in reverse order, for some reason
    message_body_parts = decoded_message[0][3][5]
    reassembled_body = b""
    for body_chunk in message_body_parts[::-1]:
        reassembled_body += body_chunk

    try:
        # try msgpack
        message_body_parsed = msgpack.unpackb(reassembled_body)
        payload = {
            "task": message_celery_task,
            "queue": message_routing_key,
            "args": message_body_parsed[0],
            "kwargs": message_body_parsed[1],
        }
        with open("recovered_tasks.jsonl", "a") as f:
            f.write(json.dumps(payload))
            f.write("\n")
        print(json.dumps(payload))
        message_body = json.dumps(message_body_parsed)
    except Exception:
        message_body = reassembled_body.decode("utf-8")

    return Message(
        id=message_id,
        vhost=message_vhost,
        exchange=message_exchange,
        routing_key=message_routing_key if message_routing_key else None,
        body=message_body,
    )
