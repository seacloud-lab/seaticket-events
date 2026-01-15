# -*- coding: utf-8 -*-
import logging
import numpy as np
from sklearn.manifold import TSNE

from seaqa_events.app.log import setup_logger
from seaqa_events.utils.constant import MAX_EMBEDDING_ANALYSIS_RECORDS, ConnectionType
from seaqa_events.utils.seadb_api import SeaDBAPI
from seaqa_events.utils.seaqa_db import SeaqaDB

task_logger = setup_logger('seaqa_events_task', propagate=False)

def perform_embedding_analysis(project_uuid, connection_ids, username, start_year=None, end_year=None):
    try:
        seadb_api = SeaDBAPI(username)
        all_records = []

        if isinstance(connection_ids, str):
            connection_ids = [cid.strip() for cid in connection_ids.split(',') if cid.strip()]

        session_class = init_db_session_class(configs)
        seaqa_db = SeaqaDB(session_class)
        connections = seaqa_db.get_connection_by_ids(project_uuid, tuple(connection_ids))

        for conn in connections:
            remaining = MAX_EMBEDDING_ANALYSIS_RECORDS - len(all_records)
            if remaining <= 0:
                break
            connection_id = conn.id
            connection_type = conn.type

            column_names = ['_pk', 'title', 'ai_summary', 'ai_summary_vector']
            if connection_type == ConnectionType.GITHUB_ISSUE.value:
                column_names.extend(['url', 'state'])
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                column_names.extend(['slug', 'topic_id'])
            elif connection_type == ConnectionType.SEAFILE.value:
                column_names.extend(['path'])
            elif connection_type == ConnectionType.SITE.value:
                column_names.extend(['url'])

            records = seadb_api.list_connection_records_with_columns(
                project_uuid, connection_id, connection_type,
                column_names, limit=remaining,
                extra_columns={'connection_id': connection_id, 'connection_type': connection_type},
                start_year=start_year, end_year=end_year
            )
            all_records.extend(records)

        if not all_records:
            task_logger.warning("No records found for embedding analysis")
            return {'records': []}

        valid_records = []
        vectors = []

        for record in all_records:
            ai_summary = record.get('ai_summary')
            ai_summary_vector = record.get('ai_summary_vector')
            if ai_summary and ai_summary_vector:
                record.pop('ai_summary_vector')
                valid_records.append(record)
                vectors.append(ai_summary_vector)

        if not vectors:
            task_logger.warning("No records with embeddings found")
            return {'records': []}

        embeddings_2d = generate_embeddings_2d_with_tsne(vectors)
        for i, record in enumerate(valid_records):
            if i < len(embeddings_2d):
                record['x'] = embeddings_2d[i][0]
                record['y'] = embeddings_2d[i][1]
        return {
            'records': valid_records,
        }
    
    except Exception as e:
        task_logger.error(f"Error in embedding analysis: {str(e)}")
        raise


def generate_embeddings_2d_with_tsne(vectors):
    if not vectors:
        return []
    
    embeddings_array = np.array(vectors)

    perplexity = min(30, len(embeddings_array) - 1)
    if perplexity < 1:
        perplexity = 5
    
    tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity)
    embeddings_2d = tsne.fit_transform(embeddings_array)
    
    result = []
    for i in range(len(embeddings_2d)):
        coords = [float(embeddings_2d[i][0]), float(embeddings_2d[i][1])]
        result.append(coords)
    
    return result
