def topic_exists(pub_client, project, topic):
    """
    """
    project_path = f'projects/{project}'
    gen = (tx.name for tx in pub_client.list_topics(project=project_path))
    topic_path = pub_client.topic_path(project, topic)
    return topic_path in gen

