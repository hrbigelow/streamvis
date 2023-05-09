import pickle

def topic_exists(pub_client, project, topic):
    """
    """
    project_path = f'projects/{project}'
    gen = (tx.name for tx in pub_client.list_topics(project=project_path))
    topic_path = pub_client.topic_path(project, topic)
    return topic_path in gen

class LogEntry:
    """
    Represents the base unit communicated from the client
    """
    def __init__(self, run, action, cds, data):
        self.run = run
        self.action = action
        self.cds = cds
        try:
            self.data = pickle.loads(data)
        except Exception as e:
            raise RuntimeError(
                f'LogEntry ({self.run}, {self.action}, {self.cds}) data '
                f'couldn\'t be unpickled:\n{e}')

    @classmethod
    def from_pubsub_message(cls, message):
        run = message.attributes.get('run')
        action = message.attributes.get('action')
        cds = message.attributes.get('cds')
        return cls(run, action, cds, message.data)

