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
    def __init__(self, run, action, plot_name, data):
        self.run = run
        self.action = action
        self.plot_name = plot_name
        try:
            self.data = pickle.loads(data)
        except Exception as e:
            raise RuntimeError(
                f'LogEntry ({self.run}, {self.action}, {self.plot_name}) data '
                f'couldn\'t be unpickled:\n{e}')

    def __repr__(self):
        return (f'{self.run=}\n'
                f'{self.action=}\n'
                f'{self.plot_name=}\n'
                f'{repr(self.data)[:100]}...\n')

    @classmethod
    def from_pubsub_message(cls, message):
        run = message.attributes.get('run')
        action = message.attributes.get('action')
        plot_name = message.attributes.get('plot_name')
        if plot_name is None:
            raise RuntimeError(f'no plot_name field')
        return cls(run, action, plot_name, message.data)

