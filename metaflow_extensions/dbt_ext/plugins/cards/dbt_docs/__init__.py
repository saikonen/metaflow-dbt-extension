from metaflow.cards import MetaflowCard


class DBTDocs(MetaflowCard):
    type = "dbt_docs"

    def render(self, task):  # this function returns the HTML to be rendered
        return (
            task.data.run_results
        )  # assumes you are saving an attribute named `html` in the task


CARDS = [DBTDocs]
