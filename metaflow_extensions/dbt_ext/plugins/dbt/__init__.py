class dbt_deco:
    def __init__(self, **kwargs):
        self.generate_docs = kwargs.get("generate_docs", False)
        self.kwargs = kwargs

    def __call__(self, step_func):
        # Make generated docs available as a card.
        from metaflow import _internal_dbt

        if self.generate_docs:
            from metaflow import card

            return card(type="dbt_docs")(_internal_dbt(**self.kwargs)(step_func))
        else:
            return _internal_dbt(**self.kwargs)(step_func)
