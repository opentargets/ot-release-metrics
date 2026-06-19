# Open Targets release metrics

Deployed app: **https://open-targets-metrics.streamlit.app/**

Streamlit dashboard to explore, compare, and visualise release metrics for Open Targets. Metrics are calculated by the [release_metrics](https://github.com/opentargets/pts/blob/main/src/pts/transformers/release_metrics.py) transformer in the `pts` repository and published to the [ot-release-metrics](https://huggingface.co/datasets/opentargets/ot-release-metrics) Hugging Face dataset.

## Running locally

```bash
uv sync
uv run streamlit run app.py
```

## Deploying

The app is hosted on Streamlit Community Cloud and deployed automatically on each push to `master`.

## Rebooting

When there are changes to either the code or the underlying metrics data, the app needs to be rebooted:

1. Log in to https://share.streamlit.io/ with your GitHub account.
2. Switch to the **opentargets** workspace (top-right corner).
3. Next to the **ot-release-metrics** app, click the three-dot menu and select **Reboot**.

> A "Rerun" option inside the app (sandwich menu, top-right) is different from a full reboot and will not ingest new data.
