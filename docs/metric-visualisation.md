# Metris visualisation and comparison using a Streamlit app
The app is deployed here: **https://open-targets-metrics.streamlit.app/**


## Running the web app to visualise and compare the metrics
To start the app locally, run `streamlit run app.py`.

If you encounter a `TomlDecodeError`, this can be resolved by removing the `~/.streamlit` folder.


## Deploying the app
The application will be **automatically deployed** each time the `master` branch is updated.

The hosting is served using Streamlit's native Cloud service.


## Rebooting the app
When there are any changes to either the code or data (release metrics) used by the app, it will need to be rebooted. In order to do that:
In order to do that:
1. Log in to https://share.streamlit.io/ with your work GitHub account.
2. In the upper right corner, click on your username and switch to the _opentargets_ workspace.
3. Next to the _ot-release-metrics_ app, click on a three dot icon and then choose “Reboot”.


> **Note**
> A “Rerun” option, available inside the app itself from a sandwich dropdown menu in the upper right corner, is _different_ to the “Reboot” option described above, and will not work to make the app ingest the new data.
